<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\Exception\RepositoryIndexNotFound;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Ncr\NcrAdmin;
use Gdbots\Ncr\Repository\LocalNodeCacheTrait;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DynamoDbNcr implements Ncr, NcrAdmin
{
    use LocalNodeCacheTrait;

    /** @var DynamoDbClient */
    protected $client;

    /** @var LoggerInterface */
    protected $logger;

    /** @var TableManager */
    private $tableManager;

    /** @var ItemMarshaler */
    private $marshaler;

    /**
     * @param DynamoDbClient       $client
     * @param TableManager         $tableManager
     * @param LoggerInterface|null $logger
     */
    public function __construct(DynamoDbClient $client, TableManager $tableManager, LoggerInterface $logger = null)
    {
        $this->client = $client;
        $this->tableManager = $tableManager;
        $this->logger = $logger ?: new NullLogger();
        $this->marshaler = new ItemMarshaler();
    }

    /**
     * {@inheritdoc}
     */
    final public function createStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        $this->tableManager->getNodeTable($qname)->create($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    final public function describeStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        return $this->tableManager->getNodeTable($qname)->describe($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    final public function hasNode(NodeRef $nodeRef, $consistent = false, array $hints = [])
    {
        if (!$consistent && $this->isInNodeCache($nodeRef)) {
            return true;
        }

        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $keyName = '#'.NodeTable::HASH_KEY_NAME;
            $response = $this->client->getItem([
                'ConsistentRead' => true,
                'TableName' => $tableName,
                'ProjectionExpression' => $keyName,
                'ExpressionAttributeNames' => [$keyName => NodeTable::HASH_KEY_NAME],
                'Key' => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]
            ]);

        } catch (AwsException $e) {
            if ('ResourceNotFoundException' === $e->getAwsErrorCode()) {
                return false;
            }

            throw new RepositoryOperationFailed(
                sprintf('%s while checking for [%s] in DynamoDb table [%s].', $e->getAwsErrorCode(), $nodeRef, $tableName),
                'ProvisionedThroughputExceededException' === $e->getAwsErrorCode() ? Code::RESOURCE_EXHAUSTED : Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to check for [%s] in DynamoDb table [%s].', $nodeRef, $tableName), Code::INTERNAL, $e
            );
        }

        return isset($response['Item']);
    }

    /**
     * {@inheritdoc}
     */
    final public function getNode(NodeRef $nodeRef, $consistent = false, array $hints = [])
    {
        if (!$consistent && $this->isInNodeCache($nodeRef)) {
            return $this->getFromNodeCache($nodeRef);
        }

        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName' => $tableName,
                'Key' => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]
            ]);

        } catch (AwsException $e) {
            if ('ResourceNotFoundException' === $e->getAwsErrorCode()) {
                throw NodeNotFound::forNodeRef($nodeRef, $e);
            }

            throw new RepositoryOperationFailed(
                sprintf('%s while fetching [%s] from DynamoDb table [%s].', $e->getAwsErrorCode(), $nodeRef, $tableName),
                'ProvisionedThroughputExceededException' === $e->getAwsErrorCode() ? Code::RESOURCE_EXHAUSTED : Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to get [%s] from DynamoDb table [%s].', $nodeRef, $tableName), Code::INTERNAL, $e
            );
        }

        if (!isset($response['Item']) || empty($response['Item'])) {
            throw NodeNotFound::forNodeRef($nodeRef);
        }

        try {
            /** @var Node $node */
            $node = $this->marshaler->unmarshal($response['Item']);
        } catch (\Exception $e) {
            $this->logger->error(
                'Item returned from DynamoDb table [{table_name}] for [{node_ref}] could not be unmarshaled.',
                [
                    'exception' => $e,
                    'item' => $response['Item'],
                    'hints' => $hints,
                    'table_name' => $tableName,
                    'node_ref' => (string)$nodeRef
                ]
            );

            throw NodeNotFound::forNodeRef($nodeRef, $e);
        }

        $this->addToNodeCache($nodeRef, $node);
        return $node;
    }

    /**
     * {@inheritdoc}
     */
    final public function putNode(Node $node, $expectedEtag = null, array $hints = [])
    {
        $node->freeze();
        $nodeRef = NodeRef::fromNode($node);
        $this->removeFromNodeCache($nodeRef);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);
        $table = $this->tableManager->getNodeTable($nodeRef->getQName());

        $params = ['TableName' => $tableName];
        if (null !== $expectedEtag) {
            $params['ConditionExpression'] = 'etag = :v_etag';
            $params['ExpressionAttributeValues'] = [':v_etag' => ['S' => (string)$expectedEtag]];
        }

        try {
            $item = $this->marshaler->marshal($node);
            $item[NodeTable::HASH_KEY_NAME] = ['S' => NodeRef::fromNode($node)->toString()];
            $table->beforePutItem($item, $node);
            $params['Item'] = $item;
            $this->client->putItem($params);

        } catch (AwsException $e) {
            if ('ConditionalCheckFailedException' === $e->getAwsErrorCode()) {
                throw new OptimisticCheckFailed(
                    sprintf(
                        'NodeRef [%s] in DynamoDb table [%s] did not have expected etag [%s].',
                        $nodeRef,
                        $tableName,
                        $expectedEtag
                    ),
                    $e
                );
            }

            throw new RepositoryOperationFailed(
                sprintf('%s while putting [%s] into DynamoDb table [%s].', $e->getAwsErrorCode(), $nodeRef, $tableName),
                'ProvisionedThroughputExceededException' === $e->getAwsErrorCode() ? Code::RESOURCE_EXHAUSTED : Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to put [%s] into DynamoDb table [%s].', $nodeRef, $tableName), Code::INTERNAL, $e
            );
        }

        $this->addToNodeCache($nodeRef, $node);
    }

    /**
     * {@inheritdoc}
     */
    final public function deleteNode(NodeRef $nodeRef, array $hints = [])
    {
        $this->removeFromNodeCache($nodeRef);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $this->client->deleteItem([
                'TableName' => $tableName,
                'Key' => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]
            ]);

        } catch (AwsException $e) {
            if ('ResourceNotFoundException' === $e->getAwsErrorCode()) {
                return;
            }

            throw new RepositoryOperationFailed(
                sprintf('%s while deleting [%s] into DynamoDb table [%s].', $e->getAwsErrorCode(), $nodeRef, $tableName),
                'ProvisionedThroughputExceededException' === $e->getAwsErrorCode() ? Code::RESOURCE_EXHAUSTED : Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to delete [%s] from DynamoDb table [%s].', $nodeRef, $tableName), Code::INTERNAL, $e
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    final public function findNodeRefs(IndexQuery $query, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($query->getQName(), $hints);
        $table = $this->tableManager->getNodeTable($query->getQName());

        if (!$table->hasIndex($query->getAlias())) {
            throw new RepositoryIndexNotFound(
                sprintf(
                    '%s::Index [%s] does not exist on table [%s].',
                    ClassUtils::getShortName($table),
                    $query->getAlias(),
                    $tableName
                )
            );
        }

        $params = $table->getIndex($query->getAlias())->createQuery($query);
        $params['TableName'] = $tableName;
        $unprocessedFilters = [];

        /*
         * When unprocessed_filters exist we must fetch the attributes from the
         * index so we can evaluate them in php before returning the result.
         */
        if (isset($params['unprocessed_filters'])) {
            $unprocessedFilters = $params['unprocessed_filters'];
            unset($params['unprocessed_filters']);
        } else {
            $params['ProjectionExpression'] = '#node_ref';
        }

        //echo json_encode($params, JSON_PRETTY_PRINT);
        //echo json_encode($unprocessedFilters, JSON_PRETTY_PRINT);

        try {
            $response = $this->client->query($params);

        } catch (AwsException $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while running IndexQuery [%s] on DynamoDb table [%s].',
                    $e->getAwsErrorCode(),
                    $query->getAlias(),
                    $tableName
                ),
                'ProvisionedThroughputExceededException' === $e->getAwsErrorCode() ? Code::RESOURCE_EXHAUSTED : Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to handle IndexQuery [%s] on DynamoDb table [%s].', $query->getAlias(), $tableName),
                Code::INTERNAL,
                $e
            );
        }

        echo json_encode($response->toArray(), JSON_PRETTY_PRINT).PHP_EOL.PHP_EOL;

        if (!isset($response['Items']) || empty($response['Items'])) {
            return new IndexQueryResult($query);
        }

        $nodeRefs = [];
        foreach ($response['Items'] as $item) {
            try {
                // todo: handle $unprocessedFilters
                $nodeRefs[] = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
            } catch (\Exception $e) {
                $this->logger->error(
                    'NodeRef returned from IndexQuery [{index_alias}] on DynamoDb table [{table_name}] is invalid.',
                    [
                        'exception' => $e,
                        'item' => $item,
                        'hints' => $hints,
                        'index_alias' => $query->getAlias(),
                        'index_query' => $query,
                        'table_name' => $tableName,
                    ]
                );
            }
        }

        if (isset($response['LastEvaluatedKey'])/* && $response['Count'] !== $response['ScannedCount']*/) {
            $nextCursor = base64_encode(json_encode($response['LastEvaluatedKey']));
        } else {
            $nextCursor = null;
        }

        return new IndexQueryResult($query, $nodeRefs, $nextCursor);
    }
}
