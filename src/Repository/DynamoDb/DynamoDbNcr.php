<?php
declare(strict_types=1);

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
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class DynamoDbNcr implements Ncr
{
    /** @var DynamoDbClient */
    private $client;

    /** @var LoggerInterface */
    private $logger;

    /** @var TableManager */
    private $tableManager;

    /** @var ItemMarshaler */
    private $marshaler;

    /**
     * @param DynamoDbClient       $client
     * @param TableManager         $tableManager
     * @param LoggerInterface|null $logger
     */
    public function __construct(DynamoDbClient $client, TableManager $tableManager, ?LoggerInterface $logger = null)
    {
        $this->client = $client;
        $this->tableManager = $tableManager;
        $this->logger = $logger ?: new NullLogger();
        $this->marshaler = new ItemMarshaler();
    }

    /**
     * {@inheritdoc}
     */
    public function createStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        $this->tableManager->getNodeTable($qname)->create($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $hints = []): string
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        return $this->tableManager->getNodeTable($qname)->describe($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): bool
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $keyName = '#' . NodeTable::HASH_KEY_NAME;
            $response = $this->client->getItem([
                'ConsistentRead'           => $consistent,
                'TableName'                => $tableName,
                'ProjectionExpression'     => $keyName,
                'ExpressionAttributeNames' => [$keyName => NodeTable::HASH_KEY_NAME],
                'Key'                      => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    return false;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while checking for [%s] in DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }

        return isset($response['Item']);
    }

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): Node
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName'      => $tableName,
                'Key'            => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    throw NodeNotFound::forNodeRef($nodeRef, $e);
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while getting [%s] from DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
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
                    'exception'  => $e,
                    'item'       => $response['Item'],
                    'hints'      => $hints,
                    'table_name' => $tableName,
                    'node_ref'   => (string)$nodeRef,
                ]
            );

            throw NodeNotFound::forNodeRef($nodeRef, $e);
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $hints = []): array
    {
        if (count($nodeRefs) === 1) {
            try {
                return [(string)$nodeRefs[0] => $this->getNode($nodeRefs[0], $consistent, $hints)];
            } catch (NodeNotFound $e) {
                return [];
            } catch (\Exception $e) {
                throw $e;
            }
        }

        $batch = new BatchGetItemRequest($this->client);
        $batch->withBatchSize(2)->usingConsistentRead($consistent);

        foreach ($nodeRefs as $nodeRef) {
            $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);
            $batch->addItemKey($tableName, [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]);
        }



        $batch->flush();

        return [];

    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void
    {
        $node->freeze();
        $nodeRef = NodeRef::fromNode($node);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);
        $table = $this->tableManager->getNodeTable($nodeRef->getQName());

        $params = ['TableName' => $tableName];
        if (null !== $expectedEtag) {
            $params['ConditionExpression'] = 'etag = :v_etag';
            $params['ExpressionAttributeValues'] = [':v_etag' => ['S' => (string)$expectedEtag]];
        }

        try {
            $item = $this->marshaler->marshal($node);
            $item[NodeTable::HASH_KEY_NAME] = ['S' => $nodeRef->toString()];
            $table->beforePutItem($item, $node);
            $params['Item'] = $item;
            $this->client->putItem($params);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ConditionalCheckFailedException' === $errorName) {
                    throw new OptimisticCheckFailed(
                        sprintf(
                            'NodeRef [%s] in DynamoDb table [%s] did not have expected etag [%s].',
                            $nodeRef,
                            $tableName,
                            $expectedEtag
                        ),
                        $e
                    );
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while putting [%s] into DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $hints = []): void
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $this->client->deleteItem([
                'TableName' => $tableName,
                'Key'       => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    // if it's already deleted, it's fine
                    return;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while deleting [%s] from DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function findNodeRefs(IndexQuery $query, array $hints = []): IndexQueryResult
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

        try {
            $response = $this->client->query($params);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s on IndexQuery [%s] on DynamoDb table [%s].',
                    $errorName,
                    $query->getAlias(),
                    $tableName
                ),
                $code,
                $e
            );
        }

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
                        'exception'   => $e,
                        'item'        => $item,
                        'hints'       => $hints,
                        'index_alias' => $query->getAlias(),
                        'index_query' => $query,
                        'table_name'  => $tableName,
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
