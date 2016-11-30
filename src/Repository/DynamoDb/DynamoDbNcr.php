<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Ncr\Ncr;
use Gdbots\Ncr\NcrAdmin;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Indexed\Indexed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DynamoDbNcr implements Ncr, NcrAdmin
{
    /** @var DynamoDbClient */
    protected $client;

    /** @var TableManager */
    protected $tableManager;

    /** @var LoggerInterface */
    protected $logger;

    /** @var ItemMarshaler */
    protected $marshaler;

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
    public function createStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        $this->tableManager->getNodeTable($qname)->create($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $hints);
        $this->tableManager->getNodeTable($qname)->describe($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, $consistent = false, array $hints = [])
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName' => $tableName,
                'Key' => [
                    NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]
                ]
            ]);

        } catch (AwsException $e) {
            if ('ProvisionedThroughputExceededException' === $e->getAwsErrorCode()) {
                throw new RepositoryOperationFailed(
                    sprintf(
                        'Read provisioning exceeded on DynamoDb table [%s].', $tableName
                    ),
                    Code::RESOURCE_EXHAUSTED,
                    $e
                );
            }

            throw new RepositoryOperationFailed(
                sprintf('Failed to query events from DynamoDb table [%s] for stream [%s].', $tableName),
                Code::UNAVAILABLE,
                $e
            );

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf('Failed to query events from DynamoDb table [%s] for stream [%s].', $tableName),
                Code::INTERNAL,
                $e
            );
        }

        if (!$response['Count']) {
            throw new NodeNotFound();
        }

        try {
            $node = $this->unmarshalItem($response['Item']);
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

            throw new NodeNotFound();
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, $expectedEtag = null, array $hints = [])
    {
        $nodeRef = NodeRef::fromNode($node);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $hints);
        $table = $this->tableManager->getNodeTable($nodeRef->getQName());

        if (null !== $expectedEtag) {
            //$this->optimisticCheck($streamId, $hints, $expectedEtag);
        }

        $item = $this->marshaler->marshal($node);
        $item[NodeTable::HASH_KEY_NAME] = ['S' => $nodeRef->toString()];
        if ($node instanceof Indexed) {
            $item[NodeTable::INDEXED_KEY_NAME] = ['BOOL' => true];
        }

        $table->beforePutItem($item, $node);
        //echo json_encode($item, JSON_PRETTY_PRINT);
        $this->client->putItem(['TableName' => $tableName, 'Item' => $item]);
    }

    /**
     * @param array $item
     * @return Node
     */
    protected function unmarshalItem(array $item)
    {
        return $this->marshaler->unmarshal($item);
    }


    /**
     * Returns an array with a "KeyConditionExpression" which can be used to query
     * this GSI using the provided value.
     *
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
     *
     * @param string $value
     *
     * @return array
     */
    /*
    public function getKeyConditionExpression($value)
    {
        return [
            'ExpressionAttributeNames' => [
                '#SLUG' => self::HASH_KEY_NAME
            ],
            'KeyConditionExpression' => '#SLUG = :v_slug',
            'ExpressionAttributeValues' => [
                ':v_slug' => ['S' => (string)$value]
            ]
        ];
    }
     */
}
