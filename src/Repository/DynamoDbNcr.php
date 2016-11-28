<?php

namespace Gdbots\Ncr\Repository;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DynamoDbNcr implements Ncr
{
    /** @var DynamoDbClient */
    protected $client;

    /** @var DynamoDbNodeTableManager */
    protected $tableManager;

    /** @var LoggerInterface */
    protected $logger;

    /** @var ItemMarshaler */
    protected $marshaler;

    /**
     * @param DynamoDbClient $client
     * @param DynamoDbNodeTableManager $tableManager
     * @param LoggerInterface|null $logger
     */
    public function __construct(
        DynamoDbClient $client,
        DynamoDbNodeTableManager $tableManager,
        LoggerInterface $logger = null
    ) {
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
        $tableName = $this->tableManager->getTableName($qname, $hints);
        $this->tableManager->getTableSchema($qname)->create($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $hints = [])
    {
        $tableName = $this->tableManager->getTableName($qname, $hints);
        $this->tableManager->getTableSchema($qname)->describe($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function getByNodeRef(NodeRef $nodeRef, $consistent = false, array $hints = [])
    {
        $tableName = $this->tableManager->getTableName($nodeRef->getQName(), $hints);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName' => $tableName,
                'Key' => [
                    DynamoDbNodeTableSchema::HASH_KEY_NAME => ['S' => $nodeRef->toString()]
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
     * @param array $item
     * @return Node
     */
    protected function unmarshalItem(array $item)
    {
        return $this->marshaler->unmarshal($item);
    }
}
