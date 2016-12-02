<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Gdbots\Ncr\NcrAdmin;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DynamoDbNcrAdmin implements NcrAdmin
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
}
