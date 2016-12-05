<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Pbjx\Util\ShardUtils;
use Gdbots\Schemas\Ncr\Mixin\Indexed\Indexed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Pbjx\Enum\Code;

/**
 * Represents a DynamoDb table and its GSI.  This class is used by
 * the TableManager to create/describe tables and fetch indexes
 * when needed during the Ncr operations.
 *
 * You can customize the Global Secondary Indexes by extending this
 * class and overriding the "getIndexes" method.
 *
 * NOTE: This will not update a table, it only handles creation.
 */
class NodeTable
{
    const SCHEMA_VERSION = 'v1';
    const HASH_KEY_NAME = '__node_ref';
    const INDEXED_KEY_NAME = '__indexed';

    /** @var GlobalSecondaryIndex[] */
    private $gsi;

    /**
     * The tables are constructed with "new $class" in the
     * TableManager so the constructor must be consistent.
     */
    final public function __construct()
    {
    }

    /**
     * Creates a DynamoDb table with the node schema.
     *
     * @param DynamoDbClient $client
     * @param string         $tableName
     *
     * @throws RepositoryOperationFailed
     */
    final public function create(DynamoDbClient $client, $tableName)
    {
        try {
            $client->describeTable(['TableName' => $tableName]);
            return;
        } catch (DynamoDbException $e) {
            // table doesn't exist, create it below
        }

        $this->loadIndexes();

        $attributes = [];
        $indexes = [];

        foreach ($this->gsi as $gsi) {
            foreach ($gsi->getKeyAttributes() as $definition) {
                $attributes[$definition['AttributeName']] = $definition;
            }

            $indexName = $gsi->getName();
            $indexes[$indexName] = [
                'IndexName' => $indexName,
                'KeySchema' => [
                    ['AttributeName' => $gsi->getHashKeyName(), 'KeyType' => 'HASH'],
                ],
                'Projection' => $gsi->getProjection() ?: ['ProjectionType' => 'KEYS_ONLY'],
                'ProvisionedThroughput' => $this->getDefaultProvisionedThroughput()
            ];

            if ($gsi->getRangeKeyName()) {
                $indexes[$indexName]['KeySchema'][] = ['AttributeName' => $gsi->getRangeKeyName(), 'KeyType' => 'RANGE'];
            }
        }

        $attributes[self::HASH_KEY_NAME] = ['AttributeName' => self::HASH_KEY_NAME, 'AttributeType' => 'S'];
        $attributes = array_values($attributes);
        $indexes = array_values($indexes);

        try {
            $client->createTable([
                'TableName' => $tableName,
                'AttributeDefinitions' => $attributes,
                'KeySchema' => [
                    ['AttributeName' => self::HASH_KEY_NAME, 'KeyType' => 'HASH'],
                ],
                'GlobalSecondaryIndexes' => $indexes,
                'StreamSpecification' => [
                    'StreamEnabled' => true,
                    'StreamViewType' => 'NEW_AND_OLD_IMAGES',
                ],
                'ProvisionedThroughput' => $this->getDefaultProvisionedThroughput()
            ]);

            $client->waitUntil('TableExists', ['TableName' => $tableName]);

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to create table [%s] in region [%s].',
                    ClassUtils::getShortName($this),
                    $tableName,
                    $client->getRegion()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    /**
     * Describes a DynamoDb table.
     *
     * @param DynamoDbClient $client
     * @param string         $tableName
     *
     * @return string
     *
     * @throws RepositoryOperationFailed
     */
    final public function describe(DynamoDbClient $client, $tableName)
    {
        try {
            $result = $client->describeTable(['TableName' => $tableName]);
            return json_encode($result->toArray(), JSON_PRETTY_PRINT);
        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to describe table [%s] in region [%s].',
                    ClassUtils::getShortName($this),
                    $tableName,
                    $client->getRegion()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    /**
     * Returns true if this NodeTable has the given index.
     * @see GlobalSecondaryIndex::getAlias()
     *
     * @param string $alias
     *
     * @return bool
     */
    final public function hasIndex($alias)
    {
        $this->loadIndexes();
        return isset($this->gsi[$alias]);
    }

    /**
     * Returns an index by its alias if it exists on this table.
     *
     * @param string $alias
     *
     * @return GlobalSecondaryIndex|null
     */
    final public function getIndex($alias)
    {
        $this->loadIndexes();
        return $this->gsi[$alias] ?? null;
    }

    /**
     * Calls all of the indexes on this table "beforePutItem" methods.
     *
     * @param array $item
     * @param Node  $node
     */
    final public function beforePutItem(array &$item, Node $node)
    {
        $this->loadIndexes();
        $this->addShardAttributes($item, $node);
        if ($node instanceof Indexed) {
            $item[NodeTable::INDEXED_KEY_NAME] = ['BOOL' => true];
        }

        foreach ($this->gsi as $gsi) {
            $gsi->beforePutItem($item, $node);
        }

        $this->doBeforePutItem($item, $node);
    }

    /**
     * Add derived/virtual fields to the item before pushing to DynamoDb.
     * Typically used to create a composite index, shards for distributed
     * parallel scans (not generally for GSI).
     *
     * @param array $item
     * @param Node  $node
     */
    protected function doBeforePutItem(array &$item, Node $node)
    {
        // override to customize
    }

    /**
     * A common use case is to run a parallel scan to reindex or reprocess
     * nodes.  The shard fields allow you to run the parallel scans in
     * parallel by using a filter expression for the "__s#" field.
     *
     * For example, parallel scan 16 separate processes with "__s16"
     * having a value of 0-15.
     *
     * @param array $item
     * @param Node  $node
     */
    protected function addShardAttributes(array &$item, Node $node)
    {
        foreach ([16, 32, 64, 128, 256] as $shard) {
            $item["__s{$shard}"] = ['N' => (string)ShardUtils::determineShard($item['_id']['S'], $shard)];
        }
    }

    /**
     * @return GlobalSecondaryIndex[]
     */
    protected function getIndexes()
    {
        return [
            new SlugIndex()
        ];
    }

    /**
     * When creating tables and GSI this provisioning will be used.
     *
     * @return array
     */
    protected function getDefaultProvisionedThroughput()
    {
        return ['ReadCapacityUnits' => 2, 'WriteCapacityUnits' => 2];
    }

    /**
     * Load the indexes for this table.
     */
    private function loadIndexes()
    {
        if (null !== $this->gsi) {
            return;
        }

        $this->gsi = [];
        foreach ($this->getIndexes() as $gsi) {
            $this->gsi[$gsi->getAlias()] = $gsi;
        }
    }
}
