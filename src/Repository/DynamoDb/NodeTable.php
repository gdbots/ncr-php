<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbjx\Util\ShardUtil;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
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
    private ?array $gsi = null;

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
    final public function create(DynamoDbClient $client, string $tableName): void
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
                'IndexName'  => $indexName,
                'KeySchema'  => [
                    ['AttributeName' => $gsi->getHashKeyName(), 'KeyType' => 'HASH'],
                ],
                'Projection' => $gsi->getProjection() ?: ['ProjectionType' => 'KEYS_ONLY'],
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
                'TableName'              => $tableName,
                'AttributeDefinitions'   => $attributes,
                'KeySchema'              => [
                    ['AttributeName' => self::HASH_KEY_NAME, 'KeyType' => 'HASH'],
                ],
                'GlobalSecondaryIndexes' => $indexes,
                'StreamSpecification'    => [
                    'StreamEnabled'  => true,
                    'StreamViewType' => 'NEW_AND_OLD_IMAGES',
                ],
                'BillingMode'            => 'PAY_PER_REQUEST',
            ]);

            $client->waitUntil('TableExists', ['TableName' => $tableName]);
        } catch (\Throwable $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to create table [%s] in region [%s].',
                    ClassUtil::getShortName($this),
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
    final public function describe(DynamoDbClient $client, string $tableName): string
    {
        try {
            $result = $client->describeTable(['TableName' => $tableName]);
            return json_encode($result->toArray(), JSON_PRETTY_PRINT);
        } catch (\Throwable $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to describe table [%s] in region [%s].',
                    ClassUtil::getShortName($this),
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
     *
     * @param string $alias
     *
     * @return bool
     *
     * @see GlobalSecondaryIndex::getAlias()
     */
    final public function hasIndex(string $alias): bool
    {
        $this->loadIndexes();
        return isset($this->gsi[$alias]);
    }

    /**
     * Returns an index by its alias if it exists on this table.
     *
     * @param string $alias
     *
     * @return GlobalSecondaryIndex
     */
    final public function getIndex(string $alias): ?GlobalSecondaryIndex
    {
        $this->loadIndexes();
        return $this->gsi[$alias] ?? null;
    }

    /**
     * Calls all of the indexes on this table "beforePutItem" methods.
     *
     * @param array   $item
     * @param Message $node
     */
    final public function beforePutItem(array &$item, Message $node): void
    {
        $this->loadIndexes();
        $this->addShardAttributes($item, $node);

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
     * @param array   $item
     * @param Message $node
     */
    protected function doBeforePutItem(array &$item, Message $node): void
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
     * @param array   $item
     * @param Message $node
     */
    protected function addShardAttributes(array &$item, Message $node): void
    {
        foreach ([16, 32, 64, 128, 256] as $shard) {
            $item["__s{$shard}"] = ['N' => (string)ShardUtil::determineShard($item[NodeV1Mixin::_ID_FIELD]['S'], $shard)];
        }
    }

    /**
     * @return GlobalSecondaryIndex[]
     */
    protected function getIndexes(): array
    {
        return [
            new SlugIndex(),
        ];
    }

    /**
     * Load the indexes for this table.
     */
    private function loadIndexes(): void
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
