<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\IndexQuery;
use Gdbots\Pbj\Message;

interface GlobalSecondaryIndex
{
    /**
     * Returns the alias of the index that can be used when executing an IndexQuery.
     *
     * This should be a friendly name, all lowercase, no special characters
     * and NOT be platform/provider specific.
     *
     * For example, "slug", "email", "publish_date".
     *
     * @return string
     *
     * @see IndexQuery
     */
    public function getAlias(): string;

    /**
     * Returns the name of the index that should be used to create the actual index
     * on the DynamoDb table.  This is generally the "{$alias}_index".
     *
     * @return string
     */
    public function getName(): string;

    /**
     * Returns the AttributeName for the KeySchema KeyType "HASH".
     *
     * @return string
     */
    public function getHashKeyName(): string;

    /**
     * Returns the AttributeName for the KeySchema KeyType "RANGE".
     * This is optional.
     *
     * @return string|null
     */
    public function getRangeKeyName(): ?string;

    /**
     * Returns attributes used in the KeySchema for a GSI as they must also be defined
     * when creating the table.
     *
     * @link https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#DDB-CreateTable-request-AttributeDefinitions
     *
     * @return array
     */
    public function getKeyAttributes(): array;

    /**
     * Returns attributes that can be used in the IndexQueryFilter. The name of the
     * field used in the IndexQueryFilter corresponds to the "alias_name".
     *
     * Expected format:
     *  [
     *      'alias_name' => ['AttributeName' => 'real_attribute_name', 'AttributeType' => 'S']
     *  ]
     *
     * @return array
     *
     * @see IndexQueryFilter
     */
    public function getFilterableAttributes(): array;

    /**
     * Return the configuration for the GSI projection.
     *
     * For details on the format of the parameters:
     * @link https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Projection.html
     *
     * @return array
     */
    public function getProjection(): array;

    /**
     * Hook to modify the item before it's pushed to DynamoDb.  It's possible that
     * this won't change the item at all if the index shouldn't be applied which makes
     * it possible to create a sparse GSI.
     *
     * For example, not setting the index attribute if the node is not of a certain status.
     *
     * @param array   $item
     * @param Message $node
     */
    public function beforePutItem(array &$item, Message $node): void;

    /**
     * Converts the IndexQuery into the parameters needed to perform a DynamoDb query.
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
     *
     * @param IndexQuery $query
     *
     * @return array
     */
    public function createQuery(IndexQuery $query): array;
}
