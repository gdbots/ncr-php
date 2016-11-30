<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Schemas\Ncr\Mixin\Node\Node;

interface GlobalSecondaryIndex
{
    /**
     * Returns the name of the index that can be used when calling @see Ncr::getNodeByIndex
     * This should be all lowercase, no special characters and NOT be platform/provider specific.
     *
     * For example, "slug", "email", "publish_date".
     *
     * @return string
     */
    public function getName();

    /**
     * Returns the AttributeName for the KeySchema KeyType "HASH".
     *
     * @return string
     */
    public function getHashKeyName();

    /**
     * Returns the AttributeName for the KeySchema KeyType "RANGE".
     * This is optional.
     *
     * @return string|null
     */
    public function getRangeKeyName();

    /**
     * Return attributes used in the KeySchema for a GSI as they must also be defined
     * when creating the table.
     *
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#DDB-CreateTable-request-AttributeDefinitions
     *
     * @return array
     */
    public function getAttributeDefinitions();

    /**
     * Return the configuration for the GSI projection.
     *
     * For details on the format of the parameters:
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Projection.html
     *
     * @return array
     */
    public function getProjection();

    /**
     * Applies the index to the item before it's pushed to DynamoDb.  It's possible that
     * this won't change the item at all if the index shouldn't be applied which makes
     * it possible to create a sparse GSI.
     *
     * For example, not setting the index attribute if the node is not of a certain status.
     *
     * @param array $item
     * @param Node $node
     */
    public function applyToItem(array &$item, Node $node);
}
