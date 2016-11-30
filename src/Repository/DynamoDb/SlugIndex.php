<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\Sluggable;

final class SlugIndex implements GlobalSecondaryIndex
{
    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'slug';
    }

    /**
     * {@inheritdoc}
     */
    public function getHashKeyName()
    {
        return '__slug';
    }

    /**
     * {@inheritdoc}
     */
    public function getRangeKeyName()
    {
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function getAttributeDefinitions()
    {
        return [
            ['AttributeName' => $this->getHashKeyName(), 'AttributeType' => 'S']
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getProjection()
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function beforePutItem(array &$item, Node $node)
    {
        if (!$node instanceof Sluggable
            || !$node->has('slug')
            || $node->get('status')->equals(NodeStatus::DELETED())
        ) {
            return;
        }

        $item[$this->getHashKeyName()] = ['S' => (string)$node->get('slug')];
    }
}
