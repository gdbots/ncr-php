<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\Sluggable;

final class SlugIndex extends AbstractIndex
{
    /**
     * {@inheritdoc}
     */
    public function getAlias()
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
        return 'created_at';
    }

    /**
     * {@inheritdoc}
     */
    public function getKeyAttributes()
    {
        return [
            ['AttributeName' => $this->getHashKeyName(), 'AttributeType' => 'S'],
            ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getFilterableAttributes()
    {
        return [
            'created_at' => ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
            'status' => ['AttributeName' => 'status', 'AttributeType' => 'S'],
            'etag' => ['AttributeName' => 'etag', 'AttributeType' => 'S'],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getProjection()
    {
        return [
            'ProjectionType' => 'INCLUDE',
            'NonKeyAttributes' => ['status', 'etag']
        ];
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


















