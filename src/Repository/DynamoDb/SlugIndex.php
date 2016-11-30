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

    /**
     * @param array $item
     * @param Node $node
     */
    public function applyToItem(array &$item, Node $node)
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
