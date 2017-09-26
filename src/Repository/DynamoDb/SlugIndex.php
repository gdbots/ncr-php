<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\Sluggable;

final class SlugIndex extends AbstractIndex
{
    /**
     * {@inheritdoc}
     */
    public function getAlias(): string
    {
        return 'slug';
    }

    /**
     * {@inheritdoc}
     */
    public function getHashKeyName(): string
    {
        return '__slug';
    }

    /**
     * {@inheritdoc}
     */
    public function getRangeKeyName(): ?string
    {
        return 'created_at';
    }

    /**
     * {@inheritdoc}
     */
    public function getKeyAttributes(): array
    {
        return [
            ['AttributeName' => $this->getHashKeyName(), 'AttributeType' => 'S'],
            ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getFilterableAttributes(): array
    {
        return [
            'created_at' => ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
            'status'     => ['AttributeName' => 'status', 'AttributeType' => 'S'],
            'etag'       => ['AttributeName' => 'etag', 'AttributeType' => 'S'],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getProjection(): array
    {
        return [
            'ProjectionType'   => 'INCLUDE',
            'NonKeyAttributes' => ['status', 'etag'],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function beforePutItem(array &$item, Node $node): void
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
