<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Pbj\Message;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\SluggableV1Mixin;

final class SlugIndex extends AbstractIndex
{
    public function getAlias(): string
    {
        return 'slug';
    }

    public function getHashKeyName(): string
    {
        return '__slug';
    }

    public function getRangeKeyName(): ?string
    {
        return NodeV1Mixin::CREATED_AT_FIELD;
    }

    public function getKeyAttributes(): array
    {
        return [
            ['AttributeName' => $this->getHashKeyName(), 'AttributeType' => 'S'],
            ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
        ];
    }

    public function getFilterableAttributes(): array
    {
        return [
            'created_at' => ['AttributeName' => $this->getRangeKeyName(), 'AttributeType' => 'N'],
            'status'     => ['AttributeName' => NodeV1Mixin::STATUS_FIELD, 'AttributeType' => 'S'],
            'etag'       => ['AttributeName' => NodeV1Mixin::ETAG_FIELD, 'AttributeType' => 'S'],
        ];
    }

    public function getProjection(): array
    {
        return [
            'ProjectionType'   => 'INCLUDE',
            'NonKeyAttributes' => [NodeV1Mixin::STATUS_FIELD, NodeV1Mixin::ETAG_FIELD],
        ];
    }

    public function beforePutItem(array &$item, Message $node): void
    {
        if (!$node->has(SluggableV1Mixin::SLUG_FIELD)
            || $node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::DELETED())
            || !$node::schema()->hasMixin(SluggableV1Mixin::SCHEMA_CURIE)
        ) {
            return;
        }

        $item[$this->getHashKeyName()] = ['S' => (string)$node->get(SluggableV1Mixin::SLUG_FIELD)];
    }
}
