<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Pbj\Message;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;

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
        return 'created_at';
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
            'status'     => ['AttributeName' => 'status', 'AttributeType' => 'S'],
            'etag'       => ['AttributeName' => 'etag', 'AttributeType' => 'S'],
        ];
    }

    public function getProjection(): array
    {
        return [
            'ProjectionType'   => 'INCLUDE',
            'NonKeyAttributes' => ['status', 'etag'],
        ];
    }

    public function beforePutItem(array &$item, Message $node): void
    {
        if (!$node->has('slug')
            || $node->fget('status') === NodeStatus::DELETED
            || !$node::schema()->hasMixin('gdbots:ncr:mixin:sluggable')
        ) {
            return;
        }

        $item[$this->getHashKeyName()] = ['S' => (string)$node->get('slug')];
    }
}
