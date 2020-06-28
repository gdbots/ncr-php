<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Document;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\Util\DateUtil;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;

class NodeMapper
{
    /**
     * The mappers are constructed with "new $class" in the
     * IndexManager so the constructor must be consistent.
     */
    final public function __construct()
    {
    }

    public function beforeIndex(Document $document, Message $node): void
    {
        $document->set(
            IndexManager::CREATED_AT_ISO_FIELD_NAME,
            $node->get(NodeV1Mixin::CREATED_AT_FIELD)->toDateTime()->format(DateUtil::ISO8601_ZULU)
        );
    }
}
