<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Enricher;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\DependencyInjection\PbjxEnricher;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeCreated\NodeCreatedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeUpdated\NodeUpdatedV1Mixin;

final class NodeEtagEnricher implements EventSubscriber, PbjxEnricher
{
    const IGNORED_FIELDS = [
        NodeV1Mixin::ETAG_FIELD,
        NodeV1Mixin::UPDATED_AT_FIELD,
        NodeV1Mixin::UPDATER_REF_FIELD,
        NodeV1Mixin::LAST_EVENT_REF_FIELD,
    ];

    public static function getSubscribedEvents()
    {
        return [
            // run these very late to ensure etag is set last
            NodeCreatedV1::SCHEMA_CURIE . '.enrich'      => ['enrichNodeCreated', -5000],
            NodeUpdatedV1::SCHEMA_CURIE . '.enrich'      => ['enrichNodeUpdated', -5000],
            // deprecated mixins, will be removed in 3.x
            NodeCreatedV1Mixin::SCHEMA_CURIE . '.enrich' => ['enrichNodeCreated', -5000],
            NodeUpdatedV1Mixin::SCHEMA_CURIE . '.enrich' => ['enrichNodeUpdated', -5000],
        ];
    }

    public function enrichNodeCreated(PbjxEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getMessage();
        if ($event->isFrozen() || !$event->has(NodeCreatedV1::NODE_FIELD)) {
            return;
        }

        /** @var Message $node */
        $node = $event->get(NodeCreatedV1::NODE_FIELD);
        if ($node->isFrozen()) {
            return;
        }

        $node->set(NodeV1Mixin::ETAG_FIELD, $node->generateEtag(self::IGNORED_FIELDS));
    }

    public function enrichNodeUpdated(PbjxEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getMessage();
        if ($event->isFrozen()) {
            return;
        }

        if ($event->has(NodeUpdatedV1::NEW_NODE_FIELD)) {
            /** @var Message $newNode */
            $newNode = $event->get(NodeUpdatedV1::NEW_NODE_FIELD);
            if (!$newNode->isFrozen()) {
                $newNode->set(NodeV1Mixin::ETAG_FIELD, $newNode->generateEtag(self::IGNORED_FIELDS));
            }

            $event->set(NodeUpdatedV1::NEW_ETAG_FIELD, $newNode->get(NodeV1Mixin::ETAG_FIELD));
        }

        if ($event->has(NodeUpdatedV1::OLD_NODE_FIELD)) {
            /** @var Message $oldNode */
            $oldNode = $event->get(NodeUpdatedV1::OLD_NODE_FIELD);
            $event->set(NodeUpdatedV1::OLD_ETAG_FIELD, $oldNode->get(NodeV1Mixin::ETAG_FIELD));
        }
    }
}
