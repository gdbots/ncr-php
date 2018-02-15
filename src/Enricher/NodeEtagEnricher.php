<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Enricher;

use Gdbots\Pbjx\DependencyInjection\PbjxEnricher;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;

final class NodeEtagEnricher implements EventSubscriber, PbjxEnricher
{
    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function enrichNodeCreated(PbjxEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getMessage();
        if ($event->isFrozen() || !$event->has('node')) {
            return;
        }

        /** @var Node $node */
        $node = $event->get('node');
        if ($node->isFrozen()) {
            return;
        }

        $node->set('etag', $node->generateEtag(['etag', 'updated_at']));
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function enrichNodeUpdated(PbjxEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getMessage();
        if ($event->isFrozen()) {
            return;
        }

        if ($event->has('new_node')) {
            /** @var Node $newNode */
            $newNode = $event->get('new_node');
            if (!$newNode->isFrozen()) {
                $newNode->set('etag', $newNode->generateEtag(['etag', 'updated_at']));
            }

            $event->set('new_etag', $newNode->get('etag'));
        }

        if ($event->has('old_node')) {
            /** @var Node $oldNode */
            $oldNode = $event->get('old_node');
            $event->set('old_etag', $oldNode->get('etag'));
        }
    }

    /**
     * {@inheritdoc}
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:node-created.enrich' => [['enrichNodeCreated', -5000]],
            'gdbots:ncr:mixin:node-updated.enrich' => [['enrichNodeUpdated', -5000]],
        ];
    }
}
