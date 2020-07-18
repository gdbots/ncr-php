<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\Command\PublishNodeV1;

class PublishableWatcher implements EventSubscriber
{
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:publishable.deleted'           => 'cancel',
            'gdbots:ncr:mixin:publishable.expired'           => 'cancel',
            'gdbots:ncr:mixin:publishable.marked-as-draft'   => 'cancel',
            'gdbots:ncr:mixin:publishable.marked-as-pending' => 'cancel',
            'gdbots:ncr:mixin:publishable.published'         => 'cancel',
            'gdbots:ncr:mixin:publishable.scheduled'         => 'schedule',
            'gdbots:ncr:mixin:publishable.unpublished'       => 'cancel',
        ];
    }

    public function cancel(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $nodeRef = $pbjxEvent->getNode()->generateNodeRef();
        $pbjxEvent::getPbjx()->cancelJobs(["{$nodeRef}.publish"], ['causator' => $event]);
    }

    public function schedule(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $pbjx = $pbjxEvent::getPbjx();
        $node = $pbjxEvent->getNode();

        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref') ?: $node->generateNodeRef();
        /** @var \DateTimeInterface $publishAt */
        $publishAt = $event->get('publish_at');

        $command = PublishNodeV1::create()
            ->set('node_ref', $nodeRef)
            ->set('publish_at', $publishAt);

        $timestamp = $publishAt->getTimestamp();
        if ($timestamp <= time()) {
            $timestamp = strtotime('+5 seconds');
        }

        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $timestamp, "{$nodeRef}.publish");
    }
}
