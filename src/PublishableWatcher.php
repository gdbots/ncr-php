<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\Command\PublishNodeV1;
use Gdbots\Schemas\Ncr\Event\NodeScheduledV1;
use Gdbots\Schemas\Ncr\Mixin\Publishable\PublishableV1Mixin;

class PublishableWatcher implements EventSubscriber
{
    public static function getSubscribedEvents()
    {
        return [
            PublishableV1Mixin::SCHEMA_CURIE . '.deleted'           => 'cancel',
            PublishableV1Mixin::SCHEMA_CURIE . '.expired'           => 'cancel',
            PublishableV1Mixin::SCHEMA_CURIE . '.marked-as-draft'   => 'cancel',
            PublishableV1Mixin::SCHEMA_CURIE . '.marked-as-pending' => 'cancel',
            PublishableV1Mixin::SCHEMA_CURIE . '.published'         => 'cancel',
            PublishableV1Mixin::SCHEMA_CURIE . '.scheduled'         => 'schedule',
            PublishableV1Mixin::SCHEMA_CURIE . '.unpublished'       => 'cancel',
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
        $nodeRef = $event->get(NodeScheduledV1::NODE_REF_FIELD) ?: $node->generateNodeRef();
        /** @var \DateTimeInterface $publishAt */
        $publishAt = $event->get(NodeScheduledV1::PUBLISH_AT_FIELD);

        $command = PublishNodeV1::create()
            ->set(PublishNodeV1::NODE_REF_FIELD, $nodeRef)
            ->set(PublishNodeV1::PUBLISH_AT_FIELD, $publishAt);

        $timestamp = $publishAt->getTimestamp();
        if ($timestamp <= time()) {
            $timestamp = strtotime('+5 seconds');
        }

        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $timestamp, "{$nodeRef}.publish");
    }
}
