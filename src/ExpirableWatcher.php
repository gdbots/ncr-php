<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\ExpireNodeV1;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
use Gdbots\Schemas\Ncr\Mixin\Expirable\ExpirableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;

class ExpirableWatcher implements EventSubscriber
{
    public static function getSubscribedEvents()
    {
        return [
            ExpirableV1Mixin::SCHEMA_CURIE . '.created'     => 'schedule',
            ExpirableV1Mixin::SCHEMA_CURIE . '.deleted'     => 'cancel',
            ExpirableV1Mixin::SCHEMA_CURIE . '.expired'     => 'cancel',
            ExpirableV1Mixin::SCHEMA_CURIE . '.unpublished' => 'cancel',
            ExpirableV1Mixin::SCHEMA_CURIE . '.updated'     => 'reschedule',
        ];
    }

    public function cancel(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $nodeRef = $pbjxEvent->getNode()->generateNodeRef();
        $pbjxEvent::getPbjx()->cancelJobs(["{$nodeRef}.expire"]);
    }

    public function schedule(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $this->createJob($pbjxEvent->getNode(), $event, $pbjxEvent::getPbjx());
    }

    public function reschedule(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $pbjx = $pbjxEvent::getPbjx();
        $newNode = $pbjxEvent->getNode();
        /** @var Message $oldNode */
        $oldNode = $event->get(NodeUpdatedV1::OLD_NODE_FIELD);
        $nodeRef = $event->get(NodeUpdatedV1::NODE_REF_FIELD) ?: $newNode->generateNodeRef();
        $oldExpiresAt = $oldNode ? $oldNode->fget(ExpirableV1Mixin::EXPIRES_AT_FIELD) : null;
        $newExpiresAt = $newNode->fget(ExpirableV1Mixin::EXPIRES_AT_FIELD);

        if ($oldExpiresAt === $newExpiresAt) {
            return;
        }

        if (null === $newExpiresAt) {
            if (null !== $oldExpiresAt) {
                $pbjx->cancelJobs(["{$nodeRef}.expire"]);
            }
            return;
        }

        $this->createJob($newNode, $event, $pbjx);
    }

    protected function createJob(Message $node, Message $event, Pbjx $pbjx): void
    {
        if (!$node->has(ExpirableV1Mixin::EXPIRES_AT_FIELD)) {
            return;
        }

        $nodeRef = $node->generateNodeRef();
        $command = ExpireNodeV1::create()->set(ExpireNodeV1::NODE_REF_FIELD, $nodeRef);
        /** @var \DateTimeInterface $expiresAt */
        $expiresAt = $node->get(ExpirableV1Mixin::EXPIRES_AT_FIELD);
        $timestamp = $expiresAt->getTimestamp();

        if ($timestamp <= time()) {
            if (NodeStatus::EXPIRED()->equals($node->get(NodeV1Mixin::STATUS_FIELD))) {
                return;
            }
            $timestamp = strtotime('+5 seconds');
        }

        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $timestamp, "{$nodeRef}.expire");
    }
}
