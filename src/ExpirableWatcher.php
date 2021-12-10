<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\ExpireNodeV1;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;

class ExpirableWatcher implements EventSubscriber
{
    public static function getSubscribedEvents(): array
    {
        return [
            'gdbots:ncr:mixin:expirable.created'     => 'schedule',
            'gdbots:ncr:mixin:expirable.deleted'     => 'cancel',
            'gdbots:ncr:mixin:expirable.expired'     => 'cancel',
            'gdbots:ncr:mixin:expirable.unpublished' => 'cancel',
            'gdbots:ncr:mixin:expirable.updated'     => 'reschedule',
        ];
    }

    public function cancel(NodeProjectedEvent $pbjxEvent): void
    {
        $event = $pbjxEvent->getLastEvent();
        if ($event->isReplay()) {
            return;
        }

        $nodeRef = $pbjxEvent->getNode()->generateNodeRef();
        $pbjxEvent::getPbjx()->cancelJobs(["{$nodeRef}.expire"], ['causator' => $event]);
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
        $oldNode = $event->get('old_node');
        $nodeRef = $event->get('node_ref') ?: $newNode->generateNodeRef();
        $oldExpiresAt = $oldNode?->fget('expires_at');
        $newExpiresAt = $newNode->fget('expires_at');

        if ($oldExpiresAt === $newExpiresAt) {
            return;
        }

        if (null === $newExpiresAt) {
            if (null !== $oldExpiresAt) {
                $pbjx->cancelJobs(["{$nodeRef}.expire"], ['causator' => $event]);
            }
            return;
        }

        $this->createJob($newNode, $event, $pbjx);
    }

    protected function createJob(Message $node, Message $event, Pbjx $pbjx): void
    {
        if (!$node->has('expires_at')) {
            return;
        }

        $nodeRef = $node->generateNodeRef();
        $command = ExpireNodeV1::create()->set('node_ref', $nodeRef);
        /** @var \DateTimeInterface $expiresAt */
        $expiresAt = $node->get('expires_at');
        $timestamp = $expiresAt->getTimestamp();

        if ($timestamp <= time()) {
            if (NodeStatus::EXPIRED === $node->get('status')) {
                return;
            }
            $timestamp = strtotime('+5 seconds');
        }

        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $timestamp, "{$nodeRef}.expire");
    }
}
