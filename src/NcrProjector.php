<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxProjector;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
use Gdbots\Schemas\Ncr\Event\NodeExpiredV1;
use Gdbots\Schemas\Ncr\Event\NodeLockedV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsDraftV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsPendingV1;
use Gdbots\Schemas\Ncr\Event\NodePublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeRenamedV1;
use Gdbots\Schemas\Ncr\Event\NodeScheduledV1;
use Gdbots\Schemas\Ncr\Event\NodeUnlockedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnpublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
use Gdbots\Schemas\Ncr\Mixin\NodeCreated\NodeCreatedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeDeleted\NodeDeletedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeExpired\NodeExpiredV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeLocked\NodeLockedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsDraft\NodeMarkedAsDraftV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsPending\NodeMarkedAsPendingV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodePublished\NodePublishedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeRenamed\NodeRenamedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeScheduled\NodeScheduledV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeUnlocked\NodeUnlockedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeUnpublished\NodeUnpublishedV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\NodeUpdated\NodeUpdatedV1Mixin;

class NcrProjector implements EventSubscriber, PbjxProjector
{
    protected const DEPRECATED_MIXINS_TO_SUFFIX = [
        NodeCreatedV1Mixin::SCHEMA_CURIE         => 'created',
        NodeDeletedV1Mixin::SCHEMA_CURIE         => 'deleted',
        NodeExpiredV1Mixin::SCHEMA_CURIE         => 'expired',
        NodeLockedV1Mixin::SCHEMA_CURIE          => 'locked',
        NodeMarkedAsDraftV1Mixin::SCHEMA_CURIE   => 'marked-as-draft',
        NodeMarkedAsPendingV1Mixin::SCHEMA_CURIE => 'marked-as-pending',
        NodePublishedV1Mixin::SCHEMA_CURIE       => 'published',
        NodeRenamedV1Mixin::SCHEMA_CURIE         => 'renamed',
        NodeScheduledV1Mixin::SCHEMA_CURIE       => 'scheduled',
        NodeUnlockedV1Mixin::SCHEMA_CURIE        => 'unlocked',
        NodeUnpublishedV1Mixin::SCHEMA_CURIE     => 'unpublished',
        NodeUpdatedV1Mixin::SCHEMA_CURIE         => 'updated',
    ];

    protected Ncr $ncr;
    protected NcrSearch $ncrSearch;
    protected bool $enabled;

    public static function getSubscribedEvents()
    {
        return [
            NodeCreatedV1::SCHEMA_CURIE              => 'onNodeCreated',
            NodeDeletedV1::SCHEMA_CURIE              => 'onNodeDeleted',
            NodeExpiredV1::SCHEMA_CURIE              => 'onNodeEvent',
            NodeLockedV1::SCHEMA_CURIE               => 'onNodeEvent',
            NodeMarkedAsDraftV1::SCHEMA_CURIE        => 'onNodeEvent',
            NodeMarkedAsPendingV1::SCHEMA_CURIE      => 'onNodeEvent',
            NodePublishedV1::SCHEMA_CURIE            => 'onNodeEvent',
            NodeRenamedV1::SCHEMA_CURIE              => 'onNodeEvent',
            NodeScheduledV1::SCHEMA_CURIE            => 'onNodeEvent',
            NodeUnlockedV1::SCHEMA_CURIE             => 'onNodeEvent',
            NodeUnpublishedV1::SCHEMA_CURIE          => 'onNodeEvent',
            NodeUpdatedV1::SCHEMA_CURIE              => 'onNodeUpdated',

            // deprecated mixins, will be removed in 3.x
            NodeCreatedV1Mixin::SCHEMA_CURIE         => 'onNodeCreated',
            NodeDeletedV1Mixin::SCHEMA_CURIE         => 'onNodeDeleted',
            NodeExpiredV1Mixin::SCHEMA_CURIE         => 'onNodeEvent',
            NodeLockedV1Mixin::SCHEMA_CURIE          => 'onNodeEvent',
            NodeMarkedAsDraftV1Mixin::SCHEMA_CURIE   => 'onNodeEvent',
            NodeMarkedAsPendingV1Mixin::SCHEMA_CURIE => 'onNodeEvent',
            NodePublishedV1Mixin::SCHEMA_CURIE       => 'onNodeEvent',
            NodeRenamedV1Mixin::SCHEMA_CURIE         => 'onNodeEvent',
            NodeScheduledV1Mixin::SCHEMA_CURIE       => 'onNodeEvent',
            NodeUnlockedV1Mixin::SCHEMA_CURIE        => 'onNodeEvent',
            NodeUnpublishedV1Mixin::SCHEMA_CURIE     => 'onNodeEvent',
            NodeUpdatedV1Mixin::SCHEMA_CURIE         => 'onNodeUpdated',
        ];
    }

    public function __construct(Ncr $ncr, NcrSearch $ncrSearch, bool $enabled = true)
    {
        $this->ncr = $ncr;
        $this->ncrSearch = $ncrSearch;
        $this->enabled = $enabled;
    }

    public function onNodeEvent(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    public function onNodeCreated(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNode($event->get(NodeCreatedV1::NODE_FIELD), $event, $pbjx);
    }

    public function onNodeDeleted(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get(NodeDeletedV1::NODE_REF_FIELD);
        $context = ['causator' => $event];

        try {
            $node = $this->ncr->getNode($nodeRef, true, $context);
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        } catch (NodeNotFound $nf) {
            // ignore already deleted nodes.
            return;
        } catch (\Throwable $e) {
            throw $e;
        }

        if ($aggregate->useSoftDelete()) {
            $this->projectNode($node, $event, $pbjx);
            return;
        }

        $this->ncr->deleteNode($nodeRef, $context);
        $this->ncrSearch->deleteNodes([$nodeRef], $context);
        $this->afterNodeProjected($node, $event, $pbjx);
    }

    public function onNodeUpdated(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNode($event->get(NodeUpdatedV1::NEW_NODE_FIELD), $event, $pbjx);
    }

    protected function projectNodeRef(NodeRef $nodeRef, Message $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($nodeRef, true, ['causator' => $event]);
        $this->projectNode($node, $event, $pbjx);
    }

    protected function projectNode(Message $node, Message $event, Pbjx $pbjx): void
    {
        $context = ['causator' => $event];
        $nodeRef = $node->generateNodeRef();
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $node = $aggregate->getNode();
        $this->ncr->putNode($node, null, $context);
        $this->ncrSearch->indexNodes([$node], $context);
        $this->afterNodeProjected($node, $event, $pbjx);
    }

    protected function afterNodeProjected(Message $node, Message $event, Pbjx $pbjx): void
    {
        $pbjxEvent = new NodeProjectedEvent($node, $event);
        $pbjx->trigger($node, 'projected', $pbjxEvent, false);
        $pbjx->trigger($node, $this->createProjectedEventSuffix($node, $event), $pbjxEvent, false);
    }

    protected function createProjectedEventSuffix(Message $node, Message $event): string
    {
        $schema = $event::schema();
        $id = $schema->getId();
        if ('gdbots' === $id->getVendor() && 'ncr' === $id->getPackage()) {
            return str_replace('node-', '', $id->getMessage());
        }

        foreach (static::DEPRECATED_MIXINS_TO_SUFFIX as $mixin => $suffix) {
            if ($schema->hasMixin($mixin)) {
                return $suffix;
            }
        }

        return $id->getMessage();
    }
}
