<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Ncr\Exception\LogicException;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\WellKnown\MessageRef;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsPendingV1;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Publishable\PublishableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\SluggableV1Mixin;
use Gdbots\Schemas\Pbjx\StreamId;

class Aggregate
{
    protected Message $node;
    protected NodeRef $nodeRef;
    protected Pbjx $pbjx;

    /** @var Message[] */
    protected array $events = [];

    /**
     * When the aggregate is first created from a node/snapshot
     * or from a NodeRef we need to inform the sync process that
     * it should read the entire stream or use the last updated
     * at value from the node itself to determine where to start.
     *
     * @var bool
     */
    protected bool $syncAllEvents;

    /**
     * Creates a new instance of the aggregate with the initial
     * state being the provided node (aka snapshot).
     *
     * @param Message $node
     * @param Pbjx    $pbjx
     *
     * @return static
     */
    public static function fromNode(Message $node, Pbjx $pbjx): self
    {
        return new static($node, $pbjx, false);
    }

    /**
     * Creates a new instance of the aggregate without an initial
     * state (node/snapshot). The aggregate will create its own default state.
     *
     * @param NodeRef $nodeRef
     * @param Pbjx    $pbjx
     *
     * @return static
     */
    public static function fromNodeRef(NodeRef $nodeRef, Pbjx $pbjx): self
    {
        $node = MessageResolver::resolveQName($nodeRef->getQName())::fromArray([
            NodeV1Mixin::_ID_FIELD => $nodeRef->getId(),
        ]);

        return new static($node, $pbjx, true);
    }

    public static function generateEtag(Message $node): string
    {
        return $node->generateEtag([
            NodeV1Mixin::ETAG_FIELD,
            NodeV1Mixin::UPDATED_AT_FIELD,
            NodeV1Mixin::UPDATER_REF_FIELD,
            NodeV1Mixin::LAST_EVENT_REF_FIELD,
        ]);
    }

    protected function __construct(Message $node, Pbjx $pbjx, bool $syncAllEvents = false)
    {
        $this->nodeRef = $node->generateNodeRef();
        $this->node = $node->isFrozen() ? clone $node : $node;
        $this->pbjx = $pbjx;
        $this->syncAllEvents = $syncAllEvents;
    }

    public function useSoftDelete(): bool
    {
        return true;
    }

    public function hasUncommittedEvents(): bool
    {
        return !empty($this->events);
    }

    /**
     * Returns any events that have resulted from processing commands
     * that have yet to be committed.
     *
     * @return Message[]
     */
    public function getUncommittedEvents(): array
    {
        return $this->events;
    }

    /**
     * Clears all uncommitted events. Note that this does NOT restore
     * the state of the aggregate. To restore just create a new instance
     * with the original node (snapshot).
     */
    public function clearUncommittedEvents(): void
    {
        $this->events = [];
    }

    /**
     * Persists all of the uncommitted events to the EventStore.
     *
     * @param array $context Data that helps the EventStore decide where to read/write data from.
     */
    public function commit(array $context = []): void
    {
        if (!$this->hasUncommittedEvents()) {
            return;
        }

        $streamId = $this->getStreamId();
        $this->pbjx->getEventStore()->putEvents($streamId, $this->events, null, $context);
        $this->events = [];
        $this->syncAllEvents = false;
    }

    /**
     * Reads events from the EventStore for this aggregate's
     * stream and applies them to the state.
     *
     * @param array $context Data that helps the EventStore decide where to read/write data from.
     */
    public function sync(array $context = []): void
    {
        if ($this->hasUncommittedEvents()) {
            throw new LogicException(sprintf('The [%s] has uncommitted events.', $this->nodeRef->toString()));
        }

        $eventStore = $this->pbjx->getEventStore();
        $streamId = $this->getStreamId();
        $since = $this->syncAllEvents ? null : $this->getLastUpdatedAt();

        foreach ($eventStore->pipeEvents($streamId, $since, null, $context) as $event) {
            $this->applyEvent($event);
        }

        $this->syncAllEvents = false;
    }

    /**
     * Returns the current state of the aggregate as a node
     * which can be stored, serialized, etc. as a snapshot.
     *
     * @return Message
     */
    public function getNode(): Message
    {
        return clone $this->node;
    }

    public function getNodeRef(): NodeRef
    {
        return $this->nodeRef;
    }

    public function getStreamId(): StreamId
    {
        return StreamId::fromNodeRef($this->nodeRef);
    }

    public function getEtag(): ?string
    {
        return $this->node->get(NodeV1Mixin::ETAG_FIELD);
    }

    public function getLastEventRef(): ?MessageRef
    {
        return $this->node->get(NodeV1Mixin::LAST_EVENT_REF_FIELD);
    }

    public function getLastUpdatedAt(): Microtime
    {
        return $this->node->get(NodeV1Mixin::UPDATED_AT_FIELD)
            ?: $this->node->get(NodeV1Mixin::CREATED_AT_FIELD);
    }

    public function createNode(Message $command): void
    {
        /** @var Message $node */
        $node = clone $command->get($command::NODE_FIELD);
        $this->assertNodeRefMatches($node->generateNodeRef());

        $event = NodeCreatedV1::create();
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_FIELD, $node);

        $node
            ->clear(NodeV1Mixin::UPDATED_AT_FIELD)
            ->clear(NodeV1Mixin::UPDATER_REF_FIELD)
            ->set(NodeV1Mixin::CREATED_AT_FIELD, $event->get($event::OCCURRED_AT_FIELD))
            ->set(NodeV1Mixin::CREATOR_REF_FIELD, $event->get($event::CTX_USER_REF_FIELD))
            ->set(NodeV1Mixin::LAST_EVENT_REF_FIELD, $event->generateMessageRef());

        if ($node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            $node->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::DRAFT());
        } else {
            $node->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::PUBLISHED());
        }

        $this->recordEvent($event);
    }

    public function deleteNode(Message $command): void
    {
        if ($this->node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::DELETED())) {
            // node already deleted, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = NodeDeletedV1::create();
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        $this->recordEvent($event);
    }

    public function markNodeAsPending(Message $command): void
    {
        if ($this->node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::PENDING())) {
            // node already pending, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = NodeMarkedAsPendingV1::create();
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        $this->recordEvent($event);
    }

    protected function applyNodeCreated(Message $event): void
    {
        $this->node = clone $event->get(NodeCreatedV1::NODE_FIELD);
    }

    protected function applyNodeDeleted(Message $event): void
    {
        $this->node->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::DELETED());
    }

    protected function applyNodeMarkedAsPending(Message $event): void
    {
        $this->node->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::PENDING());
        if ($this->node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            $this->node->clear(PublishableV1Mixin::PUBLISHED_AT_FIELD);
        }
    }

    protected function enrichNodeCreated(Message $event): void
    {
        /** @var Message $node */
        $node = $event->get($command::NODE_FIELD);
        $node->set(NodeV1Mixin::ETAG_FIELD, static::generateEtag($node));
    }

    protected function assertNodeRefMatches(NodeRef $nodeRef): void
    {
        if ($this->nodeRef->equals($nodeRef)) {
            return;
        }

        throw new InvalidArgumentException(sprintf(
            '%s Provided NodeRef [%s] does not match [%s].',
            ClassUtil::getShortName(static::class),
            $this->nodeRef->toString(),
            $nodeRef->toString()
        ));
    }

    protected function applyEvent(Message $event): void
    {
        $method = $event::schema()->getHandlerMethodName(false, 'apply');
        $this->$method($event);
        $this->syncAllEvents = false;
        $eventRef = $event->generateMessageRef();

        if ($this->node->has(NodeV1Mixin::LAST_EVENT_REF_FIELD)
            && $eventRef->equals($this->node->get(NodeV1Mixin::LAST_EVENT_REF_FIELD))
        ) {
            // the apply* method already performed the updates
            // to updated and etag fields
            return;
        }

        $this->node
            ->set(NodeV1Mixin::UPDATED_AT_FIELD, $event->get($event::OCCURRED_AT_FIELD))
            ->set(NodeV1Mixin::UPDATER_REF_FIELD, $event->get($event::CTX_USER_REF_FIELD))
            ->set(NodeV1Mixin::LAST_EVENT_REF_FIELD, $eventRef)
            ->set(NodeV1Mixin::ETAG_FIELD, static::generateEtag($this->node));
    }

    protected function recordEvent(Message $event): void
    {
        $this->pbjx->triggerLifecycle($event);
        $method = $event::schema()->getHandlerMethodName(false, 'enrich');
        if (is_callable([$this, $method])) {
            $this->$method($event);
        }

        $this->events[] = $event->freeze();
        $this->applyEvent($event);
    }
}
