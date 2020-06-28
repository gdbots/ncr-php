<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Ncr\Exception\LogicException;
use Gdbots\Ncr\Exception\NodeAlreadyLocked;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\WellKnown\MessageRef;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Common\Mixin\Taggable\TaggableV1Mixin;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
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
use Gdbots\Schemas\Ncr\Mixin\Expirable\ExpirableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Lockable\LockableV1Mixin;
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

        $event = $this->createNodeCreatedEvent($command);
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

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
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

        $event = $this->createNodeDeletedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function expireNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(ExpirableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . ExpirableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        /** @var NodeStatus $currStatus */
        $currStatus = $this->node->get(NodeV1Mixin::STATUS_FIELD);
        if ($currStatus->equals(NodeStatus::DELETED()) || $currStatus->equals(NodeStatus::EXPIRED())) {
            // already expired or soft-deleted nodes can be ignored
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeExpiredEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function lockNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(LockableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . LockableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if ($this->node->get(LockableV1Mixin::IS_LOCKED_FIELD)) {
            if ($command->has($command::CTX_USER_REF_FIELD)) {
                $userNodeRef = NodeRef::fromMessageRef($command->get($command::CTX_USER_REF_FIELD));
                if ((string)$this->node->get(LockableV1Mixin::LOCKED_BY_REF_FIELD) === (string)$userNodeRef) {
                    // if it's the same user we can ignore it because they already own the lock
                    return;
                }
            }

            throw new NodeAlreadyLocked();
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeLockedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function markNodeAsDraft(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . PublishableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if ($this->node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::DRAFT())) {
            // node already draft, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeMarkedAsDraftEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function markNodeAsPending(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . PublishableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if ($this->node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::PENDING())) {
            // node already pending, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeMarkedAsPendingEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function publishNode(Message $command, ?\DateTimeZone $localTimeZone = null): void
    {
        if (!$this->node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . PublishableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        /** @var \DateTimeInterface $publishAt */
        $publishAt = $command->get($command::PUBLISH_AT_FIELD) ?: $command->get($command::OCCURRED_AT_FIELD)->toDateTime();
        /*
         * If the node will publish within 15 seconds then we'll
         * just publish it now rather than schedule it.
         */
        $now = time() + 15;

        /** @var NodeStatus $currStatus */
        $currStatus = $this->node->get(NodeV1Mixin::STATUS_FIELD);
        $currPublishedAt = $this->node->has(PublishableV1Mixin::PUBLISHED_AT_FIELD)
            ? $this->node->get(PublishableV1Mixin::PUBLISHED_AT_FIELD)->getTimestamp()
            : null;

        if ($now >= $publishAt->getTimestamp()) {
            if ($currStatus->equals(NodeStatus::PUBLISHED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodePublishedEvent($command);
            $event->set($event::PUBLISHED_AT_FIELD, $publishAt);
        } else {
            if ($currStatus->equals(NodeStatus::SCHEDULED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodeScheduledEvent($command);
            $event->set($event::PUBLISH_AT_FIELD, $publishAt);
        }

        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $slug = $this->node->get(SluggableV1Mixin::SLUG_FIELD);
            if (null !== $localTimeZone && SlugUtils::containsDate($slug)) {
                $date = $publishAt instanceof \DateTimeImmutable
                    ? \DateTime::createFromImmutable($publishAt)
                    : clone $publishAt;
                $date->setTimezone($localTimeZone);
                $slug = SlugUtils::addDate($slug, $date);
            }
            $event->set($event::SLUG_FIELD, $slug);
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function renameNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(SluggableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . SluggableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if ($this->node->get(SluggableV1Mixin::SLUG_FIELD) === $command->get($command::NEW_SLUG_FIELD)) {
            // ignore a pointless rename
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeRenamedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event
            ->set($event::NODE_REF_FIELD, $nodeRef)
            ->set($event::NEW_SLUG_FIELD, $command->get($command::NEW_SLUG_FIELD))
            ->set($event::OLD_SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD))
            ->set($event::NODE_STATUS_FIELD, $this->node->get(NodeV1Mixin::STATUS_FIELD));

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function unlockNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(LockableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . LockableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if (!$this->node->get(LockableV1Mixin::IS_LOCKED_FIELD)) {
            // node already unlocked, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeUnlockedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function unpublishNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [" . PublishableV1Mixin::SCHEMA_CURIE . "]."
            );
        }

        if (!$this->node->get(NodeV1Mixin::STATUS_FIELD)->equals(NodeStatus::PUBLISHED())) {
            // node already not published, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeUnpublishedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event->set($event::NODE_REF_FIELD, $this->nodeRef);

        if ($this->node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $event->set($event::SLUG_FIELD, $this->node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
        }

        $this->recordEvent($event);
    }

    public function updateNode(Message $command): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get($command::NODE_REF_FIELD);
        $this->assertNodeRefMatches($nodeRef);

        /** @var Message $newNode */
        $newNode = clone $command->get($command::NEW_NODE_FIELD);
        $this->assertNodeRefMatches($newNode->generateNodeRef());

        $oldNode = (clone $this->node)->freeze();
        $event = $this->createNodeUpdatedEvent($command);
        $this->pbjx->copyContext($command, $event);
        $event
            ->set($event::NODE_REF_FIELD, $this->nodeRef)
            ->set($event::OLD_NODE_FIELD, $oldNode)
            ->set($event::NEW_NODE_FIELD, $newNode);

        $schema = $newNode::schema();

        if ($command->has($event::PATHS_FIELD)) {
            $paths = $command->get($event::PATHS_FIELD);
            $event->addToSet($event::PATHS_FIELD, $paths);
            $paths = array_flip($paths);
            foreach ($schema->getFields() as $field) {
                $fieldName = $field->getName();
                if (isset($paths[$fieldName])) {
                    // this means we intended to set this value
                    // so leave it as is.
                    continue;
                }

                $newNode->setWithoutValidation($fieldName, $oldNode->fget($fieldName));
            }
        }

        $newNode
            ->set(NodeV1Mixin::UPDATED_AT_FIELD, $event->get($event::OCCURRED_AT_FIELD))
            ->set(NodeV1Mixin::UPDATER_REF_FIELD, $event->get($event::CTX_USER_REF_FIELD))
            ->set(NodeV1Mixin::LAST_EVENT_REF_FIELD, $event->generateMessageRef())
            // status SHOULD NOT change during an update, use the appropriate
            // command to change a status (delete, publish, etc.)
            ->set(NodeV1Mixin::STATUS_FIELD, $oldNode->get(NodeV1Mixin::STATUS_FIELD))
            // created_at and creator_ref MUST NOT change
            ->set(NodeV1Mixin::CREATED_AT_FIELD, $oldNode->get(NodeV1Mixin::CREATED_AT_FIELD))
            ->set(NodeV1Mixin::CREATOR_REF_FIELD, $oldNode->get(NodeV1Mixin::CREATOR_REF_FIELD));

        // published_at SHOULD NOT change during an update, use "[un]publish-node"
        if ($schema->hasMixin(PublishableV1Mixin::SCHEMA_CURIE)) {
            $newNode->set(PublishableV1Mixin::PUBLISHED_AT_FIELD, $oldNode->get(PublishableV1Mixin::PUBLISHED_AT_FIELD));
        }

        // slug SHOULD NOT change during an update, use "rename-node"
        if ($schema->hasMixin(SluggableV1Mixin::SCHEMA_CURIE)) {
            $newNode->set(SluggableV1Mixin::SLUG_FIELD, $oldNode->get(SluggableV1Mixin::SLUG_FIELD));
        }

        // is_locked and locked_by_ref SHOULD NOT change during an update, use "[un]lock-node"
        if ($schema->hasMixin(LockableV1Mixin::SCHEMA_CURIE)) {
            $newNode
                ->set(LockableV1Mixin::IS_LOCKED_FIELD, $oldNode->get(LockableV1Mixin::IS_LOCKED_FIELD))
                ->set(LockableV1Mixin::LOCKED_BY_REF_FIELD, $oldNode->get(LockableV1Mixin::LOCKED_BY_REF_FIELD));
        }

        // if a node is being updated and it's deleted, restore the default status
        if (NodeStatus::DELETED()->equals($newNode->get(NodeV1Mixin::STATUS_FIELD))) {
            $newNode->clear(NodeV1Mixin::STATUS_FIELD);
        }

        if ($event::schema()->hasMixin(TaggableV1Mixin::SCHEMA_CURIE)) {
            foreach ($command->get($event::TAGS_FIELD, []) as $k => $v) {
                $event->addToMap($event::TAGS_FIELD, $k, $v);
            }
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

    protected function applyNodeExpired(Message $event): void
    {
        $this->node->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::EXPIRED());
    }

    protected function applyNodeLocked(Message $event): void
    {
        if ($event->has(NodeLockedV1::CTX_USER_REF_FIELD)) {
            $lockedByRef = NodeRef::fromMessageRef($event->get(NodeLockedV1::CTX_USER_REF_FIELD));
        } else {
            /*
             * todo: make "bots" a first class citizen in iam services
             * this is not likely to ever occur (being locked without a user ref)
             * but if it did we'll fake our future bot strategy for now.  the
             * eventual solution is that bots will be like users but will perform
             * operations through pbjx endpoints only, not via the web clients.
             */
            $lockedByRef = NodeRef::fromString("{$this->nodeRef->getVendor()}:user:e3949dc0-4261-4731-beb0-d32e723de939");
        }

        $this->node
            ->set(LockableV1Mixin::IS_LOCKED_FIELD, true)
            ->set(LockableV1Mixin::LOCKED_BY_REF_FIELD, $lockedByRef);
    }

    protected function applyNodeMarkedAsDraft(Message $event): void
    {
        $this->node
            ->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::DRAFT())
            ->clear(PublishableV1Mixin::PUBLISHED_AT_FIELD);
    }

    protected function applyNodeMarkedAsPending(Message $event): void
    {
        $this->node
            ->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::PENDING())
            ->clear(PublishableV1Mixin::PUBLISHED_AT_FIELD);
    }

    protected function applyNodePublished(Message $event): void
    {
        $this->node
            ->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::PUBLISHED())
            ->set(PublishableV1Mixin::PUBLISHED_AT_FIELD, $event->get(NodePublishedV1::PUBLISHED_AT_FIELD));

        if ($event->has(NodePublishedV1::SLUG_FIELD)) {
            $this->node->set(SluggableV1Mixin::SLUG_FIELD, $event->get(NodePublishedV1::SLUG_FIELD));
        }
    }

    protected function applyNodeRenamed(Message $event): void
    {
        $this->node->set(SluggableV1Mixin::SLUG_FIELD, $event->get(NodeRenamedV1::NEW_SLUG_FIELD));
    }

    protected function applyNodeScheduled(Message $event): void
    {
        $this->node
            ->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::SCHEDULED())
            ->set(PublishableV1Mixin::PUBLISHED_AT_FIELD, $event->get(NodeScheduledV1::PUBLISH_AT_FIELD));

        if ($event->has(NodeScheduledV1::SLUG_FIELD)) {
            $this->node->set(SluggableV1Mixin::SLUG_FIELD, $event->get(NodeScheduledV1::SLUG_FIELD));
        }
    }

    protected function applyNodeUnlocked(Message $event): void
    {
        $this->node
            ->set(LockableV1Mixin::IS_LOCKED_FIELD, false)
            ->clear(LockableV1Mixin::LOCKED_BY_REF_FIELD);
    }

    protected function applyNodeUnpublished(Message $event): void
    {
        $this->node
            ->set(NodeV1Mixin::STATUS_FIELD, NodeStatus::DRAFT())
            ->clear(PublishableV1Mixin::PUBLISHED_AT_FIELD);
    }

    protected function applyNodeUpdated(Message $event): void
    {
        $this->node = clone $event->get(NodeUpdatedV1::NEW_NODE_FIELD);
    }

    protected function enrichNodeCreated(Message $event): void
    {
        /** @var Message $node */
        $node = $event->get(NodeCreatedV1::NODE_FIELD);
        $node->set(NodeV1Mixin::ETAG_FIELD, static::generateEtag($node));
    }

    protected function enrichNodeUpdated(Message $event): void
    {
        /** @var Message $node */
        $node = $event->get(NodeUpdatedV1::NEW_NODE_FIELD);
        $node->set(NodeV1Mixin::ETAG_FIELD, static::generateEtag($node));
        $event
            ->set(NodeUpdatedV1::OLD_ETAG_FIELD, $this->node->get(NodeV1Mixin::ETAG_FIELD))
            ->set(NodeUpdatedV1::NEW_ETAG_FIELD, $node->get(NodeV1Mixin::ETAG_FIELD));
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

    protected function shouldRecordEvent(Message $event): bool
    {
        if (!$event->has(NodeUpdatedV1::NEW_ETAG_FIELD)) {
            return true;
        }

        $oldEtag = $event->get(NodeUpdatedV1::OLD_ETAG_FIELD);
        $newEtag = $event->get(NodeUpdatedV1::NEW_ETAG_FIELD);
        return $oldEtag !== $newEtag;
    }

    protected function recordEvent(Message $event): void
    {
        $this->pbjx->triggerLifecycle($event);
        $method = $event::schema()->getHandlerMethodName(false, 'enrich');
        if (is_callable([$this, $method])) {
            $this->$method($event);
        }

        if (!$this->shouldRecordEvent($event)) {
            return;
        }

        $this->events[] = $event->freeze();
        $this->applyEvent($event);
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeCreatedEvent(Message $command): Message
    {
        return NodeCreatedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeDeletedEvent(Message $command): Message
    {
        return NodeDeletedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeExpiredEvent(Message $command): Message
    {
        return NodeExpiredV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeLockedEvent(Message $command): Message
    {
        return NodeLockedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeMarkedAsDraftEvent(Message $command): Message
    {
        return NodeMarkedAsDraftV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeMarkedAsPendingEvent(Message $command): Message
    {
        return NodeMarkedAsPendingV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodePublishedEvent(Message $command): Message
    {
        return NodePublishedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeRenamedEvent(Message $command): Message
    {
        return NodeRenamedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeScheduledEvent(Message $command): Message
    {
        return NodeScheduledV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeUnlockedEvent(Message $command): Message
    {
        return NodeUnlockedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeUnpublishedEvent(Message $command): Message
    {
        return NodeUnpublishedV1::create();
    }

    /**
     * This is for legacy uses of command/event mixins for common
     * ncr operations. It will be removed in 3.x.
     *
     * @param Message $command
     *
     * @return Message
     *
     * @deprecated Will be removed in 3.x.
     */
    protected function createNodeUpdatedEvent(Message $command): Message
    {
        return NodeUpdatedV1::create();
    }
}
