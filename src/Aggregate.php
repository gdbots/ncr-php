<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Ncr\Exception\LogicException;
use Gdbots\Ncr\Exception\NodeAlreadyLocked;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\Util\SlugUtil;
use Gdbots\Pbj\WellKnown\MessageRef;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
use Gdbots\Schemas\Ncr\Event\NodeExpiredV1;
use Gdbots\Schemas\Ncr\Event\NodeLabelsUpdatedV1;
use Gdbots\Schemas\Ncr\Event\NodeLockedV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsDraftV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsPendingV1;
use Gdbots\Schemas\Ncr\Event\NodePublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeRenamedV1;
use Gdbots\Schemas\Ncr\Event\NodeScheduledV1;
use Gdbots\Schemas\Ncr\Event\NodeTagsUpdatedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnlockedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnpublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
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
            '_id' => $nodeRef->getId(),
        ]);

        return new static($node, $pbjx, true);
    }

    public static function generateEtag(Message $node): string
    {
        return $node->generateEtag([
            'etag',
            'updated_at',
            'updater_ref',
            'last_event_ref',
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
        return $this->node->get('etag');
    }

    public function getLastEventRef(): ?MessageRef
    {
        return $this->node->get('last_event_ref');
    }

    public function getLastUpdatedAt(): Microtime
    {
        return $this->node->get('updated_at') ?: $this->node->get('created_at');
    }

    public function createNode(Message $command): void
    {
        /** @var Message $node */
        $node = clone $command->get('node');
        $this->assertNodeRefMatches($node->generateNodeRef());

        $event = $this->createNodeCreatedEvent($command);
        $this->copyContext($command, $event);
        $event->set('node', $node);

        $node
            ->clear('updated_at')
            ->clear('updater_ref')
            ->set('created_at', $event->get('occurred_at'))
            ->set('creator_ref', $event->get('ctx_user_ref'))
            ->set('last_event_ref', $event->generateMessageRef());

        if ($node::schema()->hasMixin('gdbots:ncr:mixin:publishable')) {
            $node->set('status', NodeStatus::DRAFT());
        } else {
            $node->set('status', NodeStatus::PUBLISHED());
        }

        $this->recordEvent($event);
    }

    public function deleteNode(Message $command): void
    {
        if ($this->node->get('status')->equals(NodeStatus::DELETED())) {
            // node already deleted, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeDeletedEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function expireNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:expirable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:expirable]."
            );
        }

        /** @var NodeStatus $currStatus */
        $currStatus = $this->node->get('status');
        if ($currStatus->equals(NodeStatus::DELETED()) || $currStatus->equals(NodeStatus::EXPIRED())) {
            // already expired or soft-deleted nodes can be ignored
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeExpiredEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function lockNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:lockable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:lockable]."
            );
        }

        if ($this->node->get('is_locked')) {
            if ($command->has('ctx_user_ref')) {
                $userNodeRef = NodeRef::fromMessageRef($command->get('ctx_user_ref'));
                if ((string)$this->node->get('locked_by_ref') === (string)$userNodeRef) {
                    // if it's the same user we can ignore it because they already own the lock
                    return;
                }
            }

            throw new NodeAlreadyLocked();
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeLockedEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function markNodeAsDraft(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:publishable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:publishable]."
            );
        }

        if ($this->node->get('status')->equals(NodeStatus::DRAFT())) {
            // node already draft, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeMarkedAsDraftEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function markNodeAsPending(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:publishable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:publishable]."
            );
        }

        if ($this->node->get('status')->equals(NodeStatus::PENDING())) {
            // node already pending, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeMarkedAsPendingEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function publishNode(Message $command, ?\DateTimeZone $localTimeZone = null): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:publishable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:publishable]."
            );
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        /** @var \DateTimeInterface $publishAt */
        $publishAt = $command->get('publish_at') ?: $command->get('occurred_at')->toDateTime();
        /*
         * If the node will publish within 15 seconds then we'll
         * just publish it now rather than schedule it.
         */
        $now = time() + 15;

        /** @var NodeStatus $currStatus */
        $currStatus = $this->node->get('status');
        $currPublishedAt = $this->node->has('published_at') ? $this->node->get('published_at')->getTimestamp() : null;

        if ($now >= $publishAt->getTimestamp()) {
            if ($currStatus->equals(NodeStatus::PUBLISHED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodePublishedEvent($command);
            $event->set('published_at', $publishAt);
        } else {
            if ($currStatus->equals(NodeStatus::SCHEDULED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodeScheduledEvent($command);
            $event->set('publish_at', $publishAt);
        }

        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $slug = $this->node->get('slug');
            if (null !== $localTimeZone && SlugUtil::containsDate($slug)) {
                $date = $publishAt instanceof \DateTimeImmutable
                    ? \DateTime::createFromImmutable($publishAt)
                    : clone $publishAt;
                $date->setTimezone($localTimeZone);
                $slug = SlugUtil::addDate($slug, $date);
            }
            $event->set('slug', $slug);
        }

        $this->recordEvent($event);
    }

    public function renameNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:sluggable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:sluggable]."
            );
        }

        if ($this->node->get('slug') === $command->get('new_slug')) {
            // ignore a pointless rename
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeRenamedEvent($command);
        $this->copyContext($command, $event);
        $event
            ->set('node_ref', $nodeRef)
            ->set('new_slug', $command->get('new_slug'))
            ->set('old_slug', $this->node->get('slug'))
            ->set('node_status', $this->node->get('status'));

        $this->recordEvent($event);
    }

    public function unlockNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:lockable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:lockable]."
            );
        }

        if (!$this->node->get('is_locked')) {
            // node already unlocked, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeUnlockedEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function unpublishNode(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:ncr:mixin:publishable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:ncr:mixin:publishable]."
            );
        }

        if (!$this->node->get('status')->equals(NodeStatus::PUBLISHED())) {
            // node already not published, ignore
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $event = $this->createNodeUnpublishedEvent($command);
        $this->copyContext($command, $event);
        $event->set('node_ref', $this->nodeRef);

        if ($this->node->has('slug')) {
            $event->set('slug', $this->node->get('slug'));
        }

        $this->recordEvent($event);
    }

    public function updateNode(Message $command): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        /** @var Message $newNode */
        $newNode = clone $command->get('new_node');
        $this->assertNodeRefMatches($newNode->generateNodeRef());

        $oldNode = (clone $this->node)->freeze();
        $event = $this->createNodeUpdatedEvent($command);
        $this->copyContext($command, $event);
        $event
            ->set('node_ref', $this->nodeRef)
            ->set('old_node', $oldNode)
            ->set('new_node', $newNode);

        $schema = $newNode::schema();

        if ($command->has('paths')) {
            $paths = $command->get('paths');
            $event->addToSet('paths', $paths);
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
            ->set('updated_at', $event->get('occurred_at'))
            ->set('updater_ref', $event->get('ctx_user_ref'))
            ->set('last_event_ref', $event->generateMessageRef())
            // status SHOULD NOT change during an update, use the appropriate
            // command to change a status (delete, publish, etc.)
            ->set('status', $oldNode->get('status'))
            // created_at and creator_ref MUST NOT change
            ->set('created_at', $oldNode->get('created_at'))
            ->set('creator_ref', $oldNode->get('creator_ref'));

        // labels SHOULD NOT change during an update, use "update-node-labels"
        if ($schema->hasMixin('gdbots:common:mixin:labelable')) {
            $newNode->setWithoutValidation('labels', $oldNode->fget('labels'));
        }

        // published_at SHOULD NOT change during an update, use "[un]publish-node"
        if ($schema->hasMixin('gdbots:ncr:mixin:publishable')) {
            $newNode->set('published_at', $oldNode->get('published_at'));
        }

        // slug SHOULD NOT change during an update, use "rename-node"
        if ($schema->hasMixin('gdbots:ncr:mixin:sluggable')) {
            $newNode->set('slug', $oldNode->get('slug'));
        }

        // is_locked and locked_by_ref SHOULD NOT change during an update, use "[un]lock-node"
        if ($schema->hasMixin('gdbots:ncr:mixin:lockable')) {
            $newNode
                ->set('is_locked', $oldNode->get('is_locked'))
                ->set('locked_by_ref', $oldNode->get('locked_by_ref'));
        }

        // if a node is being updated and it's deleted, restore the default status
        if (NodeStatus::DELETED()->equals($newNode->get('status'))) {
            $newNode->clear('status');
        }

        $this->recordEvent($event);
    }

    public function updateNodeLabels(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:common:mixin:labelable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:common:mixin:labelable]."
            );
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $added = array_values(array_filter(
            $command->get('add_labels', []),
            fn(string $label) => !$this->node->isInSet('labels', $label)
        ));

        $removed = array_values(array_filter(
            $command->get('remove_labels', []),
            fn(string $label) => $this->node->isInSet('labels', $label)
        ));

        if (empty($added) && empty($removed)) {
            return;
        }

        $event = NodeLabelsUpdatedV1::create();
        $this->copyContext($command, $event);
        $event
            ->set('node_ref', $this->nodeRef)
            ->addToSet('labels_added', $added)
            ->addToSet('labels_removed', $removed);

        $this->recordEvent($event);
    }

    public function updateNodeTags(Message $command): void
    {
        if (!$this->node::schema()->hasMixin('gdbots:common:mixin:taggable')) {
            throw new InvalidArgumentException(
                "Node [{$this->nodeRef}] must have [gdbots:common:mixin:taggable]."
            );
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->assertNodeRefMatches($nodeRef);

        $removed = array_values(array_filter(
            $command->get('remove_tags', []),
            fn(string $tag) => $this->node->isInMap('tags', $tag)
        ));

        if (!$command->has('add_tags') && empty($removed)) {
            return;
        }

        $event = NodeTagsUpdatedV1::create();
        $this->copyContext($command, $event);
        $event
            ->set('node_ref', $this->nodeRef)
            ->addToSet('tags_removed', $removed);

        foreach ($command->get('add_tags') as $k => $v) {
            $event->addToMap('tags_added', $k, $v);
        }

        $this->recordEvent($event);
    }

    protected function applyNodeCreated(Message $event): void
    {
        $this->node = clone $event->get('node');
    }

    protected function applyNodeDeleted(Message $event): void
    {
        $this->node->set('status', NodeStatus::DELETED());
    }

    protected function applyNodeExpired(Message $event): void
    {
        $this->node->set('status', NodeStatus::EXPIRED());
    }

    protected function applyNodeLabelsUpdated(Message $event): void
    {
        $this->node
            ->removeFromSet('labels', $event->get('labels_removed', []))
            ->addToSet('labels', $event->get('labels_added', []));
    }

    protected function applyNodeLocked(Message $event): void
    {
        if ($event->has('ctx_user_ref')) {
            $lockedByRef = NodeRef::fromMessageRef($event->get('ctx_user_ref'));
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
            ->set('is_locked', true)
            ->set('locked_by_ref', $lockedByRef);
    }

    protected function applyNodeMarkedAsDraft(Message $event): void
    {
        $this->node
            ->set('status', NodeStatus::DRAFT())
            ->clear('published_at');
    }

    protected function applyNodeMarkedAsPending(Message $event): void
    {
        $this->node
            ->set('status', NodeStatus::PENDING())
            ->clear('published_at');
    }

    protected function applyNodePublished(Message $event): void
    {
        $this->node
            ->set('status', NodeStatus::PUBLISHED())
            ->set('published_at', $event->get('published_at'));

        if ($event->has('slug')) {
            $this->node->set('slug', $event->get('slug'));
        }
    }

    protected function applyNodeRenamed(Message $event): void
    {
        $this->node->set('slug', $event->get('new_slug'));
    }

    protected function applyNodeScheduled(Message $event): void
    {
        $this->node
            ->set('status', NodeStatus::SCHEDULED())
            ->set('published_at', $event->get('publish_at'));

        if ($event->has('slug')) {
            $this->node->set('slug', $event->get('slug'));
        }
    }

    protected function applyNodeTagsUpdated(Message $event): void
    {
        foreach ($event->get('tags_removed', []) as $tag) {
            $this->node->removeFromMap('tags', $tag);
        }

        foreach ($event->get('tags_added', []) as $k => $v) {
            $this->node->addToMap('tags', $k, $v);
        }
    }

    protected function applyNodeUnlocked(Message $event): void
    {
        $this->node
            ->set('is_locked', false)
            ->clear('locked_by_ref');
    }

    protected function applyNodeUnpublished(Message $event): void
    {
        $this->node
            ->set('status', NodeStatus::DRAFT())
            ->clear('published_at');
    }

    protected function applyNodeUpdated(Message $event): void
    {
        $this->node = clone $event->get('new_node');
    }

    protected function enrichNodeCreated(Message $event): void
    {
        /** @var Message $node */
        $node = $event->get('node');
        $node->set('etag', static::generateEtag($node));
    }

    protected function enrichNodeUpdated(Message $event): void
    {
        /** @var Message $node */
        $node = $event->get('new_node');
        $node->set('etag', static::generateEtag($node));
        $event
            ->set('old_etag', $this->node->get('etag'))
            ->set('new_etag', $node->get('etag'));
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

        if ($this->node->has('last_event_ref') && $eventRef->equals($this->node->get('last_event_ref'))) {
            // the apply* method already performed the updates
            // to updated and etag fields
            return;
        }

        $this->node
            ->set('updated_at', $event->get('occurred_at'))
            ->set('updater_ref', $event->get('ctx_user_ref'))
            ->set('last_event_ref', $eventRef)
            ->set('etag', static::generateEtag($this->node));
    }

    protected function copyContext(Message $command, Message $event): void
    {
        $this->pbjx->copyContext($command, $event);
        if ($event::schema()->hasMixin('gdbots:common:mixin:taggable')) {
            foreach ($command->get('tags', []) as $k => $v) {
                $event->addToMap('tags', $k, $v);
            }
        }
    }

    protected function shouldRecordEvent(Message $event): bool
    {
        if (!$event->has('new_etag')) {
            return true;
        }

        $oldEtag = $event->get('old_etag');
        $newEtag = $event->get('new_etag');
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
