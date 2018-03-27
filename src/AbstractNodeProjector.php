<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\LogicException;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\DependencyInjection\PbjxProjector;
use Gdbots\Pbjx\EventSubscriberTrait;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Expirable\Expirable;
use Gdbots\Schemas\Ncr\Mixin\ExpireNode\ExpireNode;
use Gdbots\Schemas\Ncr\Mixin\Indexed\Indexed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\NodeCreated\NodeCreated;
use Gdbots\Schemas\Ncr\Mixin\NodeDeleted\NodeDeleted;
use Gdbots\Schemas\Ncr\Mixin\NodeExpired\NodeExpired;
use Gdbots\Schemas\Ncr\Mixin\NodeLocked\NodeLocked;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsDraft\NodeMarkedAsDraft;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsPending\NodeMarkedAsPending;
use Gdbots\Schemas\Ncr\Mixin\NodePublished\NodePublished;
use Gdbots\Schemas\Ncr\Mixin\NodeRenamed\NodeRenamed;
use Gdbots\Schemas\Ncr\Mixin\NodeScheduled\NodeScheduled;
use Gdbots\Schemas\Ncr\Mixin\NodeUnlocked\NodeUnlocked;
use Gdbots\Schemas\Ncr\Mixin\NodeUnpublished\NodeUnpublished;
use Gdbots\Schemas\Ncr\Mixin\NodeUpdated\NodeUpdated;
use Gdbots\Schemas\Ncr\Mixin\Publishable\Publishable;
use Gdbots\Schemas\Ncr\Mixin\PublishNode\PublishNode;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\Mixin\Event\Event;

abstract class AbstractNodeProjector implements PbjxProjector
{
    use EventSubscriberTrait;

    /** @var Ncr */
    protected $ncr;

    /** @var NcrSearch */
    protected $ncrSearch;

    /** @var bool */
    protected $useSoftDelete = true;

    /**
     * @param Ncr       $ncr
     * @param NcrSearch $ncrSearch
     */
    public function __construct(Ncr $ncr, NcrSearch $ncrSearch)
    {
        $this->ncr = $ncr;
        $this->ncrSearch = $ncrSearch;
    }

    /**
     * @param NodeCreated $event
     * @param Pbjx        $pbjx
     */
    protected function handleNodeCreated(NodeCreated $event, Pbjx $pbjx): void
    {
        /** @var Node $node */
        $node = $event->get('node');
        $this->ncr->putNode($node, null, $this->createNcrContext($event));

        if ($event->isReplay()) {
            return;
        }

        if ($node instanceof Indexed) {
            $this->ncrSearch->indexNodes([$node], $this->createNcrSearchContext($event));
        }

        if ($node instanceof Expirable) {
            $this->createExpireNodeJob($node, $event, $pbjx);
        }
    }

    /**
     * @param NodeDeleted $event
     * @param Pbjx        $pbjx
     */
    protected function handleNodeDeleted(NodeDeleted $event, Pbjx $pbjx): void
    {
        $context = $this->createNcrContext($event);
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $context);

        if ($this->useSoftDelete) {
            $node->set('status', NodeStatus::DELETED());
            $this->updateAndIndexNode($node, $event, $pbjx);
        } else {
            $this->ncr->deleteNode($nodeRef, $context);
            if ($node instanceof Indexed) {
                $this->ncrSearch->deleteNodes([$nodeRef], $this->createNcrSearchContext($event));
            }
        }

        if ($event->isReplay()) {
            return;
        }

        $jobs = [];

        if ($node instanceof Expirable) {
            $jobs[] = "{$nodeRef}.expire";
        }

        if ($node instanceof Publishable) {
            $jobs[] = "{$nodeRef}.publish";
        }

        if (!empty($jobs)) {
            $pbjx->cancelJobs($jobs);
        }
    }

    /**
     * @param NodeExpired $event
     * @param Pbjx        $pbjx
     */
    protected function handleNodeExpired(NodeExpired $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($event->get('node_ref'), true, $this->createNcrContext($event));
        $node->set('status', NodeStatus::EXPIRED());
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    /**
     * @param NodeLocked $event
     * @param Pbjx       $pbjx
     */
    protected function handleNodeLocked(NodeLocked $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

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
            $lockedByRef = NodeRef::fromString("{$nodeRef->getVendor()}:bot:system");
        }

        $node->set('is_locked', true)->set('locked_by_ref', $lockedByRef);
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    /**
     * @param NodeMarkedAsDraft $event
     * @param Pbjx              $pbjx
     */
    protected function handleNodeMarkedAsDraft(NodeMarkedAsDraft $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

        /** @var NodeStatus $prevStatus */
        $prevStatus = $node->get('status');
        $node->set('status', NodeStatus::DRAFT());
        $this->updateAndIndexNode($node, $event, $pbjx);

        if ($event->isReplay()) {
            return;
        }

        if ($prevStatus->equals(NodeStatus::SCHEDULED())) {
            $pbjx->cancelJobs(["{$nodeRef}.publish"]);
        }
    }

    /**
     * @param NodeMarkedAsPending $event
     * @param Pbjx                $pbjx
     */
    protected function handleNodeMarkedAsPending(NodeMarkedAsPending $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

        /** @var NodeStatus $prevStatus */
        $prevStatus = $node->get('status');
        $node->set('status', NodeStatus::PENDING());
        $this->updateAndIndexNode($node, $event, $pbjx);

        if ($event->isReplay()) {
            return;
        }

        if ($prevStatus->equals(NodeStatus::SCHEDULED())) {
            $pbjx->cancelJobs(["{$nodeRef}.publish"]);
        }
    }

    /**
     * @param NodePublished $event
     * @param Pbjx          $pbjx
     */
    protected function handleNodePublished(NodePublished $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($event->get('node_ref'), true, $this->createNcrContext($event));
        $node->set('status', NodeStatus::PUBLISHED())
            ->set('published_at', $event->get('published_at'));
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    /**
     * @param NodeRenamed $event
     * @param Pbjx        $pbjx
     */
    protected function handleNodeRenamed(NodeRenamed $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($event->get('node_ref'), true, $this->createNcrContext($event));
        $node->set('slug', $event->get('new_slug'));
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    /**
     * @param NodeScheduled $event
     * @param Pbjx          $pbjx
     */
    protected function handleNodeScheduled(NodeScheduled $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

        /** @var \DateTime $publishAt */
        $publishAt = $event->get('publish_at');
        $node->set('status', NodeStatus::SCHEDULED())->set('published_at', $publishAt);
        $this->updateAndIndexNode($node, $event, $pbjx);

        if ($event->isReplay()) {
            return;
        }

        $command = $this->createPublishNode($node, $event, $pbjx)
            ->set('node_ref', $event->get('node_ref'))
            ->set('publish_at', $publishAt);

        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $publishAt->getTimestamp(), "{$nodeRef}.publish");
    }

    /**
     * @param NodeUnlocked $event
     * @param Pbjx         $pbjx
     */
    protected function handleNodeUnlocked(NodeUnlocked $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($event->get('node_ref'), true, $this->createNcrContext($event));
        $node->set('is_locked', false)->clear('locked_by_ref');
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    /**
     * @param NodeUnpublished $event
     * @param Pbjx            $pbjx
     */
    protected function handleNodeUnpublished(NodeUnpublished $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

        $node->set('status', NodeStatus::DRAFT())->clear('published_at');
        $this->updateAndIndexNode($node, $event, $pbjx);

        if ($event->isReplay()) {
            return;
        }

        $jobs = ["{$nodeRef}.publish"];
        if ($node instanceof Expirable && $node->has('expires_at')) {
            $jobs[] = "{$nodeRef}.expire";
        }

        $pbjx->cancelJobs($jobs);
    }

    /**
     * @param NodeUpdated $event
     * @param Pbjx        $pbjx
     */
    protected function handleNodeUpdated(NodeUpdated $event, Pbjx $pbjx): void
    {
        /** @var Node $newNode */
        $newNode = $event->get('new_node');
        $expectedEtag = $event->isReplay() ? null : $event->get('old_etag');
        $this->ncr->putNode($newNode, $expectedEtag, $this->createNcrContext($event));

        if ($event->isReplay()) {
            return;
        }

        if ($newNode instanceof Indexed) {
            $this->ncrSearch->indexNodes([$newNode], $this->createNcrSearchContext($event));
        }

        if ($newNode instanceof Expirable) {
            $this->cancelOrCreateExpireNodeJob($newNode, $event, $pbjx, $event->get('old_node'));
        }
    }

    /**
     * @param Expirable $newNode
     * @param Event     $event
     * @param Pbjx      $pbjx
     * @param Expirable $oldNode
     */
    protected function cancelOrCreateExpireNodeJob(
        Expirable $newNode,
        Event $event,
        Pbjx $pbjx,
        ?Expirable $oldNode = null
    ): void {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref') ?: NodeRef::fromNode($newNode);
        $expiresAtField = $newNode::schema()->getField('expires_at');

        /** @var \DateTime $oldExpiresAt */
        $oldExpiresAt = $oldNode ? $oldNode->get('expires_at') : null;

        $oldExpiresAt = $expiresAtField->getType()->encode($oldExpiresAt, $expiresAtField);
        $newExpiresAt = $expiresAtField->getType()->encode($newNode->get('expires_at'), $expiresAtField);

        if ($oldExpiresAt === $newExpiresAt) {
            return;
        }

        if (null !== $oldExpiresAt && null === $newExpiresAt) {
            $pbjx->cancelJobs(["{$nodeRef}.expire"]);
            return;
        }

        if (null !== $newExpiresAt) {
            $this->createExpireNodeJob($newNode, $event, $pbjx);
            return;
        }
    }

    /**
     * @param Node  $node
     * @param Event $event
     * @param Pbjx  $pbjx
     */
    protected function updateAndIndexNode(Node $node, Event $event, Pbjx $pbjx): void
    {
        $expectedEtag = $node->get('etag');
        $node
            ->set('updated_at', $event->get('occurred_at'))
            ->set('updater_ref', $event->get('ctx_user_ref'))
            ->set('last_event_ref', $event->generateMessageRef())
            ->set('etag', $node->generateEtag(['etag', 'updated_at']));

        $this->ncr->putNode($node, $expectedEtag, $this->createNcrContext($event));

        if ($event->isReplay()) {
            // on replay we don't want to reindex, we generally do that
            // as a separate task, in batches, using console ncr:reindex-nodes
            return;
        }

        if ($node instanceof Indexed) {
            $this->ncrSearch->indexNodes([$node], $this->createNcrSearchContext($event));
        }
    }

    /**
     * @param Message $message
     *
     * @return array
     */
    protected function createNcrContext(Message $message): array
    {
        return [];
    }

    /**
     * @param Message $message
     *
     * @return array
     */
    protected function createNcrSearchContext(Message $message): array
    {
        return [];
    }

    /**
     * @param Node  $node
     * @param Event $event
     * @param Pbjx  $pbjx
     *
     * @return ExpireNode
     *
     * @throws LogicException
     */
    protected function createExpireNode(Node $node, Event $event, Pbjx $pbjx): ExpireNode
    {
        throw new LogicException('You must implement createExpireNode.', Code::UNIMPLEMENTED);
    }

    /**
     * @param Expirable $node
     * @param Event     $event
     * @param Pbjx      $pbjx
     */
    protected function createExpireNodeJob(Expirable $node, Event $event, Pbjx $pbjx): void
    {
        if (!$node->has('expires_at')) {
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = NodeRef::fromNode($node);

        /** @var \DateTime $expiresAt */
        $expiresAt = $node->get('expires_at');

        $command = $this->createExpireNode($node, $event, $pbjx)->set('node_ref', $nodeRef);
        $pbjx->copyContext($event, $command);
        $pbjx->sendAt($command, $expiresAt->getTimestamp(), "{$nodeRef}.expire");
    }

    /**
     * @param Node  $node
     * @param Event $event
     * @param Pbjx  $pbjx
     *
     * @return PublishNode
     *
     * @throws LogicException
     */
    protected function createPublishNode(Node $node, Event $event, Pbjx $pbjx): PublishNode
    {
        throw new LogicException('You must implement createPublishNode.', Code::UNIMPLEMENTED);
    }
}