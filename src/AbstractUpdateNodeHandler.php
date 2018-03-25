<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\NodeUpdated\NodeUpdated;
use Gdbots\Schemas\Ncr\Mixin\UpdateNode\UpdateNode;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractUpdateNodeHandler extends AbstractNodeCommandHandler
{
    /** @var Ncr */
    protected $ncr;

    /**
     * @param Ncr $ncr
     */
    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    /**
     * @param UpdateNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(UpdateNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        // if we decide to allow the incoming old_node to be trusted
        // this is fetched by the server in NodeCommandBinder but there's
        // a slight chance it's stale by the time the handler runs since
        // the handler is not always in the same process (gearman).
        /*
        $oldNode = $command->has('old_node')
            ? $command->get('old_node')
            : $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        */
        $oldNode = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($oldNode);
        $oldNode->freeze();

        $event = $this->createNodeUpdated($command, $pbjx);
        $pbjx->copyContext($command, $event);

        /** @var Node $newNode */
        $newNode = clone $command->get('new_node');
        $this->assertIsNodeSupported($newNode);
        $newNodeRef = NodeRef::fromNode($newNode);
        if (!$nodeRef->equals($newNodeRef)) {
            throw new InvalidArgumentException(
                "The old [{$nodeRef}] and new [{$newNodeRef}] node refs must match."
            );
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

        $schema = $newNode::schema();
        if ($schema->hasMixin('gdbots:ncr:mixin:publishable')) {
            // published_at SHOULD NOT change during an update, use "[un]publish-node"
            $newNode->set('published_at', $oldNode->get('published_at'));
        }

        if ($schema->hasMixin('gdbots:ncr:mixin:sluggable')) {
            // slug SHOULD NOT change during an update, use "rename-node"
            $newNode->set('slug', $oldNode->get('slug'));
        }

        if ($schema->hasMixin('gdbots:ncr:mixin:lockable')) {
            // is_locked and locked_by_ref SHOULD NOT change during an update, use "[un]lock-node"
            $newNode
                ->set('is_locked', $oldNode->get('is_locked'))
                ->set('locked_by_ref', $oldNode->get('locked_by_ref'));
        }

        $event
            ->set('node_ref', $nodeRef)
            ->set('old_node', $oldNode)
            ->set('new_node', $newNode);

        $this->filterEvent($event, $command, $pbjx);
        $streamId = $this->createStreamId($nodeRef, $command, $event);
        $this->putEvents($command, $pbjx, $streamId, [$event]);
    }

    /**
     * @param NodeUpdated $event
     * @param UpdateNode  $command
     * @param Pbjx        $pbjx
     */
    protected function filterEvent(NodeUpdated $event, UpdateNode $command, Pbjx $pbjx): void
    {
        // override to customize the event before putEvents is run.
    }

    /**
     * @param UpdateNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeUpdated
     */
    abstract protected function createNodeUpdated(UpdateNode $command, Pbjx $pbjx): NodeUpdated;
}
