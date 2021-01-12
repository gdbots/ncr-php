<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Pbj\AbstractMessage;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
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
        /** @var Node $oldNode */
        $oldNode = $command->has('old_node')
            ? $command->get('old_node')
            : $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($oldNode);
        $oldNode->freeze();

        $event = $this->createNodeUpdated($command, $pbjx);
        $pbjx->copyContext($command, $event);

        /** @var Node $newNode */
        $newNode = clone $command->get('new_node');
        $this->assertIsNodeSupported($newNode);

        if (!$nodeRef->equals(NodeRef::fromNode($oldNode)) || !$nodeRef->equals(NodeRef::fromNode($newNode))) {
            throw new InvalidArgumentException(
                "The old_node and new_node must have node ref [{$nodeRef}]."
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
        if ($schema->hasMixin('gdbots:common:mixin:labelable')) {
            // labels SHOULD NOT change during an update, use "update-node-labels"
            $newNode->clear('labels');
            $newNode->addToSet('labels', $oldNode->get('labels', []));
        }

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

        // if a node is being updated and it's deleted, restore the default status
        if (NodeStatus::DELETED()->equals($newNode->get('status'))) {
            $newNode->clear('status');
        }

        if ($command->has('paths')) {
            $event->addToSet('paths', $command->get('paths'));
            $oldNodeArray = $oldNode->toArray();
            $newNodeArray = $newNode->toArray();
            $paths = array_flip($command->get('paths'));
            foreach ($schema->getFields() as $field) {
                $fieldName = $field->getName();
                if (isset($paths[$fieldName])) {
                    continue;
                }
                unset($newNodeArray[$fieldName]);
                if (!isset($oldNodeArray[$fieldName])) {
                    continue;
                }
                $newNodeArray[$fieldName] = $oldNodeArray[$fieldName];
            }
            $newNode = AbstractMessage::fromArray($newNodeArray);
        }

        $event
            ->set('node_ref', $nodeRef)
            ->set('old_node', $oldNode)
            ->set('new_node', $newNode);

        $this->beforePutEvents($event, $command, $pbjx);
        $streamId = $this->createStreamId($nodeRef, $command, $event);
        $this->putEvents($command, $pbjx, $streamId, [$event]);
    }

    /**
     * @param NodeUpdated $event
     * @param UpdateNode  $command
     * @param Pbjx        $pbjx
     */
    protected function beforePutEvents(NodeUpdated $event, UpdateNode $command, Pbjx $pbjx): void
    {
        // override to customize the event before putEvents is run.
    }

    /**
     * @param UpdateNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeUpdated
     */
    protected function createNodeUpdated(UpdateNode $command, Pbjx $pbjx): NodeUpdated
    {
        /** @var NodeUpdated $event */
        $event = $this->createEventFromCommand($command, 'updated');
        return $event;
    }
}
