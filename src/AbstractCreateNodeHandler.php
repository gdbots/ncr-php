<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\CreateNode\CreateNode;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\Mixin\NodeCreated\NodeCreated;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractCreateNodeHandler extends AbstractNodeCommandHandler
{
    /**
     * @param CreateNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(CreateNode $command, Pbjx $pbjx): void
    {
        $event = $this->createNodeCreated($command, $pbjx);
        $pbjx->copyContext($command, $event);

        /** @var Node $node */
        $node = clone $command->get('node');
        $this->assertIsNodeSupported($node);
        $node
            ->clear('updated_at')
            ->clear('updater_ref')
            ->set('status', NodeStatus::DRAFT())
            ->set('created_at', $event->get('occurred_at'))
            ->set('creator_ref', $event->get('ctx_user_ref'))
            ->set('last_event_ref', $event->generateMessageRef());

        $event->set('node', $node);
        $this->filterEvent($event, $command, $pbjx);
        $streamId = $this->createStreamId(NodeRef::fromNode($node), $command, $event);
        $this->putEvents($command, $pbjx, $streamId, [$event]);
    }

    /**
     * @param NodeCreated $event
     * @param CreateNode  $command
     * @param Pbjx        $pbjx
     */
    protected function filterEvent(NodeCreated $event, CreateNode $command, Pbjx $pbjx): void
    {
        // override to customize the event before putEvents is run.
    }

    /**
     * @param CreateNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeCreated
     */
    abstract protected function createNodeCreated(CreateNode $command, Pbjx $pbjx): NodeCreated;
}
