<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\DeleteNode\DeleteNode;
use Gdbots\Schemas\Ncr\Mixin\NodeDeleted\NodeDeleted;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractDeleteNodeHandler extends AbstractNodeCommandHandler
{
    /**
     * @param DeleteNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(DeleteNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);
        /*
        if ($node->get('status')->equals(NodeStatus::DELETED())) {
            // already soft-deleted, ignore?
            return;
        }
        */

        $event = $this->createNodeDeleted($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param DeleteNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeDeleted
     */
    abstract protected function createNodeDeleted(DeleteNode $command, Pbjx $pbjx): NodeDeleted;
}
