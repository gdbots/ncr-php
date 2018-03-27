<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\MarkNodeAsPending\MarkNodeAsPending;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsPending\NodeMarkedAsPending;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractMarkNodeAsPendingHandler extends AbstractNodeCommandHandler
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
     * @param MarkNodeAsPending $command
     * @param Pbjx              $pbjx
     */
    protected function handle(MarkNodeAsPending $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if ($node->get('status')->equals(NodeStatus::PENDING())) {
            // node already pending, ignore
            return;
        }

        $event = $this->createNodeMarkedAsPending($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param MarkNodeAsPending $command
     * @param Pbjx              $pbjx
     *
     * @return NodeMarkedAsPending
     */
    abstract protected function createNodeMarkedAsPending(MarkNodeAsPending $command, Pbjx $pbjx): NodeMarkedAsPending;
}
