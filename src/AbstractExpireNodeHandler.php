<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\ExpireNode\ExpireNode;
use Gdbots\Schemas\Ncr\Mixin\NodeExpired\NodeExpired;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractExpireNodeHandler extends AbstractNodeCommandHandler
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
     * @param ExpireNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(ExpireNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);
        /*
        // @var NodeStatus $currStatus
        $currStatus = $node->get('status');
        if ($currStatus->equals(NodeStatus::DELETED()) || $currStatus->equals(NodeStatus::EXPIRED())) {
            // already expired or soft-deleted nodes can be ignored?
            return;
        }
        */

        $event = $this->createNodeExpired($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param ExpireNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeExpired
     */
    abstract protected function createNodeExpired(ExpireNode $command, Pbjx $pbjx): NodeExpired;
}
