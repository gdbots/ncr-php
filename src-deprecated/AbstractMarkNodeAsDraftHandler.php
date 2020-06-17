<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\MarkNodeAsDraft\MarkNodeAsDraft;
use Gdbots\Schemas\Ncr\Mixin\NodeMarkedAsDraft\NodeMarkedAsDraft;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractMarkNodeAsDraftHandler extends AbstractNodeCommandHandler
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
     * @param MarkNodeAsDraft $command
     * @param Pbjx            $pbjx
     */
    protected function handle(MarkNodeAsDraft $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if ($node->get('status')->equals(NodeStatus::DRAFT())) {
            // node already draft, ignore
            return;
        }

        $event = $this->createNodeMarkedAsDraft($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param MarkNodeAsDraft $command
     * @param Pbjx            $pbjx
     *
     * @return NodeMarkedAsDraft
     */
    protected function createNodeMarkedAsDraft(MarkNodeAsDraft $command, Pbjx $pbjx): NodeMarkedAsDraft
    {
        /** @var NodeMarkedAsDraft $event */
        $event = $this->createEventFromCommand($command, 'marked-as-draft');
        return $event;
    }
}