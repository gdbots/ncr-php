<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\NodeUnpublished\NodeUnpublished;
use Gdbots\Schemas\Ncr\Mixin\UnpublishNode\UnpublishNode;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractUnpublishNodeHandler extends AbstractNodeCommandHandler
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
     * @param UnpublishNode $command
     * @param Pbjx          $pbjx
     */
    protected function handle(UnpublishNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if (!$node->get('status')->equals(NodeStatus::PUBLISHED())) {
            // node already not published, ignore
            return;
        }

        $event = $this->createNodeUnpublished($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param UnpublishNode $command
     * @param Pbjx          $pbjx
     *
     * @return NodeUnpublished
     */
    protected function createNodeUnpublished(UnpublishNode $command, Pbjx $pbjx): NodeUnpublished
    {
        /** @var NodeUnpublished $event */
        $event = $this->createEventFromCommand($command, 'unpublished');
        return $event;
    }
}
