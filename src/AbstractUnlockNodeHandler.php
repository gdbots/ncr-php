<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\NodeUnlocked\NodeUnlocked;
use Gdbots\Schemas\Ncr\Mixin\UnlockNode\UnlockNode;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractUnlockNodeHandler extends AbstractNodeCommandHandler
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
     * @param UnlockNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(UnlockNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if (!$node->get('is_locked')) {
            // node already unlocked, ignore
            return;
        }

        $event = $this->createNodeUnlocked($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param UnlockNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeUnlocked
     */
    abstract protected function createNodeUnlocked(UnlockNode $command, Pbjx $pbjx): NodeUnlocked;
}
