<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeAlreadyLocked;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\LockNode\LockNode;
use Gdbots\Schemas\Ncr\Mixin\NodeLocked\NodeLocked;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractLockNodeHandler extends AbstractNodeCommandHandler
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
     * @param LockNode $command
     * @param Pbjx     $pbjx
     */
    protected function handle(LockNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if ($node->get('is_locked')) {
            if ($command->has('ctx_user_ref')) {
                $userNodeRef = NodeRef::fromMessageRef($command->get('ctx_user_ref'));
                if ((string)$node->get('locked_by_ref') === (string)$userNodeRef) {
                    // if it's the same user we can ignore it
                    // because they already own the lock
                    return;
                }
            }

            throw new NodeAlreadyLocked();
        }

        $event = $this->createNodeLocked($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param LockNode $command
     * @param Pbjx     $pbjx
     *
     * @return NodeLocked
     */
    abstract protected function createNodeLocked(LockNode $command, Pbjx $pbjx): NodeLocked;
}
