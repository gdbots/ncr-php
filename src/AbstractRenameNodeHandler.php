<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\NodeRenamed\NodeRenamed;
use Gdbots\Schemas\Ncr\Mixin\RenameNode\RenameNode;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractRenameNodeHandler extends AbstractNodeCommandHandler
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
     * @param RenameNode $command
     * @param Pbjx       $pbjx
     */
    protected function handle(RenameNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        if ($node->get('slug') === $command->get('new_slug')) {
            // ignore a pointless rename
            return;
        }

        $event = $this->createNodeRenamed($command, $pbjx);
        $pbjx->copyContext($command, $event);
        $event
            ->set('node_ref', $nodeRef)
            ->set('new_slug', $command->get('new_slug'))
            ->set('old_slug', $node->get('slug'))
            ->set('node_status', $node->get('status'));

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param RenameNode $command
     * @param Pbjx       $pbjx
     *
     * @return NodeRenamed
     */
    protected function createNodeRenamed(RenameNode $command, Pbjx $pbjx): NodeRenamed
    {
        /** @var NodeRenamed $event */
        $event = $this->createEventFromCommand($command, 'renamed');
        return $event;
    }
}
