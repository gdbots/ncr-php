<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\CommandHandlerTrait;
use Gdbots\Pbjx\Exception\GdbotsPbjxException;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Mixin\Command\Command;
use Gdbots\Schemas\Pbjx\Mixin\Event\Event;
use Gdbots\Schemas\Pbjx\StreamId;

abstract class AbstractNodeCommandHandler implements CommandHandler
{
    use CommandHandlerTrait;

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
     * @param Command $command
     * @param Pbjx    $pbjx
     *
     * @return Node
     */
    protected function getNode(Command $command, Pbjx $pbjx): Node
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        return $this->ncr->getNode($nodeRef, true);
    }

    /**
     * @param Event $event
     * @param Node  $node
     */
    protected function filterEvent(Event $event, Node $node): void
    {
        // override to customize the event before putEvents is run.
    }

    /**
     * @param Command $command
     * @param Event   $event
     *
     * @return StreamId
     */
    protected function createStreamId(Command $command, Event $event): StreamId
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        return StreamId::fromString(sprintf('%s.history:%s', $nodeRef->getLabel(), $nodeRef->getId()));
    }

    /**
     * @param Command  $command
     * @param Pbjx     $pbjx
     * @param StreamId $streamId
     * @param Event[]  $events
     *
     * @throws GdbotsPbjxException
     */
    protected function putEvents(Command $command, Pbjx $pbjx, StreamId $streamId, array $events): void
    {
        $pbjx->getEventStore()->putEvents($streamId, $events);
    }
}
