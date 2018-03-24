<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Event\BindFromNodeEvent;
use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Pbj\Message;
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
     * @param Node $node
     *
     * @throws InvalidArgumentException
     */
    protected function assertIsNodeSupported(Node $node): void
    {
        if (!$this->isNodeSupported($node)) {
            $class = ClassUtils::getShortName(static::class);
            throw new InvalidArgumentException(
                "Node [{$node::schema()->getCurie()}] not supported by [{$class}]."
            );
        }
    }

    /**
     * Determines if the given node can or should be handled by this handler.
     * A sanity/security check to ensure mismatched node refs are not given
     * maliciously or otherwise to unsuspecting handlers.
     *
     * @param Node $node
     *
     * @return bool
     */
    protected function isNodeSupported(Node $node): bool
    {
        return true;
    }

    /**
     * Creates the context object that is passed to Ncr methods.
     * @see Ncr::getNode
     *
     * @param Message $message
     *
     * @return array
     */
    protected function createNcrContext(Message $message): array
    {
        return [];
    }

    /**
     * During the handling of a command zero or more events may be
     * created. The handler generally has the current node which
     * provides an opportunity to bind data to the event from the
     * node before it's persisted.
     *
     * @param Event $event
     * @param Node  $node
     * @param Pbjx  $pbjx
     */
    protected function bindFromNode(Event $event, Node $node, Pbjx $pbjx): void
    {
        $node->freeze();
        $pbjxEvent = new BindFromNodeEvent($event, $node);
        $pbjx->trigger($event, 'bind_from_node', $pbjxEvent, false);
    }

    /**
     * @param NodeRef $nodeRef
     * @param Command $command
     * @param Event   $event
     *
     * @return StreamId
     */
    protected function createStreamId(NodeRef $nodeRef, Command $command, Event $event): StreamId
    {
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
        $context = $this->createEventStoreContext($command, $streamId);
        $pbjx->getEventStore()->putEvents($streamId, $events, null, $context);
    }

    /**
     * Creates the context object that is passed to EventStore methods.
     * @see EventStore::putEvents
     *
     * @param Message  $message
     * @param StreamId $streamId
     *
     * @return array
     */
    protected function createEventStoreContext(Message $message, StreamId $streamId): array
    {
        return [];
    }
}
