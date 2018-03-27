<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Exception\InvalidArgumentException;
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
    use PbjxHandlerTrait;

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
}
