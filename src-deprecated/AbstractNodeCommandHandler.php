<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaCurie;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\CommandHandlerTrait;
use Gdbots\Pbjx\Exception\GdbotsPbjxException;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Mixin\Command\Command;
use Gdbots\Schemas\Pbjx\Mixin\Event\Event;
use Gdbots\Schemas\Pbjx\StreamId;

abstract class AbstractNodeCommandHandler implements CommandHandler
{
    use CommandHandlerTrait;
    use PbjxHelperTrait;

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
     * 90% of the time this works 100% of the time.  When events for common
     * node operations match the convention of "blah-suffix" you can use
     * this method to save some typing.  It's always optional.
     *
     * @param Command $command
     * @param string  $suffix
     *
     * @return Event
     */
    protected function createEventFromCommand(Command $command, string $suffix): Event
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref') ?: NodeRef::fromNode($command->get('node'));
        $curie = $command::schema()->getCurie();
        $eventCurie = "{$curie->getVendor()}:{$curie->getPackage()}:event:{$nodeRef->getLabel()}-{$suffix}";

        /** @var Event $class */
        $class = MessageResolver::resolveCurie(SchemaCurie::fromString($eventCurie));
        return $class::create();
    }
}
