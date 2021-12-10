<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;

class LockNodeHandler implements CommandHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 4.x.
        $curies = MessageResolver::findAllUsingMixin('gdbots:ncr:mixin:lock-node:v1', false);
        $curies[] = 'gdbots:ncr:command:lock-node';
        return $curies;
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $context = ['causator' => $command];

        $node = $this->ncr->getNode($nodeRef, true, $context);
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $aggregate->lockNode($command);
        $aggregate->commit($context);
    }
}
