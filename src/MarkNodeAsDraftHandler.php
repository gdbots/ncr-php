<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\MarkNodeAsDraftV1;
use Gdbots\Schemas\Ncr\Mixin\MarkNodeAsDraft\MarkNodeAsDraftV1Mixin;

class MarkNodeAsDraftHandler implements CommandHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 3.x
        $curies = MessageResolver::findAllUsingMixin(MarkNodeAsDraftV1Mixin::SCHEMA_CURIE, false);
        $curies[] = MarkNodeAsDraftV1::SCHEMA_CURIE;
        return $curies;
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(MarkNodeAsDraftV1::NODE_REF_FIELD);
        $context = ['causator' => $command];

        $node = $this->ncr->getNode($nodeRef, true, $context);
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $aggregate->markNodeAsDraft($command);
        $aggregate->commit($context);
    }
}
