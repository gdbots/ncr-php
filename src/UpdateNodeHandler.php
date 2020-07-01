<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\UpdateNodeV1;
use Gdbots\Schemas\Ncr\Mixin\UpdateNode\UpdateNodeV1Mixin;

class UpdateNodeHandler implements CommandHandler
{
    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 3.x
        $curies = MessageResolver::findAllUsingMixin(UpdateNodeV1Mixin::SCHEMA_CURIE_MAJOR, false);
        $curies[] = UpdateNodeV1::SCHEMA_CURIE;
        return $curies;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(UpdateNodeV1::NODE_REF_FIELD);
        /** @var Message $node */
        $node = $command->get(UpdateNodeV1::OLD_NODE_FIELD);
        $context = ['causator' => $command];

        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->updateNode($command);
        $aggregate->commit($context);
    }
}
