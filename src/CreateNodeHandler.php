<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Mixin\CreateNode\CreateNodeV1Mixin;

class CreateNodeHandler implements CommandHandler
{
    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 3.x
        $curies = MessageResolver::findAllUsingMixin(CreateNodeV1Mixin::SCHEMA_CURIE, false);
        $curies[] = CreateNodeV1::SCHEMA_CURIE;
        return $curies;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var Message $node */
        $node = $command->get(CreateNodeV1::NODE_FIELD);
        $context = ['causator' => $command];

        $aggregate = AggregateResolver::resolve($node::schema()->getQName())::fromNode($node, $pbjx);
        $aggregate->createNode($command);
        $aggregate->commit($context);
    }
}
