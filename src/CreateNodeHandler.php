<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;

class CreateNodeHandler implements CommandHandler
{
    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 4.x.
        $curies = MessageResolver::findAllUsingMixin('gdbots:ncr:mixin:create-node:v1', false);
        $curies[] = 'gdbots:ncr:command:create-node';
        return $curies;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var Message $node */
        $node = $command->get('node');
        $context = ['causator' => $command];

        $aggregate = AggregateResolver::resolve($node::schema()->getQName())::fromNode($node, $pbjx);
        $aggregate->createNode($command);
        $aggregate->commit($context);
    }
}
