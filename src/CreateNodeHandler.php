<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;

class CreateNodeHandler implements CommandHandler
{
    public static function handlesCuries(): array
    {
        return [
            CreateNodeV1::SCHEMA_CURIE,
        ];
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
