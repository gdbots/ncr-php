<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\DeleteNodeV1;

class DeleteNodeHandler implements CommandHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        return [
            DeleteNodeV1::SCHEMA_CURIE,
        ];
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(DeleteNodeV1::NODE_REF_FIELD);
        $context = ['causator' => $command];

        try {
            $node = $this->ncr->getNode($nodeRef, true, $context);
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        } catch (\Throwable $e) {
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNodeRef($nodeRef, $pbjx);
        }

        $aggregate->sync($context);
        $aggregate->deleteNode($command);
        $aggregate->commit($context);
    }
}
