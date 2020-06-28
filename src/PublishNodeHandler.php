<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\PublishNodeV1;
use Gdbots\Schemas\Ncr\Mixin\PublishNode\PublishNodeV1Mixin;

class PublishNodeHandler implements CommandHandler
{
    protected Ncr $ncr;

    /**
     * If the node has a slug and it contains a date and a time zone
     * has been set then we'll automatically update the slug to
     * contain the date it was published.
     *
     * @var \DateTimeZone
     */
    protected ?\DateTimeZone $localTimeZone;

    public static function handlesCuries(): array
    {
        // deprecated mixins, will be removed in 3.x
        $curies = MessageResolver::findAllUsingMixin(PublishNodeV1Mixin::SCHEMA_CURIE, false);
        $curies[] = PublishNodeV1::SCHEMA_CURIE;
        return $curies;
    }

    public function __construct(Ncr $ncr, ?string $localTimeZone = null)
    {
        $this->ncr = $ncr;
        $this->localTimeZone = null !== $localTimeZone ? new \DateTimeZone($localTimeZone) : null;
    }

    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(PublishNodeV1::NODE_REF_FIELD);
        $context = ['causator' => $command];

        $node = $this->ncr->getNode($nodeRef, true, $context);
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $aggregate->publishNode($command, $this->localTimeZone);
        $aggregate->commit($context);
    }
}
