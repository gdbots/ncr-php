<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaCurie;
use Gdbots\Pbjx\CommandHandler;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\StreamId;
use Gdbots\Schemas\Ncr\Command\UpdateNodeLabelsV1;
use Gdbots\Schemas\Ncr\Event\NodeLabelsUpdatedV1;

abstract class UpdateNodeLabelsNodeHandler extends AbstractNodeCommandHandler
{
    /** @var Ncr */
    protected $ncr;

    /**
     * @param Ncr    $ncr
     */
    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public static function handlesCuries(): array
    {
        return [
            UpdateNodeLabelsV1::schema()->getCurie(),
        ];
    }

    /**
     * @param Message $command
     * @param Pbjx        $pbjx
     */
    public function handleCommand(Message $command, Pbjx $pbjx): void
    {
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef);

        if (!$this->node::schema()->hasMixin('gdbots:common:mixin:labelable')) {
            return;
        }

        $added = array_values(array_filter(
            $command->get('add_labels', []),
            fn(string $label) => !$this->node->isInSet('labels', $label)
        ));

        $removed = array_values(array_filter(
            $command->get('remove_labels', []),
            fn(string $label) => $this->node->isInSet('labels', $label)
        ));

        if (empty($added) && empty($removed)) {
            return;
        }
        
        $event = NodeLabelsUpdatedV1::create();
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);
        $event->set('labels_added', $added);
        $event->set('labels_removed', $removed);

        $streamId = StreamId::fromString(sprintf('%s.history:%s', $nodeRef->getLabel(), $nodeRef->getId()));
        $pbjx->getEventStore()->putEvents($streamId, [$event]);
    }

}
