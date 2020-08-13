<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Command\UpdateNodeLabelsV1;
use Gdbots\Schemas\Ncr\Event\NodeLabelsUpdatedV1;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Mixin\Command\Command;

class UpdateNodeLabelsHandler extends AbstractNodeCommandHandler
{
    /** @var Ncr */
    protected $ncr;

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

    public function handleCommand(Command $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        $added = array_values(array_filter(
            $command->get('add_labels', []),
            function (string $label) use ($node) {
                return !$node->isInSet('labels', $label);
            }
        ));

        $removed = array_values(array_filter(
            $command->get('remove_labels', []),
            function (string $label) use ($node) {
                return $node->isInSet('labels', $label);
            }
        ));

        if (empty($added) && empty($removed)) {
            return;
        }

        $event = NodeLabelsUpdatedV1::create();
        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $nodeRef);
        $event->addToSet('labels_added', $added);
        $event->addToSet('labels_removed', $removed);

        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    protected function isNodeSupported(Node $node): bool
    {
        return $node::schema()->hasMixin('gdbots:common:mixin:labelable');
    }
}
