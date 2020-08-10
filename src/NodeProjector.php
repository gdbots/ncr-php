<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\NodeRef;

class NodeProjector extends AbstractNodeProjector implements EventSubscriber
{
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:event:node-labels-updated' => 'onNodeLabelsUpdated',
        ];
    }

    public function onNodeLabelsUpdated(Message $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($event));

        $node
            ->removeFromSet('labels', $event->get('labels_removed', []))
            ->addToSet('labels', $event->get('labels_added', []));

        $this->updateAndIndexNode($node, $event, $pbjx);
    }
}
