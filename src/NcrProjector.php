<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxProjector;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;

class NcrProjector implements EventSubscriber, PbjxProjector
{
    protected Ncr $ncr;
    protected NcrSearch $ncrSearch;
    protected bool $enabled;

    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:*' => 'onEvent',
        ];
    }

    public function __construct(Ncr $ncr, NcrSearch $ncrSearch, bool $enabled = true)
    {
        $this->ncr = $ncr;
        $this->ncrSearch = $ncrSearch;
        $this->enabled = $enabled;
    }

    public function onEvent(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $method = $event::schema()->getHandlerMethodName(false, 'on');
        if (is_callable([$this, $method])) {
            $this->$method($event, $pbjx);
        }
    }

    public function onNodeCreated(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNode($event->get($event::NODE_FIELD), $event, $pbjx);
    }

    public function onNodeDeleted(Message $event, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get($event::NODE_REF_FIELD);
        $context = ['causator' => $event];

        try {
            $node = $this->ncr->getNode($nodeRef, true, $context);
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        } catch (NodeNotFound $nf) {
            // ignore already deleted nodes.
            return;
        } catch (\Throwable $e) {
            throw $e;
        }

        if ($aggregate->useSoftDelete()) {
            $this->updateAndIndexNode($node, $event, $pbjx);
            return;
        }

        $this->ncr->deleteNode($nodeRef, $context);
        $this->ncrSearch->deleteNodes([$nodeRef], $context);
    }

    public function onNodeExpired(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    public function onNodeMarkedAsDraft(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    public function onNodeMarkedAsPending(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    public function onNodeRenamed(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    public function onNodeUnpublished(Message $event, Pbjx $pbjx): void
    {
        $this->updateAndIndexNodeRef($event->get($event::NODE_REF_FIELD), $event, $pbjx);
    }

    protected function updateAndIndexNodeRef(NodeRef $nodeRef, Message $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($nodeRef, true, ['causator' => $event]);
        $this->updateAndIndexNode($node, $event, $pbjx);
    }

    protected function updateAndIndexNode(Message $node, Message $event, Pbjx $pbjx): void
    {
        $context = ['causator' => $event];
        $nodeRef = $node->generateNodeRef();
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $node = $aggregate->getNode();
        $this->ncr->putNode($node, null, $context);
        $this->ncrSearch->indexNodes([$node], $context);
    }
}
