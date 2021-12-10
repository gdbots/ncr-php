<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\NodeProjectedEvent;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxProjector;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Pbjx;

class NcrProjector implements EventSubscriber, PbjxProjector
{
    protected const DEPRECATED_MIXINS_TO_SUFFIX = [
        'gdbots:ncr:mixin:node-created'           => 'created',
        'gdbots:ncr:mixin:node-deleted'           => 'deleted',
        'gdbots:ncr:mixin:node-expired'           => 'expired',
        'gdbots:ncr:mixin:node-locked'            => 'locked',
        'gdbots:ncr:mixin:node-marked-as-draft'   => 'marked-as-draft',
        'gdbots:ncr:mixin:node-marked-as-pending' => 'marked-as-pending',
        'gdbots:ncr:mixin:node-published'         => 'published',
        'gdbots:ncr:mixin:node-renamed'           => 'renamed',
        'gdbots:ncr:mixin:node-scheduled'         => 'scheduled',
        'gdbots:ncr:mixin:node-unlocked'          => 'unlocked',
        'gdbots:ncr:mixin:node-unpublished'       => 'unpublished',
        'gdbots:ncr:mixin:node-updated'           => 'updated',
    ];

    protected Ncr $ncr;
    protected NcrSearch $ncrSearch;
    protected bool $enabled;

    public static function getSubscribedEvents(): array
    {
        return [
            'gdbots:ncr:event:node-created'           => 'onNodeCreated',
            'gdbots:ncr:event:node-deleted'           => 'onNodeDeleted',
            'gdbots:ncr:event:node-expired'           => 'onNodeEvent',
            'gdbots:ncr:event:node-labels-updated'    => 'onNodeEvent',
            'gdbots:ncr:event:node-locked'            => 'onNodeEvent',
            'gdbots:ncr:event:node-marked-as-draft'   => 'onNodeEvent',
            'gdbots:ncr:event:node-marked-as-pending' => 'onNodeEvent',
            'gdbots:ncr:event:node-published'         => 'onNodeEvent',
            'gdbots:ncr:event:node-renamed'           => 'onNodeEvent',
            'gdbots:ncr:event:node-scheduled'         => 'onNodeEvent',
            'gdbots:ncr:event:node-tags-updated'      => 'onNodeEvent',
            'gdbots:ncr:event:node-unlocked'          => 'onNodeEvent',
            'gdbots:ncr:event:node-unpublished'       => 'onNodeEvent',
            'gdbots:ncr:event:node-updated'           => 'onNodeUpdated',

            // deprecated mixins, will be removed in 4.x.
            'gdbots:ncr:mixin:node-created'           => 'onNodeCreated',
            'gdbots:ncr:mixin:node-deleted'           => 'onNodeDeleted',
            'gdbots:ncr:mixin:node-expired'           => 'onNodeEvent',
            'gdbots:ncr:mixin:node-locked'            => 'onNodeEvent',
            'gdbots:ncr:mixin:node-marked-as-draft'   => 'onNodeEvent',
            'gdbots:ncr:mixin:node-marked-as-pending' => 'onNodeEvent',
            'gdbots:ncr:mixin:node-published'         => 'onNodeEvent',
            'gdbots:ncr:mixin:node-renamed'           => 'onNodeEvent',
            'gdbots:ncr:mixin:node-scheduled'         => 'onNodeEvent',
            'gdbots:ncr:mixin:node-unlocked'          => 'onNodeEvent',
            'gdbots:ncr:mixin:node-unpublished'       => 'onNodeEvent',
            'gdbots:ncr:mixin:node-updated'           => 'onNodeUpdated',
        ];
    }

    public function __construct(Ncr $ncr, NcrSearch $ncrSearch, bool $enabled = true)
    {
        $this->ncr = $ncr;
        $this->ncrSearch = $ncrSearch;
        $this->enabled = $enabled;
    }

    public function onNodeEvent(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNodeRef($event->get('node_ref'), $event, $pbjx);
    }

    public function onNodeCreated(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNode($event->get('node'), $event, $pbjx);
    }

    public function onNodeDeleted(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        /** @var NodeRef $nodeRef */
        $nodeRef = $event->get('node_ref');
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
            $this->projectNode($node, $event, $pbjx);
            return;
        }

        $this->ncr->deleteNode($nodeRef, $context);
        $this->ncrSearch->deleteNodes([$nodeRef], $context);
        $this->afterNodeProjected($node, $event, $pbjx);
    }

    public function onNodeUpdated(Message $event, Pbjx $pbjx): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->projectNode($event->get('new_node'), $event, $pbjx);
    }

    protected function projectNodeRef(NodeRef $nodeRef, Message $event, Pbjx $pbjx): void
    {
        $node = $this->ncr->getNode($nodeRef, true, ['causator' => $event]);
        $this->projectNode($node, $event, $pbjx);
    }

    protected function projectNode(Message $node, Message $event, Pbjx $pbjx): void
    {
        $context = ['causator' => $event];
        $nodeRef = $node->generateNodeRef();
        $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
        $aggregate->sync($context);
        $node = $aggregate->getNode();
        $this->ncr->putNode($node, null, $context);
        $this->ncrSearch->indexNodes([$node], $context);
        $this->afterNodeProjected($node, $event, $pbjx);
    }

    protected function afterNodeProjected(Message $node, Message $event, Pbjx $pbjx): void
    {
        $pbjxEvent = new NodeProjectedEvent($node, $event);
        $pbjx->trigger($node, 'projected', $pbjxEvent, false, false);
        $pbjx->trigger($node, $this->createProjectedEventSuffix($node, $event), $pbjxEvent, false, false);
    }

    protected function createProjectedEventSuffix(Message $node, Message $event): string
    {
        $schema = $event::schema();
        $id = $schema->getId();
        if ('gdbots' === $id->getVendor() && 'ncr' === $id->getPackage()) {
            return str_replace('node-', '', $id->getMessage());
        }

        foreach (static::DEPRECATED_MIXINS_TO_SUFFIX as $mixin => $suffix) {
            if ($schema->hasMixin($mixin)) {
                return $suffix;
            }
        }

        return $id->getMessage();
    }
}
