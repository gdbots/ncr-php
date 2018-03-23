<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\NodePublished\NodePublished;
use Gdbots\Schemas\Ncr\Mixin\NodeScheduled\NodeScheduled;
use Gdbots\Schemas\Ncr\Mixin\PublishNode\PublishNode;

abstract class AbstractPublishNodeHandler extends AbstractNodeCommandHandler
{
    /**
     * If the node will publish within 15 seconds then we'll
     * just publish it now rather than schedule it.
     *
     * @var int
     */
    protected $anticipationThreshold = 15;

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     */
    protected function handle(PublishNode $command, Pbjx $pbjx): void
    {
        $node = $this->getNode($command, $pbjx);
        $now = time() + $this->anticipationThreshold;

        /** @var \DateTime $publishAt */
        $publishAt = $command->get('publish_at') ?: $command->get('occurred_at')->toDateTime();

        /** @var NodeStatus $prevStatus */
        $currStatus = $node->get('status');
        $currPublishedAt = $node->has('published_at')
            ? $node->get('published_at')->getTimestamp()
            : null;

        if ($now >= $publishAt->getTimestamp()) {
            if ($currStatus->equals(NodeStatus::PUBLISHED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodePublished($command, $pbjx);
            $event->set('published_at', $publishAt);
        } else {
            if ($currStatus->equals(NodeStatus::SCHEDULED()) && $currPublishedAt === $publishAt->getTimestamp()) {
                return;
            }
            $event = $this->createNodeScheduled($command, $pbjx);
            $event->set('publish_at', $publishAt);
        }

        $pbjx->copyContext($command, $event);
        $event->set('node_ref', $command->get('node_ref'));

        if ($node->has('slug')) {
            $event->set('slug', $node->get('slug'));
        }

        $this->enrichEvent($event, $node);
        $streamId = $this->createStreamId($command, $event);
        $this->putEvents($command, $pbjx, $streamId, [$event]);
    }

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     *
     * @return NodeScheduled
     */
    abstract protected function createNodeScheduled(PublishNode $command, Pbjx $pbjx): NodeScheduled;

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     *
     * @return NodePublished
     */
    abstract protected function createNodePublished(PublishNode $command, Pbjx $pbjx): NodePublished;
}
