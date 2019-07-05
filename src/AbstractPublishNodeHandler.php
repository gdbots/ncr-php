<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\Util\SlugUtils;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\NodePublished\NodePublished;
use Gdbots\Schemas\Ncr\Mixin\NodeScheduled\NodeScheduled;
use Gdbots\Schemas\Ncr\Mixin\PublishNode\PublishNode;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractPublishNodeHandler extends AbstractNodeCommandHandler
{
    /**
     * If the node will publish within 15 seconds then we'll
     * just publish it now rather than schedule it.
     *
     * @var int
     */
    protected $anticipationThreshold = 15;

    /** @var Ncr */
    protected $ncr;

    /**
     * If the node has a slug and it contains a date and a time zone
     * has been set then we'll automatically update the slug to
     * contain the date it was published.
     *
     * @var \DateTimeZone
     */
    protected $localTimeZone;

    /**
     * @param Ncr    $ncr
     * @param string $localTimeZone
     */
    public function __construct(Ncr $ncr, ?string $localTimeZone = null)
    {
        $this->ncr = $ncr;
        $this->localTimeZone = null !== $localTimeZone ? new \DateTimeZone($localTimeZone) : null;
    }

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     */
    protected function handle(PublishNode $command, Pbjx $pbjx): void
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $node = $this->ncr->getNode($nodeRef, true, $this->createNcrContext($command));
        $this->assertIsNodeSupported($node);

        /** @var \DateTime $publishAt */
        $publishAt = $command->get('publish_at') ?: $command->get('occurred_at')->toDateTime();
        $now = time() + $this->anticipationThreshold;

        /** @var NodeStatus $currStatus */
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
        $event->set('node_ref', $nodeRef);

        if ($node->has('slug')) {
            $slug = $node->get('slug');
            if (null !== $this->localTimeZone && SlugUtils::containsDate($slug)) {
                $date = $publishAt instanceof \DateTimeImmutable
                    ? \DateTime::createFromImmutable($publishAt)
                    : clone $publishAt;
                $date->setTimezone($this->localTimeZone);
                $slug = SlugUtils::addDate($slug, $date);
            }
            $event->set('slug', $slug);
        }

        $this->bindFromNode($event, $node, $pbjx);
        $this->putEvents($command, $pbjx, $this->createStreamId($nodeRef, $command, $event), [$event]);
    }

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     *
     * @return NodeScheduled
     */
    protected function createNodeScheduled(PublishNode $command, Pbjx $pbjx): NodeScheduled
    {
        /** @var NodeScheduled $event */
        $event = $this->createEventFromCommand($command, 'scheduled');
        return $event;
    }

    /**
     * @param PublishNode $command
     * @param Pbjx        $pbjx
     *
     * @return NodePublished
     */
    protected function createNodePublished(PublishNode $command, Pbjx $pbjx): NodePublished
    {
        /** @var NodePublished $event */
        $event = $this->createEventFromCommand($command, 'published');
        return $event;
    }
}
