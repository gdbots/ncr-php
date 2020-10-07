<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Forms\Node\FormV1;
use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Aggregate;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Command\DeleteNodeV1;
use Gdbots\Schemas\Ncr\Command\ExpireNodeV1;
use Gdbots\Schemas\Ncr\Command\LockNodeV1;
use Gdbots\Schemas\Ncr\Command\MarkNodeAsDraftV1;
use Gdbots\Schemas\Ncr\Command\MarkNodeAsPendingV1;
use Gdbots\Schemas\Ncr\Command\PublishNodeV1;
use Gdbots\Schemas\Ncr\Command\RenameNodeV1;
use Gdbots\Schemas\Ncr\Command\UnlockNodeV1;
use Gdbots\Schemas\Ncr\Command\UnpublishNodeV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeLabelsV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeTagsV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeV1;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
use Gdbots\Schemas\Ncr\Event\NodeExpiredV1;
use Gdbots\Schemas\Ncr\Event\NodeLabelsUpdatedV1;
use Gdbots\Schemas\Ncr\Event\NodeLockedV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsDraftV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsPendingV1;
use Gdbots\Schemas\Ncr\Event\NodePublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeRenamedV1;
use Gdbots\Schemas\Ncr\Event\NodeTagsUpdatedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnlockedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnpublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
use Gdbots\Schemas\Pbjx\StreamId;

final class AggregateTest extends AbstractPbjxTest
{
    public function testCreateNode(): void
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->createNode(CreateNodeV1::create()->set('node', $node));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $eventNode = $event->get('node');
            $this->assertInstanceOf(NodeCreatedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
            $this->assertSame($eventNode->get('created_at'), $event->get('occurred_at'));
            $this->assertSame($eventNode->get('creator_ref'), $event->get('ctx_user_ref'));
            $this->assertTrue($eventNode->get('last_event_ref')->equals($event->generateMessageRef()));
        }
    }

    public function testDeleteNode(): void
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->deleteNode(DeleteNodeV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeDeletedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testLockNode(): void
    {
        $node = FormV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->lockNode(LockNodeV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeLockedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testUnlockNode(): void
    {
        $node = FormV1::create()->set('is_locked', true);
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->unlockNode(UnlockNodeV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeUnlockedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testMarkNodeAsDraft(): void
    {
        $node = FormV1::create()->set('status', NodeStatus::PENDING());
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->markNodeAsDraft(MarkNodeAsDraftV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeMarkedAsDraftV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testMarkNodeAsPending(): void
    {
        $node = FormV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->markNodeAsPending(MarkNodeAsPendingV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeMarkedAsPendingV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testPublishNode(): void
    {
        $node = FormV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->publishNode(PublishNodeV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodePublishedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testRenameNode(): void
    {
        $node = FormV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $command = RenameNodeV1::create()
            ->set('new_slug', 'foo-bar')
            ->set('node_ref', $nodeRef);
        $aggregate->renameNode($command);
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeRenamedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testUnpublishNode(): void
    {
        $node = FormV1::create()->set('status', NodeStatus::PUBLISHED());
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->unpublishNode(UnpublishNodeV1::create()->set('node_ref', $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeUnpublishedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testExpireNode(): void
    {
        $node = FormV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $aggregate->expireNode(ExpireNodeV1::create()->set('node_ref',  $nodeRef));
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeExpiredV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }

    public function testUpdateNode(): void
    {
        $node = FormV1::create()
            ->set('title', 'foo')
            ->set('slug', 'bar');
        $newNode = (clone $node)
            ->set('title', 'bar')
            ->set('status', NodeStatus::PUBLISHED())
            ->set('published_at', new \DateTime())
            ->set('slug', 'baz')
            ->set('is_locked', true)
            ->set('locked_by_ref', NodeRef::fromNode(UserV1::create()))
            ->addToSet('labels', ['label']);
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $command = UpdateNodeV1::create()
            ->set('node_ref', $nodeRef)
            ->set('new_node', $newNode);
        $aggregate->updateNode($command);
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $eventNewNode = $event->get('new_node');
            $this->assertInstanceOf(NodeUpdatedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
            $this->assertTrue($event->get('node_ref')->equals($nodeRef));
            $this->assertSame($eventNewNode->get('updated_at'), $event->get('occurred_at'));
            $this->assertSame($eventNewNode->get('updater_ref'), $event->get('ctx_user_ref'));
            $this->assertTrue($eventNewNode->get('last_event_ref')->equals($event->generateMessageRef()));
            $this->assertTrue($eventNewNode->get('status')->equals($node->get('status')));
            $this->assertSame((string)$eventNewNode->get('created_at'), (string)$node->get('created_at'));
            $this->assertSame($eventNewNode->get('creator_ref'), $event->get('creator_ref'));
            $this->assertFalse($eventNewNode->isInSet('labels', 'label'));
            $this->assertFalse($eventNewNode->has('published_at'));
            $this->assertTrue($eventNewNode->get('slug') === 'bar');
            $this->assertFalse($eventNewNode->get('is_locked'));
            $this->assertFalse($eventNewNode->has('locked_by_ref'));
        }
    }

    public function testUpdateNodeLabels(): void
    {
        $node = FormV1::create()
            ->addToSet('labels', ['existing-label-1', 'existing-label-2']);
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $command = UpdateNodeLabelsV1::create()
            ->set('node_ref', $nodeRef)
            ->addToSet('remove_labels', ['existing-label-1', 'existing-label-2'])
            ->addToSet('add_labels', ['new-label-1', 'new-label-2']);
        $aggregate->updateNodeLabels($command);
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeLabelsUpdatedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
            $this->assertTrue($event->get('node_ref')->equals($nodeRef));
            $this->assertTrue(in_array('new-label-1', $event->get('labels_added')));
            $this->assertTrue(in_array('new-label-2', $event->get('labels_added')));
            $this->assertTrue(in_array('existing-label-1', $event->get('labels_removed')));
            $this->assertTrue(in_array('existing-label-2', $event->get('labels_removed')));
        }
    }

    public function testUpdateNodeTags(): void
    {
        $node = FormV1::create()
            ->addToMap('tags', 'existing-tag', 'foo');
        $nodeRef = NodeRef::fromNode($node);
        $aggregate = Aggregate::fromNode($node, $this->pbjx);
        $command = UpdateNodeTagsV1::create()
            ->set('node_ref', $nodeRef)
            ->addToSet('remove_tags', ['existing-tag'])
            ->addToMap('add_tags', 'new-tag', 'bar');
        $aggregate->updateNodeTags($command);
        $aggregate->commit();
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as [$event, $streamId]) {
            $this->assertInstanceOf(NodeTagsUpdatedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
            $this->assertTrue($event->get('node_ref')->equals($nodeRef));
            $this->assertSame('bar', $event->get('tags_added')['new-tag']);
            $this->assertTrue(in_array('existing-tag', $event->get('tags_removed')));
        }
    }
}
