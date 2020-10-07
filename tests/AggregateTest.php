<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Forms\Node\FormV1;
use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Aggregate;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Command\DeleteNodeV1;
use Gdbots\Schemas\Ncr\Command\LockNodeV1;
use Gdbots\Schemas\Ncr\Command\MarkNodeAsDraftV1;
use Gdbots\Schemas\Ncr\Command\MarkNodeAsPendingV1;
use Gdbots\Schemas\Ncr\Command\PublishNodeV1;
use Gdbots\Schemas\Ncr\Command\RenameNodeV1;
use Gdbots\Schemas\Ncr\Command\UnlockNodeV1;
use Gdbots\Schemas\Ncr\Command\UnpublishNodeV1;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
use Gdbots\Schemas\Ncr\Event\NodeLockedV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsDraftV1;
use Gdbots\Schemas\Ncr\Event\NodeMarkedAsPendingV1;
use Gdbots\Schemas\Ncr\Event\NodePublishedV1;
use Gdbots\Schemas\Ncr\Event\NodeRenamedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnlockedV1;
use Gdbots\Schemas\Ncr\Event\NodeUnpublishedV1;
use Gdbots\Schemas\Pbjx\StreamId;

final class AggregateTest extends AbstractPbjxTest
{
    // todo: beef up assertions even moar
    // todo: testExpireNode?

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
            ->set('new_slug', 'thylacine-daydream')
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
}
