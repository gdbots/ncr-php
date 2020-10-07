<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Aggregate;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Command\DeleteNodeV1;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeDeletedV1;
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
}
