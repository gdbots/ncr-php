<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Aggregate;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
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
        foreach ($this->pbjx->getEventStore()->pipeAllEvents() as $yield) {
            $event = $yield[0];
            $streamId = $yield[1];
            $this->assertInstanceOf(NodeCreatedV1::class, $event);
            $this->assertTrue(StreamId::fromNodeRef($nodeRef)->equals($streamId));
        }
    }
}
