<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Enricher;

use Acme\Schemas\Iam\Event\UserCreatedV1;
use Acme\Schemas\Iam\Event\UserUpdatedV1;
use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Enricher\NodeEtagEnricher;
use Gdbots\Pbj\MessageRef;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Schemas\Iam\UserId;
use PHPUnit\Framework\TestCase;

final class NodeEtagEnricherTest extends TestCase
{
    public function testEnrichNodeCreated(): void
    {
        $node = UserV1::create()
            ->set('_id', UserId::fromString('bab103f0-6d83-4729-9828-1e6bb60537a0'))
            ->set('created_at', Microtime::fromString('1518675519085709'))
            // using time() ensures etag itself is never apart of final etag
            ->set('etag', 'invalid' . time())
            ->set('email', 'homer@simpson.com')
            ->set('first_name', 'Homer')
            ->set('last_name', 'Simpson');

        $event = UserCreatedV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($event);

        (new NodeEtagEnricher())->enrichNodeCreated($pbjxEvent);
        $actual = $event->get('node')->get('etag');
        $expected = '2f690830572e0ffb7119d2f4ce5a9cab';

        $this->assertSame($expected, $actual, 'Enriched etag on node should match.');
    }

    public function testEnrichNodeUpdated(): void
    {
        $lastEventRef = MessageRef::fromString('vendor:package:category:event:cab103f0-6d83-4729-9828-1e6bb60537a0');
        $oldNode = UserV1::create()
            ->set('_id', UserId::fromString('bab103f0-6d83-4729-9828-1e6bb60537a0'))
            ->set('created_at', Microtime::fromString('1518675519085709'))
            ->clear('last_event_ref')
            ->set('etag', 'c1aa9b320ff5c06ec635298a26674317')
            ->set('email', 'homer@simpson.com')
            ->set('first_name', 'Homer')
            ->set('last_name', 'Simpson')
            ->freeze();

        $newNode = clone $oldNode;
        $newNode->set('is_blocked', true)->set('last_event_ref', $lastEventRef);

        $event = UserUpdatedV1::create()
            ->set('node_ref', $oldNode->get('_id')->toNodeRef())
            ->set('old_node', $oldNode)
            ->set('new_node', $newNode);
        $pbjxEvent = new PbjxEvent($event);

        (new NodeEtagEnricher())->enrichNodeUpdated($pbjxEvent);

        $actual = $event->get('old_node')->get('etag');
        $expected = 'c1aa9b320ff5c06ec635298a26674317';
        $this->assertSame($expected, $actual, 'Enriched etag on old_node should match.');

        $actual = $event->get('new_node')->get('etag');
        $expected = '61a24cf518c07ad042a8040bb394490e';
        $this->assertSame($expected, $actual, 'Enriched etag on new_node updated should match.');

        $actual = $event->get('old_etag');
        $expected = $oldNode->get('etag');
        $this->assertSame($expected, $actual, 'Enriched old_etag should match.');

        $actual = $event->get('new_etag');
        $expected = $newNode->get('etag');
        $this->assertSame($expected, $actual, 'Enriched new_etag should match.');

        echo $event;
    }
}
