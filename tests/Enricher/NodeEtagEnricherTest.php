<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Enricher;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Enricher\NodeEtagEnricher;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\Pbj\WellKnown\UuidIdentifier;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Event\NodeUpdatedV1;
use PHPUnit\Framework\TestCase;

final class NodeEtagEnricherTest extends TestCase
{
    public function testEnrichNodeCreated(): void
    {
        $node = UserV1::create()
            ->set(UserV1::_ID_FIELD, UuidIdentifier::fromString('bab103f0-6d83-4729-9828-1e6bb60537a0'))
            ->set(UserV1::CREATED_AT_FIELD, Microtime::fromString('1518675519085709'))
            // using time() ensures etag itself is never apart of final etag
            ->set(UserV1::ETAG_FIELD, 'invalid' . time())
            ->set(UserV1::EMAIL_FIELD, 'homer@simpson.com')
            ->set(UserV1::FIRST_NAME_FIELD, 'Homer')
            ->set(UserV1::LAST_NAME_FIELD, 'Simpson');

        $event = NodeCreatedV1::create()->set(NodeCreatedV1::NODE_FIELD, $node);
        $pbjxEvent = new PbjxEvent($event);

        (new NodeEtagEnricher())->enrichNodeCreated($pbjxEvent);
        $actual = $event->get(NodeCreatedV1::NODE_FIELD)->get(UserV1::ETAG_FIELD);
        $expected = '2f690830572e0ffb7119d2f4ce5a9cab';

        $this->assertSame($expected, $actual, 'Enriched etag on node should match.');
    }

    public function testEnrichNodeUpdated(): void
    {
        $oldNode = UserV1::create()
            ->set(UserV1::_ID_FIELD, UuidIdentifier::fromString('bab103f0-6d83-4729-9828-1e6bb60537a0'))
            ->set(UserV1::CREATED_AT_FIELD, Microtime::fromString('1518675519085709'))
            ->set(UserV1::ETAG_FIELD, 'c1aa9b320ff5c06ec635298a26674317')
            ->set(UserV1::EMAIL_FIELD, 'homer@simpson.com')
            ->set(UserV1::FIRST_NAME_FIELD, 'Homer')
            ->set(UserV1::LAST_NAME_FIELD, 'Simpson')
            ->freeze();

        $newNode = clone $oldNode;
        $newNode->set(UserV1::IS_BLOCKED_FIELD, true);

        $event = NodeUpdatedV1::create()
            ->set(NodeUpdatedV1::NODE_REF_FIELD, $oldNode->generateNodeRef())
            ->set(NodeUpdatedV1::OLD_NODE_FIELD, $oldNode)
            ->set(NodeUpdatedV1::NEW_NODE_FIELD, $newNode);
        $pbjxEvent = new PbjxEvent($event);

        (new NodeEtagEnricher())->enrichNodeUpdated($pbjxEvent);

        $actual = $event->get(NodeUpdatedV1::OLD_NODE_FIELD)->get(UserV1::ETAG_FIELD);
        $expected = 'c1aa9b320ff5c06ec635298a26674317';
        $this->assertSame($expected, $actual, 'Enriched etag on old_node should match.');

        $actual = $event->get(NodeUpdatedV1::NEW_NODE_FIELD)->get(UserV1::ETAG_FIELD);
        $expected = '61a24cf518c07ad042a8040bb394490e';
        $this->assertSame($expected, $actual, 'Enriched etag on new_node updated should match.');

        $actual = $event->get(NodeUpdatedV1::OLD_ETAG_FIELD);
        $expected = $oldNode->get(UserV1::ETAG_FIELD);
        $this->assertSame($expected, $actual, 'Enriched old_etag should match.');

        $actual = $event->get(NodeUpdatedV1::NEW_ETAG_FIELD);
        $expected = $newNode->get(UserV1::ETAG_FIELD);
        $this->assertSame($expected, $actual, 'Enriched new_etag should match.');
    }
}
