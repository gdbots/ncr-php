<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\IndexQueryBuilder;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\Repository\InMemoryNcr;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Tests\Ncr\Fixtures\SimpsonsTrait;
use PHPUnit\Framework\TestCase;

class InMemoryNcrTest extends TestCase
{
    use SimpsonsTrait;

    /** @var InMemoryNcr */
    protected $ncr;

    public function setUp(): void
    {
        $this->ncr = new InMemoryNcr();
    }

    public function testHasNode()
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);

        $this->ncr->putNode($node);
        $this->assertTrue($this->ncr->hasNode($nodeRef));
        $this->ncr->deleteNode($nodeRef);
        $this->assertFalse($this->ncr->hasNode($nodeRef));
    }

    public function testGetAndPutNode()
    {
        $expectedNode = UserV1::create();
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);
        $actualNode = $this->ncr->getNode($nodeRef);
        $this->assertTrue($actualNode->equals($expectedNode));
    }

    public function testDeleteNode()
    {
        $node = UserV1::create();
        $this->ncr->putNode($node);
        $nodeRef = NodeRef::fromNode($node);
        $this->assertTrue($this->ncr->hasNode($nodeRef));

        $this->ncr->deleteNode($nodeRef);
        $this->assertFalse($this->ncr->hasNode($nodeRef));
    }

    public function testPutNodeWithValidExpectedEtag()
    {
        $expectedEtag = 'test';
        $expectedNode = UserV1::create()->set('etag', $expectedEtag);
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);

        $nextNode = $this->ncr->getNode($nodeRef);
        $this->ncr->putNode($nextNode, $expectedEtag);
        $this->assertSame($expectedNode->get('etag'), $nextNode->get('etag'));
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\OptimisticCheckFailed
     */
    public function testPutNodeWithInvalidExpectedEtag1()
    {
        $expectedEtag = 'test';
        $expectedNode = UserV1::create()->set('etag', $expectedEtag);
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);

        $invalidEtag = 'test2';
        $nextNode = $this->ncr->getNode($nodeRef);
        $this->ncr->putNode($nextNode, $invalidEtag);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\OptimisticCheckFailed
     */
    public function testPutNodeWithInvalidExpectedEtag2()
    {
        $expectedEtag = 'test';
        $expectedNode = UserV1::create()->set('etag', $expectedEtag);
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);

        // expectedEtag should fail here because the node doesn't exist at all
        $this->ncr->deleteNode($nodeRef);
        $this->ncr->putNode($expectedNode, $expectedEtag);
    }

    public function testGetNodes()
    {
        $nodes = [];
        $nodeRefs = [];

        for ($i = 0; $i < 50; $i++) {
            $node = UserV1::create();
            $nodeRef = NodeRef::fromNode($node);
            $nodes[$nodeRef->toString()] = $node;
            $nodeRefs[] = $nodeRef;
        }

        foreach ($nodes as $nodeRef => $node) {
            $this->ncr->putNode($node);
        }

        /** @var Node[] $actualNodes */
        shuffle($nodeRefs);
        $expectedNodeRefs = array_slice($nodeRefs, 0, 2);
        $actualNodes = $this->ncr->getNodes($expectedNodeRefs);
        $actualNodeRefs = array_keys($actualNodes);
        sort($actualNodeRefs);
        $expectedNodeRefs = array_map('strval', $expectedNodeRefs);
        sort($expectedNodeRefs);
        $this->assertSame(array_values($actualNodeRefs), array_values($expectedNodeRefs));
        $this->assertCount(2, $actualNodes);

        foreach ($actualNodes as $actualNodeRef => $actualNode) {
            $this->assertTrue($actualNode->equals($nodes[$actualNodeRef]));
        }
    }

    public function testFindNodeRefs()
    {
        foreach ($this->getSimpsonsAsNodes() as $node) {
            $this->ncr->putNode($node);
        }

        foreach ($this->getSimpsonsIndexQueryFilterTests() as $test) {
            /** @var IndexQueryFilter $filter */
            $filter = array_shift($test['filters']);
            $qb = IndexQueryBuilder::create(
                SchemaQName::fromString('gdbots:fake-node'),
                $filter->getField(),
                (string)$filter->getValue()
            );

            foreach ($test['filters'] as $filter) {
                $qb->addFilter($filter);
            }

            $result = $this->ncr->findNodeRefs($qb->build());
            $expectedNodeRefs = array_map('strval', $test['expected']);
            $actualNodeRefs = array_map('strval', $result->getNodeRefs());

            sort($expectedNodeRefs);
            sort($actualNodeRefs);

            $this->assertEquals($expectedNodeRefs, $actualNodeRefs, "Test filter [{$test['name']}] failed.");
        }
    }

    public function testFindNodeRefsPaged1()
    {
        foreach ($this->getSimpsonsAsNodes() as $node) {
            $this->ncr->putNode($node);
        }

        $qb = IndexQueryBuilder::create(SchemaQName::fromString('gdbots:fake-node'), 'status', 'draft')
            ->filterLt('age', 39)
            ->setCount(1);

        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(1, $result);
        $this->assertTrue($result->hasMore());
        $this->assertEquals([NodeRef::fromString('gdbots:fake-node:bart')], $result->getNodeRefs());

        $qb->setCursor($result->getNextCursor());
        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(1, $result);
        $this->assertTrue($result->hasMore());
        $this->assertEquals([NodeRef::fromString('gdbots:fake-node:lisa')], $result->getNodeRefs());

        $qb->setCursor($result->getNextCursor());
        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(1, $result);
        $this->assertTrue($result->hasMore());
        $this->assertEquals([NodeRef::fromString('gdbots:fake-node:maggie')], $result->getNodeRefs());

        $qb->setCursor($result->getNextCursor());
        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(1, $result);
        $this->assertTrue($result->hasMore());
        $this->assertEquals([NodeRef::fromString('gdbots:fake-node:marge')], $result->getNodeRefs());

        $qb->setCursor($result->getNextCursor());
        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(1, $result);
        $this->assertFalse($result->hasMore());
        $this->assertEquals([NodeRef::fromString('gdbots:fake-node:milhouse')], $result->getNodeRefs());
    }

    public function testFindNodeRefsPaged3()
    {
        foreach ($this->getSimpsonsAsNodes() as $node) {
            $this->ncr->putNode($node);
        }

        $qb = IndexQueryBuilder::create(SchemaQName::fromString('gdbots:fake-node'), 'status', 'draft')
            ->filterLt('age', 39)
            ->setCount(3);

        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(3, $result);
        $this->assertTrue($result->hasMore());
        $this->assertEquals(
            [
                NodeRef::fromString('gdbots:fake-node:bart'),
                NodeRef::fromString('gdbots:fake-node:lisa'),
                NodeRef::fromString('gdbots:fake-node:maggie'),
            ],
            $result->getNodeRefs()
        );

        $qb->setCursor($result->getNextCursor());
        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(2, $result);
        $this->assertFalse($result->hasMore());
        $this->assertEquals(
            [
                NodeRef::fromString('gdbots:fake-node:marge'),
                NodeRef::fromString('gdbots:fake-node:milhouse'),
            ],
            $result->getNodeRefs()
        );
    }


    public function testFindNodeRefsPaged25()
    {
        foreach ($this->getSimpsonsAsNodes() as $node) {
            $this->ncr->putNode($node);
        }

        $qb = IndexQueryBuilder::create(SchemaQName::fromString('gdbots:fake-node'), 'status', 'draft')
            ->filterLt('age', 39)
            ->setCount(25);

        $result = $this->ncr->findNodeRefs($qb->build());
        $this->assertCount(5, $result);
        $this->assertFalse($result->hasMore());
        $this->assertEquals(
            [
                NodeRef::fromString('gdbots:fake-node:bart'),
                NodeRef::fromString('gdbots:fake-node:lisa'),
                NodeRef::fromString('gdbots:fake-node:maggie'),
                NodeRef::fromString('gdbots:fake-node:marge'),
                NodeRef::fromString('gdbots:fake-node:milhouse'),
            ],
            $result->getNodeRefs()
        );
    }
}
