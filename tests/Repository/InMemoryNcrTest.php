<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\Repository\InMemoryNcr;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

class InMemoryNcrTest extends \PHPUnit_Framework_TestCase
{
    /** @var InMemoryNcr */
    protected $ncr;

    public function setUp()
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
        asort($actualNodeRefs);
        $expectedNodeRefs = array_map('strval', $expectedNodeRefs);
        asort($expectedNodeRefs);
        $this->assertSame(array_values($actualNodeRefs), array_values($expectedNodeRefs));
        $this->assertCount(2, $actualNodes);

        foreach ($actualNodes as $actualNodeRef => $actualNode) {
            $this->assertTrue($actualNode->equals($nodes[$actualNodeRef]));
        }
    }
}
