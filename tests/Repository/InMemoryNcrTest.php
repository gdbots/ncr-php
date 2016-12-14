<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Gdbots\Ncr\Repository\InMemoryNcr;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Tests\Ncr\Fixtures\FakeNode;

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
        $node = FakeNode::create();
        $nodeRef = NodeRef::fromNode($node);

        $this->ncr->putNode($node);
        $this->assertTrue($this->ncr->hasNode($nodeRef));
        $this->ncr->deleteNode($nodeRef);
        $this->assertFalse($this->ncr->hasNode($nodeRef));
    }

    public function testGetAndPutNode()
    {
        $expectedNode = FakeNode::create();
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);
        $actualNode = $this->ncr->getNode($nodeRef);
        $this->assertTrue($actualNode->equals($expectedNode));
    }

    public function testDeleteNode()
    {
        $node = FakeNode::create();
        $this->ncr->putNode($node);
        $nodeRef = NodeRef::fromNode($node);
        $this->assertTrue($this->ncr->hasNode($nodeRef));

        $this->ncr->deleteNode($nodeRef);
        $this->assertFalse($this->ncr->hasNode($nodeRef));
    }

    public function testPutNodeWithValidExpectedEtag()
    {
        $expectedEtag = 'test';
        $expectedNode = FakeNode::create()->set('etag', $expectedEtag);
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
        $expectedNode = FakeNode::create()->set('etag', $expectedEtag);
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
        $expectedNode = FakeNode::create()->set('etag', $expectedEtag);
        $this->ncr->putNode($expectedNode);
        $nodeRef = NodeRef::fromNode($expectedNode);

        // expectedEtag should fail here because the node doesn't exist at all
        $this->ncr->deleteNode($nodeRef);
        $this->ncr->putNode($expectedNode, $expectedEtag);
    }
}
