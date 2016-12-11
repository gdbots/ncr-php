<?php

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
        $this->ncr->putNode($node);
        $this->assertTrue($this->ncr->hasNode(NodeRef::fromNode($node)));
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
}
