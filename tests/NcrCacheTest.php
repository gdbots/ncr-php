<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr;

use Gdbots\Ncr\NcrCache;
use Gdbots\Ncr\NcrLazyLoader;
use Gdbots\Pbjx\DefaultPbjx;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Tests\Ncr\Fixtures\FakeNode;

class NcrCacheTest extends \PHPUnit_Framework_TestCase
{
    /** @var RegisteringServiceLocator */
    protected $locator;

    /** @var Pbjx */
    protected $pbjx;

    /** @var NcrLazyLoader */
    protected $ncrLazyLoader;

    /** @var NcrCache */
    protected $ncrCache;

    public function setUp()
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = new DefaultPbjx($this->locator);
        $this->ncrLazyLoader = new NcrLazyLoader($this->pbjx);
        $this->ncrCache = new NcrCache($this->ncrLazyLoader);
    }

    public function testHasNode()
    {
        $node = FakeNode::create();
        $nodeRef = NodeRef::fromNode($node);
        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
        $this->ncrCache->addNode($node);
        $this->assertTrue($this->ncrCache->hasNode($nodeRef));

        $this->ncrCache->removeNode($nodeRef);
        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
    }

    public function testGetNode()
    {
        $expectedNode = FakeNode::create();
        $nodeRef = NodeRef::fromNode($expectedNode);

        $this->ncrCache->addNode($expectedNode);
        $actualNode = $this->ncrCache->getNode($nodeRef);
        $this->assertSame($expectedNode, $actualNode);
        $expectedNode->freeze();

        $actualNode = $this->ncrCache->getNode($nodeRef);
        $this->assertNotSame($expectedNode, $actualNode);
        $this->assertTrue($expectedNode->equals($actualNode));
    }

    public function testClear()
    {
        $node1 = FakeNode::create();
        $node2 = FakeNode::create();

        $this->ncrCache->addNodes([$node1, $node2]);
        $this->assertTrue($this->ncrCache->hasNode(NodeRef::fromNode($node1)));
        $this->assertTrue($this->ncrCache->hasNode(NodeRef::fromNode($node2)));

        $this->ncrCache->clear();
        $this->assertFalse($this->ncrCache->hasNode(NodeRef::fromNode($node1)));
        $this->assertFalse($this->ncrCache->hasNode(NodeRef::fromNode($node2)));
    }
}
