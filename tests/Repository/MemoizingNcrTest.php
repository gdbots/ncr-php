<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\GetNodeBatchRequestHandler;
use Gdbots\Ncr\NcrCache;
use Gdbots\Ncr\NcrLazyLoader;
use Gdbots\Ncr\Repository\InMemoryNcr;
use Gdbots\Ncr\Repository\MemoizingNcr;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Pbjx\SimplePbjx;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;

class MemoizingNcrTest extends \PHPUnit_Framework_TestCase
{
    /** @var RegisteringServiceLocator */
    protected $locator;

    /** @var Pbjx */
    protected $pbjx;

    /** @var NcrLazyLoader */
    protected $ncrLazyLoader;

    /** @var NcrCache */
    protected $ncrCache;

    /** @var InMemoryNcr */
    protected $inMemoryNcr;

    /** @var MemoizingNcr */
    protected $ncr;

    public function setUp()
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = new SimplePbjx($this->locator);
        $this->ncrLazyLoader = new NcrLazyLoader($this->pbjx);
        $this->ncrCache = new NcrCache($this->ncrLazyLoader);
        $this->inMemoryNcr = new InMemoryNcr();
        $this->ncr = new MemoizingNcr($this->inMemoryNcr, $this->ncrCache, true);
    }

    public function testGetNode()
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);

        $this->inMemoryNcr->putNode($node);

        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
        $this->assertTrue($this->ncr->hasNode($nodeRef, true));
        $this->assertTrue($this->ncr->hasNode($nodeRef));
        $this->assertTrue($this->ncr->getNode($nodeRef)->equals($node));
    }

    public function testPutNode()
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);

        $this->ncr->putNode($node);

        $this->assertNotSame($this->ncr->getNode($nodeRef), $node);
        $this->assertTrue($this->ncr->getNode($nodeRef)->equals($node));
    }

    public function testGetNodes()
    {
        $node1 = UserV1::create();
        $node2 = UserV1::create();
        $nodeRef1 = NodeRef::fromNode($node1);
        $nodeRef2 = NodeRef::fromNode($node2);

        // add to inner ncr (decorated)
        $this->inMemoryNcr->putNode($node1);
        $this->inMemoryNcr->putNode($node2);

        // request from outer ncr (memoized)
        $nodes = $this->ncr->getNodes([$nodeRef1, $nodeRef2]);

        // result should contain them
        $this->assertArrayHasKey($nodeRef1->toString(), $nodes);
        $this->assertArrayHasKey($nodeRef2->toString(), $nodes);
    }

    public function testGetNodesFromMemoizerOnly()
    {
        $node1 = UserV1::create();
        $node2 = UserV1::create();
        $nodeRef1 = NodeRef::fromNode($node1);
        $nodeRef2 = NodeRef::fromNode($node2);

        // add them to memoizer (should populate NcrCache)
        $this->ncr->putNode($node1);
        $this->ncr->putNode($node2);

        // under the hood, remove from decorated Ncr
        $this->inMemoryNcr->deleteNode($nodeRef1);
        $this->inMemoryNcr->deleteNode($nodeRef2);
        $this->assertFalse($this->ncr->hasNode($nodeRef1, true));
        $this->assertFalse($this->ncr->hasNode($nodeRef2, true));

        // outer Ncr (memoized) should still have them
        $nodes = $this->ncr->getNodes([$nodeRef1, $nodeRef2]);
        $this->assertArrayHasKey($nodeRef1->toString(), $nodes);
        $this->assertArrayHasKey($nodeRef2->toString(), $nodes);
        $this->assertTrue($this->ncr->hasNode($nodeRef1));
        $this->assertTrue($this->ncr->hasNode($nodeRef2));
        $this->assertTrue($this->ncrCache->hasNode($nodeRef1));
        $this->assertTrue($this->ncrCache->hasNode($nodeRef2));

        // remove from NcrCache direct and retest
        $this->ncrCache->removeNode($nodeRef1);
        $this->assertFalse($this->ncr->hasNode($nodeRef1));
        $this->assertTrue($this->ncr->hasNode($nodeRef2));
        $this->assertFalse($this->ncrCache->hasNode($nodeRef1));
        $this->assertTrue($this->ncrCache->hasNode($nodeRef2));
    }

    public function testDeleteNode()
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);

        $this->inMemoryNcr->putNode($node);
        $this->ncr->deleteNode($nodeRef);

        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
        $this->assertFalse($this->ncr->hasNode($nodeRef, true));
        $this->assertFalse($this->ncr->hasNode($nodeRef));
    }

    public function testLazyLoad()
    {
        $expectedNode = UserV1::create();
        $nodeRef = NodeRef::fromNode($expectedNode);
        $this->inMemoryNcr->putNode($expectedNode);
        $this->ncrLazyLoader->addNodeRefs([$nodeRef]);

        $handler = new GetNodeBatchRequestHandler($this->ncr);
        $this->locator->registerRequestHandler(GetNodeBatchRequestV1::schema()->getCurie(), $handler);

        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
        $this->assertTrue($this->ncr->hasNode($nodeRef));
        $actualNode = $this->ncrCache->getNode($nodeRef);
        $this->assertTrue($expectedNode->equals($actualNode));
    }
}
