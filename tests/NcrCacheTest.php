<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\NcrCache;
use Gdbots\Ncr\NcrLazyLoader;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Pbjx\SimplePbjx;
use PHPUnit\Framework\TestCase;

class NcrCacheTest extends TestCase
{
    protected RegisteringServiceLocator $locator;
    protected Pbjx $pbjx;
    protected NcrLazyLoader $ncrLazyLoader;
    protected NcrCache $ncrCache;

    public function setUp(): void
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = new SimplePbjx($this->locator);
        $this->ncrLazyLoader = new NcrLazyLoader($this->pbjx);
        $this->ncrCache = new NcrCache($this->ncrLazyLoader, 10);
    }

    public function testHasNode(): void
    {
        $node = UserV1::create();
        $nodeRef = NodeRef::fromNode($node);
        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
        $this->ncrCache->addNode($node);
        $this->assertTrue($this->ncrCache->hasNode($nodeRef));

        $this->ncrCache->removeNode($nodeRef);
        $this->assertFalse($this->ncrCache->hasNode($nodeRef));
    }

    public function testGetNode(): void
    {
        $expectedNode = UserV1::create();
        $nodeRef = NodeRef::fromNode($expectedNode);

        $this->ncrCache->addNode($expectedNode);
        $actualNode = $this->ncrCache->getNode($nodeRef);
        $this->assertSame($expectedNode, $actualNode);
        $expectedNode->freeze();

        $actualNode = $this->ncrCache->getNode($nodeRef);
        $this->assertNotSame($expectedNode, $actualNode);
        $this->assertTrue($expectedNode->equals($actualNode));
    }

    public function testClear(): void
    {
        $node1 = UserV1::create();
        $node2 = UserV1::create();

        $this->ncrCache->addNodes([$node1, $node2]);
        $this->assertTrue($this->ncrCache->hasNode(NodeRef::fromNode($node1)));
        $this->assertTrue($this->ncrCache->hasNode(NodeRef::fromNode($node2)));

        $this->ncrCache->clear();
        $this->assertFalse($this->ncrCache->hasNode(NodeRef::fromNode($node1)));
        $this->assertFalse($this->ncrCache->hasNode(NodeRef::fromNode($node2)));
    }

    public function testPrune(): void
    {
        $nodes = [];
        $count = 10;

        $i = 0;
        do {
            $nodes[] = UserV1::create();
            $i++;
        } while ($i < $count);

        foreach ($nodes as $node) {
            $this->ncrCache->addNode($node);
            $this->assertTrue($this->ncrCache->hasNode(NodeRef::fromNode($node)));
        }

        // these will cause a prune to occur
        $this->ncrCache->addNode(UserV1::create());
        $this->ncrCache->addNode(UserV1::create());

        $found = 0;
        foreach ($nodes as $node) {
            if ($this->ncrCache->hasNode(NodeRef::fromNode($node))) {
                $found++;
            }
        }

        // 20% of the cache should have been removed
        $this->assertSame($count - (int)($count * .2), $found);
    }
}
