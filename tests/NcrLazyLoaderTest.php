<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\NcrLazyLoader;
use Gdbots\Pbj\MessageRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\SimplePbjx;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;
use Gdbots\Schemas\Pbjx\Mixin\Request\Request;
use Gdbots\Schemas\Pbjx\Mixin\Response\Response;
use PHPUnit\Framework\TestCase;

class NcrLazyLoaderTest extends TestCase
{
    /** @var RegisteringServiceLocator */
    protected $locator;

    /** @var Pbjx */
    protected $pbjx;

    /** @var NcrLazyLoader */
    protected $ncrLazyLoader;

    public function setUp()
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = new SimplePbjx($this->locator);
        $this->ncrLazyLoader = new NcrLazyLoader($this->pbjx);
    }

    public function testHasNodeRef()
    {
        $nodeRef1 = NodeRef::fromString('acme:user:123');
        $nodeRef2 = NodeRef::fromString('acme:user:abc');
        $this->ncrLazyLoader->addNodeRefs([$nodeRef1, $nodeRef2]);
        $this->assertTrue($this->ncrLazyLoader->hasNodeRef($nodeRef1));
        $this->assertTrue($this->ncrLazyLoader->hasNodeRef($nodeRef2));

        $this->ncrLazyLoader->removeNodeRefs([$nodeRef1]);
        $this->assertFalse($this->ncrLazyLoader->hasNodeRef($nodeRef1));
        $this->assertTrue($this->ncrLazyLoader->hasNodeRef($nodeRef2));
    }

    public function testaddEmbeddedNodeRefs()
    {
        $messageRef = MessageRef::fromString('acme:iam:node:user:homer');
        $nodeRef = NodeRef::fromMessageRef($messageRef);
        $node = UserV1::create()->set('creator_ref', $messageRef);
        $this->ncrLazyLoader->addEmbeddedNodeRefs([$node], [
            'creator_ref' => 'acme:user',
            '_id'         => 'acme:user',
        ]);
        $this->assertTrue($this->ncrLazyLoader->hasNodeRef($nodeRef));
        $this->assertTrue($this->ncrLazyLoader->hasNodeRef(NodeRef::fromString("acme:user:{$node->get('_id')}")));
    }

    public function testClear()
    {
        $nodeRef1 = NodeRef::fromString('acme:user:123');
        $nodeRef2 = NodeRef::fromString('acme:user:abc');
        $this->ncrLazyLoader->addNodeRefs([$nodeRef1, $nodeRef2]);

        $this->ncrLazyLoader->clear();

        $this->assertFalse($this->ncrLazyLoader->hasNodeRef($nodeRef1));
        $this->assertFalse($this->ncrLazyLoader->hasNodeRef($nodeRef2));
    }

    public function testFlush()
    {
        $handler = new class implements RequestHandler
        {
            public $worked = false;

            public function handleRequest(Request $request, Pbjx $pbjx): Response
            {
                $this->worked = $request->isInSet('node_refs', NodeRef::fromString('acme:user:123'));
                return GetNodeBatchResponseV1::create();
            }

            public static function handlesCuries(): array
            {
                return [];
            }
        };

        $this->locator->registerRequestHandler(GetNodeBatchRequestV1::schema()->getCurie(), $handler);
        $this->ncrLazyLoader->addNodeRefs([NodeRef::fromString('acme:user:123')]);
        $this->ncrLazyLoader->flush();
        $this->assertTrue($handler->worked);
    }
}
