<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Iam\Node\UserV1;
use Gdbots\Ncr\NcrLazyLoader;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\MessageRef;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\SimplePbjx;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;
use PHPUnit\Framework\TestCase;

class NcrLazyLoaderTest extends TestCase
{
    protected RegisteringServiceLocator $locator;
    protected Pbjx $pbjx;
    protected NcrLazyLoader $ncrLazyLoader;

    public function setUp(): void
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = new SimplePbjx($this->locator);
        $this->ncrLazyLoader = new NcrLazyLoader($this->pbjx);
    }

    public function testHasNodeRef(): void
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

    public function testAddEmbeddedNodeRefs(): void
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

    public function testClear(): void
    {
        $nodeRef1 = NodeRef::fromString('acme:user:123');
        $nodeRef2 = NodeRef::fromString('acme:user:abc');
        $this->ncrLazyLoader->addNodeRefs([$nodeRef1, $nodeRef2]);

        $this->ncrLazyLoader->clear();

        $this->assertFalse($this->ncrLazyLoader->hasNodeRef($nodeRef1));
        $this->assertFalse($this->ncrLazyLoader->hasNodeRef($nodeRef2));
    }

    public function testFlush(): void
    {
        $handler = new class implements RequestHandler {
            public bool $worked = false;

            public function handleRequest(Message $request, Pbjx $pbjx): Message
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
