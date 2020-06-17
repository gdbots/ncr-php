<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Forms\Node\FormV1;
use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Ncr\Validator\NodeIdempotencyValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use PHPUnit\Framework\TestCase;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

class NodeIdempotencyValidatorTest extends TestCase
{
    protected RegisteringServiceLocator $locator;
    protected Pbjx $pbjx;
    protected CacheItemPoolInterface $cache;

    public function setUp(): void
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = $this->locator->getPbjx();
        $this->cache = new ArrayAdapter();
        PbjxEvent::setPbjx($this->pbjx);
    }

    public function testValidateCreateNode(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()
            ->set('title', 'A Title')
            ->set('slug', 'slug');
        $command = CreateNodeV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        $this->assertTrue(true, 'no exception should be thrown without existing cache entry');
    }

    public function testValidateCreateNodeWithExistingTitle(): void
    {
        $this->expectException(NodeAlreadyExists::class);
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()->set('title', 'Existing Title');

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $this->cache->save($this->cache->getItem($cacheKey)->set(true));
        }

        $command = CreateNodeV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    public function testValidateCreateNodeWithExistingSlug(): void
    {
        $this->expectException(NodeAlreadyExists::class);
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()->set('slug', 'existing-slug');

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $this->cache->save($this->cache->getItem($cacheKey)->set(true));
        }

        $command = CreateNodeV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    public function testOnCreateNodeAfterHandle(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()
            ->set('title', 'A Title')
            ->set('slug', 'slug');

        $command = CreateNodeV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->onCreateNodeAfterHandle($pbjxEvent);

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $cacheItem = $this->cache->getItem($cacheKey);
            $this->assertTrue($cacheItem->isHit());
            $this->assertTrue($cacheItem->get());
        }
    }
}
