<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Forms\Command\CreateFormV1;
use Acme\Schemas\Forms\Node\FormV1;
use Gdbots\Ncr\Validator\NodeIdempotencyValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use PHPUnit\Framework\TestCase;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

class NodeIdempotencyValidatorTest extends TestCase
{
    /** @var RegisteringServiceLocator */
    protected $locator;

    /** @var Pbjx */
    protected $pbjx;

    /** @var CacheItemPoolInterface */
    protected $cache;

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
        $command = CreateFormV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        $this->assertTrue(true, 'no exception should be thrown without existing cache entry');
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeWithExistingTitle(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()->set('title', 'Existing Title');

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $this->cache->save($this->cache->getItem($cacheKey)->set(true));
        }

        $command = CreateFormV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeWithExistingSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()->set('slug', 'existing-slug');

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $this->cache->save($this->cache->getItem($cacheKey)->set(true));
        }

        $command = CreateFormV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    public function testOnCreateNodeAfterHandle(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create()
            ->set('title', 'A Title')
            ->set('slug', 'slug');
        $command = CreateFormV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->onCreateNodeAfterHandle($pbjxEvent);

        foreach ($validator->getIdempotencyKeys($node) as $cacheKey) {
            $cacheItem = $this->cache->getItem($cacheKey);
            $this->assertTrue($cacheItem->isHit());
            $this->assertTrue($cacheItem->get());
        }
    }
}
