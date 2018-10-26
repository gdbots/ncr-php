<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Forms\Node\FormV1;
use Acme\Schemas\Forms\Command\CreateFormV1;
use Psr\Cache\CacheItemPoolInterface;
use Gdbots\Ncr\Validator\NodeIdempotencyValidator;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\RegisteringServiceLocator;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

class NodeIdempotencyValidatorTest extends TestCase
{
    /** @var RegisteringServiceLocator */
    protected $locator;

    /** @var Pbjx */
    protected $pbjx;

    /** @var CacheItemPoolInterface */
    protected $cache;

    public function setUp()
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = $this->locator->getPbjx();
        $this->cache = new ArrayAdapter();
        PbjxEvent::setPbjx($this->pbjx);
    }

    public function testValidateCreateNodeOnNoTitleAndSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $expectedCacheKey = 'acme_form.some_title.php';
        $expectedValue = 'title';
        $this->cache->saveDeferred($this->cache->getItem($expectedCacheKey)->set($expectedValue));

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        $this->assertTrue(true);
    }

    public function testValidateCreateNodeOnNonExistingTitle(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $expectedCacheKey = 'acme_form.some_existing_title.php';
        $expectedValue = 'title';
        $this->cache->saveDeferred($this->cache->getItem($expectedCacheKey)->set($expectedValue));

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('title', 'Unique Title');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        $this->assertTrue(true);
    }

    public function testValidateCreateNodeOnNonExistingSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $expectedCacheKey = 'acme_form.some_existing_slug.php';
        $expectedValue = 'slug';
        $this->cache->saveDeferred($this->cache->getItem($expectedCacheKey)->set($expectedValue));

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('slug', 'unique-slug');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        $this->assertTrue(true);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeOnExistingTitle(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $expectedCacheKey = 'acme_form.some_existing_title.php';
        $expectedValue = 'title';
        $this->cache->saveDeferred($this->cache->getItem($expectedCacheKey)->set($expectedValue));

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('title', 'Some Existing Title');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeOnExistingSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $expectedCacheKey = 'acme_form.some_existing_slug.php';
        $expectedValue = 'slug';
        $this->cache->saveDeferred($this->cache->getItem($expectedCacheKey)->set($expectedValue));

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('slug', 'some-existing-slug');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    public function testCreateNodeAfterHandlerOnNoTitleAndSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->onCreateNodeAfterHandler($pbjxEvent);

        $this->assertTrue(true);
    }

    public function testCreateNodeAfterHandlerOnSetTitleAndSlug(): void
    {
        $validator = new NodeIdempotencyValidator($this->cache);

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('title', 'unique-title');
        $node->set('slug', 'unique-slug');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);

        $validator->onCreateNodeAfterHandler($pbjxEvent);

        $expectedCaches = [
            'acme_form.unique_title.php' => 'title',
            'acme_form.unique_slug.php' => 'slug',
        ];
        // assert that cache items are in the cache
        foreach (array_keys($expectedCaches) as $cacheKey) {
            $this->assertTrue($this->cache->getItem($cacheKey)->isHit());
        }
    }

    public function testCreateNodeAfterHandlerOnExpiredTTL(): void
    {
        // set a ttl of 1 sec
        $validator = new NodeIdempotencyValidator($this->cache, 1);

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('title', 'unique-title');
        $node->set('slug', 'unique-slug');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);
        $validator->onCreateNodeAfterHandler($pbjxEvent);

        sleep(1);

        $expectedCaches = [
            'acme_form.unique_title.php' => 'title',
            'acme_form.unique_slug.php' => 'slug',
        ];
        // assert that cache items are not found in the cache
        foreach (array_keys($expectedCaches) as $cacheKey) {
            $this->assertFalse($this->cache->getItem($cacheKey)->isHit());
        }
    }

    public function testCreateNodeAfterHandlerOnNonExpiredTTL(): void
    {
        // set a ttl of 2 secs
        $validator = new NodeIdempotencyValidator($this->cache, 2);

        $node = FormV1::create();
        $command = CreateFormV1::create();
        $node->set('title', 'unique-title');
        $node->set('slug', 'unique-slug');
        $command->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);
        $validator->onCreateNodeAfterHandler($pbjxEvent);

        sleep(1);

        $expectedCaches = [
            'acme_form.unique_title.php' => 'title',
            'acme_form.unique_slug.php' => 'slug',
        ];
        // assert that cache items are still found in the cache
        foreach (array_keys($expectedCaches) as $cacheKey) {
            $this->assertTrue($this->cache->getItem($cacheKey)->isHit());
        }
    }

}
