<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository;

use Acme\Schemas\Forms\Node\FormV1;
use Acme\Schemas\Forms\Command\CreateFormV1;
use Psr\Cache\CacheItemPoolInterface;
use Psr\Cache\CacheItemInterface;
use Gdbots\Ncr\Validator\NodeIdempotencyValidator;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\RegisteringServiceLocator;
use PHPUnit\Framework\TestCase;

class InMemoryCacheItem implements CacheItemInterface {
    protected $key;
    protected $value;

    public function __construct($key, $value = null)
    {
        $this->key = $key;
        $this->value = $value;
    }

    public function getKey()
    {
        return $this->key;
    }

    public function get()
    {
        return $this->value;
    }

    public function isHit()
    {
        if(is_null($this->value)) {
            return false;
        }
        return true;
    }

    public function set($value): void
    {
        $this->value = $value;
    }

    public function expiresAt($date) {}
    public function expiresAfter($date) {}
}

class InMemoryCache implements CacheItemPoolInterface {
    protected $storage = [];

    public function getItems(array $keys = array())
    {
        $items = [];
        foreach ($keys as $key) {
            if (!isset($this->storage[$key])) {
                $cacheItem = new InMemoryCacheItem($key);
            }
            else {
                $cacheItem = $this->storage[$key];
            }
            $items[$cacheItem->getKey()] = $cacheItem;
        }
        return $items;
    }

    public function clear()
    {
        $this->storage = [];
    }

    public function save(CacheItemInterface $item)
    {
        $this->storage[$item->getKey()] = $item;
    }

    public function getItem($key){}
    public function saveDeferred(CacheItemInterface $item) {}
    public function commit() {}
    public function hasItem($key) {}
    public function deleteItem($key) {}
    public function deleteItems(array $keys = array()) {}
}

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
        $this->cache = new InMemoryCache();
        PbjxEvent::setPbjx($this->pbjx);
    }

    public function testValidateCreateNodeThatDoesNotExist(): void
    {
        $node = FormV1::create();
        $command = CreateFormV1::create();

        $node->set('title', 'title 1');
        $node->set('slug', 'title-1');
        $command->set('node', $node);

        $validator = new NodeIdempotencyValidator($this->cache);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);

        // passed at this point
        $this->assertTrue(true);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeOnExistingTitle(): void
    {
        $node = FormV1::create();

        $this->storeCacheItem("acme_form.existing_title.php");

        $command = CreateFormV1::create();
        $node->set('title', 'Existing Title');
        $command->set('node', $node);

        $validator = new NodeIdempotencyValidator($this->cache);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }

    /**
     * @expectedException \Gdbots\Ncr\Exception\NodeAlreadyExists
     */
    public function testValidateCreateNodeOnExistingSlug(): void
    {
        $node = FormV1::create();

        $this->storeCacheItem("acme_form.existing_slug.php");

        $command = CreateFormV1::create();
        $node->set('slug', 'existing-slug');
        $command->set('node', $node);

        $validator = new NodeIdempotencyValidator($this->cache);
        $pbjxEvent = new PbjxEvent($command);

        $validator->validateCreateNode($pbjxEvent);
    }


    protected function storeCacheItem ($key) {
        // just do inline
        $cacheItem = new InMemoryCacheItem($key, true);
        $this->cache->save($cacheItem);
    }

}
