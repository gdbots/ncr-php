<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Common\Util\SlugUtils;
use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Psr\Cache\CacheItemPoolInterface;
use Psr\Cache\CacheItemInterface;
use Gdbots\Pbj\SchemaQName;

class NodeIdempotencyValidator implements EventSubscriber, PbjxValidator
{
    /** @var CacheItemPoolInterface */
    protected $cache;
    protected $ttl;

    /**
     * @param CacheItemPoolInterface $cache
     * @param int $ttl sets the expire time for cache items
     */
    public function __construct(CacheItemPoolInterface $cache, int $ttl = 60)
    {
      $this->cache = $cache;
      $this->ttl= $ttl;
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('node'), 'Field "node" is required.', 'node');

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = [];
        /** @var Message $node */
        $node = $command->get('node');
        $qname = $node::schema()->getQName();

        $keys = [
            $this->getCacheKey($qname, SlugUtils::create($node->get('title'))) => 'title',
        ];
        if ($node->has('slug')) {
            $keys[$this->getCacheKey($qname, $node->get('slug'))] = 'slug';
        }

        $cacheItems = $this->cache->getItems(array_keys($keys));
        if ($cacheItems instanceof \Traversable) {
            $cacheItems = iterator_to_array($cacheItems);
        }

        /** @var CacheItemInterface $cacheItem */
        foreach ($keys as $cacheKey => $value) {
            $cacheItem = $cacheItems[$cacheKey];
            if (!$cacheItem->isHit()) {
                continue;
            }

            throw new NodeAlreadyExists(
                sprintf(
                    'The [%s] with [%s] [%s] already exists so [%s] cannot continue.',
                    $node::schema()->getCurie()->getMessage(),
                    $value,
                    $node->get($value),
                    $command->generateMessageRef()
                )
            );
        }
    }

    /**
     * Handler will actually save the cache items.
     *
     * @param PbjxEvent $pbjxEvent
     */
    public function onCreateNodeAfterHandler(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = [];
        /** @var Message $node */
        $node = $command->get('node');

        $qname = $node::schema()->getQName();
        $keys = [
            $this->getCacheKey($qname, SlugUtils::create($node->get('title'))) => 'title',
        ];
        if ($node->has('slug')) {
            $keys[$this->getCacheKey($qname, $node->get('slug'))] = 'slug';
        }

        $cacheItems = $this->cache->getItems(array_keys($keys));
        if ($cacheItems instanceof \Traversable) {
            $cacheItems = iterator_to_array($cacheItems);
        }

        /** @var CacheItemInterface $cacheItem */
        foreach ($cacheItems as $cacheItem) {
            $value = $keys[$cacheItem->getKey()];
            if ($this->ttl > 0) {
                $cacheItem->expiresAfter($this->ttl);
            }
            $this->cache->saveDeferred($cacheItem->set($value));
        }
    }

    /**
     * Creates the cache key based from the node's qname appended with it's title/slug value.
     *
     * @param SchemaQName $qname in format `acme:video`
     * @param string $sluggifiedValue the sluggified string
     * @param array $context
     *
     * @return string return a string in this format for example `acme_video.some_title.php`
     */
    protected function getCacheKey(SchemaQName $qname, string $sluggifiedValue, array $context = []): string
    {
        $qnameStr = str_replace(':', '_', $qname);
        return str_replace('-', '_', sprintf(
            '%s.%s.php',
            $qnameStr,
            $sluggifiedValue
        ));
    }


    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:create-node.validate' => 'validateCreateNode',
            'gdbots:ncr:mixin:create-node.after_handle' => 'onCreateNodeAfterHandler',
        ];
    }
}
