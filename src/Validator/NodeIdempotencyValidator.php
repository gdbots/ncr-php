<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Common\Util\SlugUtils;
use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Iam\Mixin\User\User;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

class NodeIdempotencyValidator implements EventSubscriber, PbjxValidator
{
    /** @var CacheItemPoolInterface */
    protected $cache;

    /** @var array */
    protected $ttl = ['default' => 60];

    /**
     * @param CacheItemPoolInterface $cache
     * @param array                  $ttl
     */
    public function __construct(CacheItemPoolInterface $cache, array $ttl = [])
    {
        $this->cache = $cache;
        // defaults
        $ttl += ['default' => 60];
        $this->ttl = $ttl;
    }

    /**
     * @param PbjxEvent $pbjxEvent
     *
     * @throws NodeAlreadyExists
     */
    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        if (!$command->has('node')) {
            return;
        }

        /** @var Node $node */
        $node = $command->get('node');
        if ($this->shouldIgnoreNode($node)) {
            return;
        }

        $cacheKeys = $this->getCacheKeys($node);

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = $this->cache->getItems(array_keys($cacheKeys));

        foreach ($cacheItems as $cacheItem) {
            if (!$cacheItem->isHit()) {
                continue;
            }

            $field = $cacheKeys[$cacheItem->getKey()];
            throw new NodeAlreadyExists(
                sprintf(
                    'The [%s] with %s [%s] already exists so [%s] cannot continue.',
                    $node::schema()->getCurie()->getMessage(),
                    $field,
                    $node->get($field),
                    $command->generateMessageRef()
                )
            );
        }
    }

    /**
     * Populates cache keys so idempotency check can reject duplicates
     * after nodes are successfully created.
     *
     * @param PbjxEvent $pbjxEvent
     */
    public function onCreateNodeAfterHandle(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        if (!$command->has('node')) {
            return;
        }

        /** @var Node $node */
        $node = $command->get('node');
        if ($this->shouldIgnoreNode($node)) {
            return;
        }

        $cacheKeys = $this->getCacheKeys($node);

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = $this->cache->getItems(array_keys($cacheKeys));

        foreach ($cacheItems as $cacheItem) {
            $field = $cacheKeys[$cacheItem->getKey()];
            $cacheItem->set($field)->expiresAfter($this->getCacheTtl($node));
            $this->cache->saveDeferred($cacheItem);
        }
    }

    /**
     * Override to provide logic which can ignore when the idempotency
     * check is performed, useful for when you expect to get a lot of
     * nodes with the same title, email, etc.
     *
     * @param Node $node
     *
     * @return bool
     */
    protected function shouldIgnoreNode(Node $node): bool
    {
        return false;
    }

    /**
     * Derive the cache keys to use for the idempotency check
     * from the node itself.
     *
     * @param Node $node
     *
     * @return array
     */
    protected function getCacheKeys(Node $node): array
    {
        $qname = $node::schema()->getQName();
        $cacheKeys = [];

        if ($node->has('title')) {
            $cacheKeys[$this->getCacheKey($qname, SlugUtils::create($node->get('title')))] = 'title';
        }

        if ($node->has('slug')) {
            $cacheKeys[$this->getCacheKey($qname, $node->get('slug'))] = 'slug';
        }

        if ($node instanceof User && $node->has('email')) {
            $cacheKeys[$this->getCacheKey($qname, $node->get('email'))] = 'email';
        }

        return $cacheKeys;
    }

    /**
     * Returns the cache key to use for the provided NodeRef.
     * This must be compliant with psr6 "Key" definition.
     *
     * @link http://www.php-fig.org/psr/psr-6/#definitions
     *
     * The ".php" suffix here is used because the cache item
     * will be stored as serialized php.
     *
     * @param SchemaQName $qname
     * @param string      $key
     *
     * @return string
     */
    protected function getCacheKey(SchemaQName $qname, string $key): string
    {
        // niv (node imdempotency validator) prefix is to avoid collision
        return str_replace('-', '_', sprintf(
            'niv.%s.%s.%s.php',
            $qname->getVendor(),
            $qname->getMessage(),
            md5($key)
        ));
    }

    /**
     * @param Node $node
     *
     * @return int
     */
    protected function getCacheTtl(Node $node): int
    {
        return $this->ttl[$node::schema()->getQName()->toString()] ?? $this->ttl['default'];
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:create-node.validate'     => 'validateCreateNode',
            'gdbots:ncr:mixin:create-node.after_handle' => 'onCreateNodeAfterHandle',
        ];
    }
}
