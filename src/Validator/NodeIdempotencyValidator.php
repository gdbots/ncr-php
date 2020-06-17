<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\SlugUtil;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Iam\Mixin\User\UserV1Mixin;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Mixin\CreateNode\CreateNodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\SluggableV1Mixin;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

class NodeIdempotencyValidator implements EventSubscriber, PbjxValidator
{
    protected CacheItemPoolInterface $cache;
    protected array $ttl;

    public static function getSubscribedEvents()
    {
        return [
            CreateNodeV1Mixin::SCHEMA_CURIE . '.validate'     => 'validateCreateNode',
            CreateNodeV1Mixin::SCHEMA_CURIE . '.after_handle' => 'onCreateNodeAfterHandle',
            CreateNodeV1::SCHEMA_CURIE . '.validate'          => 'validateCreateNode',
            CreateNodeV1::SCHEMA_CURIE . '.after_handle'      => 'onCreateNodeAfterHandle',
        ];
    }

    public function __construct(CacheItemPoolInterface $cache, array $ttl = [])
    {
        $this->cache = $cache;
        $ttl += ['default' => 60];
        $this->ttl = $ttl;
    }

    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        if (!$command->has(CreateNodeV1::NODE_FIELD)) {
            return;
        }

        /** @var Message $node */
        $node = $command->get(CreateNodeV1::NODE_FIELD);
        if ($this->shouldIgnoreNode($node)) {
            return;
        }

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = $this->cache->getItems($this->getIdempotencyKeys($node));

        foreach ($cacheItems as $cacheItem) {
            if (!$cacheItem->isHit()) {
                continue;
            }

            throw new NodeAlreadyExists(
                sprintf(
                    'A similar [%s] was just created moments ago so [%s] cannot continue.',
                    $node::schema()->getCurie()->getMessage(),
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
        if (!$command->has(CreateNodeV1::NODE_FIELD)) {
            return;
        }

        /** @var Message $node */
        $node = $command->get(CreateNodeV1::NODE_FIELD);
        if ($this->shouldIgnoreNode($node)) {
            return;
        }

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = $this->cache->getItems($this->getIdempotencyKeys($node));

        foreach ($cacheItems as $cacheItem) {
            $cacheItem->set(true)->expiresAfter($this->getCacheTtl($node));
            $this->cache->saveDeferred($cacheItem);
        }
    }

    /**
     * Derives the keys to use for the idempotency check from the node itself.
     *
     * @param Message $node
     *
     * @return array
     */
    public function getIdempotencyKeys(Message $node): array
    {
        $qname = $node::schema()->getQName();
        $keys = [];

        if ($node->has(NodeV1Mixin::TITLE_FIELD)) {
            $keys[$this->getCacheKey($qname, SlugUtil::create($node->get(NodeV1Mixin::TITLE_FIELD)))] = true;
        }

        if ($node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $keys[$this->getCacheKey($qname, $node->get(SluggableV1Mixin::SLUG_FIELD))] = true;
        }

        if ($node->has(UserV1Mixin::EMAIL_FIELD)
            && $node::schema()->hasMixin(UserV1Mixin::SCHEMA_CURIE)
        ) {
            $keys[$this->getCacheKey($qname, $node->get(UserV1Mixin::EMAIL_FIELD))] = true;
        }

        return array_keys($keys);
    }

    /**
     * Override to provide logic which can ignore when the idempotency
     * check is performed, useful for when you expect to get a lot of
     * nodes with the same title, email, etc.
     *
     * @param Message $node
     *
     * @return bool
     */
    protected function shouldIgnoreNode(Message $node): bool
    {
        return false;
    }

    /**
     * Returns the cache key to use for the provided NodeRef.
     * This must be compliant with psr6 "Key" definition.
     *
     * @link http://www.php-fig.org/psr/psr-6/#definitions
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
            'niv.%s.%s.%s',
            $qname->getVendor(),
            $qname->getMessage(),
            md5($key)
        ));
    }

    protected function getCacheTtl(Message $node): int
    {
        return $this->ttl[$node::schema()->getQName()->toString()] ?? $this->ttl['default'];
    }
}
