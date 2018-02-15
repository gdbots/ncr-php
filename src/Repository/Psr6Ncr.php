<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

class Psr6Ncr implements Ncr
{
    /** @var Ncr */
    private $next;

    /** @var CacheItemPoolInterface */
    private $cache;

    /**
     * If true, the cache pool will be updated when a cache miss occurs.
     *
     * @var bool
     */
    private $readThrough = true;

    /**
     * @param Ncr                    $next
     * @param CacheItemPoolInterface $cache
     * @param bool                   $readThrough
     */
    public function __construct(Ncr $next, CacheItemPoolInterface $cache, bool $readThrough = true)
    {
        $this->next = $next;
        $this->cache = $cache;
        $this->readThrough = $readThrough;
    }

    /**
     * {@inheritdoc}
     */
    final public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $this->next->createStorage($qname, $context);
    }

    /**
     * {@inheritdoc}
     */
    final public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        return $this->next->describeStorage($qname, $context);
    }

    /**
     * {@inheritdoc}
     */
    final public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        if (!$consistent) {
            $cacheKey = $this->getCacheKey($nodeRef, $context);
            $cacheItem = $this->cache->getItem($cacheKey);
            if ($cacheItem->isHit()) {
                return true;
            }
        }

        return $this->next->hasNode($nodeRef, $consistent, $context);
    }

    /**
     * {@inheritdoc}
     */
    final public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Node
    {
        $cacheKey = $this->getCacheKey($nodeRef, $context);
        $cacheItem = null;

        if (!$consistent) {
            $cacheItem = $this->cache->getItem($cacheKey);
            if ($cacheItem->isHit()) {
                $node = $cacheItem->get();
                // if it's not a Node, it's a corrupt key
                if ($node instanceof Node) {
                    return $node;
                }
            }
        }

        $node = $this->next->getNode($nodeRef, $consistent, $context);

        if ($this->readThrough) {
            if (null === $cacheItem) {
                $cacheItem = $this->cache->getItem($cacheKey);
            }

            $this->beforeSaveCacheItem($cacheItem, $node);
            $this->cache->saveDeferred($cacheItem->set($node));
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    final public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        if (empty($nodeRefs)) {
            return [];
        } elseif (count($nodeRefs) === 1) {
            try {
                $nodeRef = array_shift($nodeRefs);
                return [(string)$nodeRef => $this->getNode($nodeRef, $consistent, $context)];
            } catch (NodeNotFound $e) {
                return [];
            } catch (\Throwable $e) {
                throw $e;
            }
        }

        $cachedNodes = [];
        $cacheKeys = [];
        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = [];

        if (!$consistent || $this->readThrough) {
            foreach ($nodeRefs as $nodeRef) {
                $cacheKeys[$nodeRef->toString()] = $this->getCacheKey($nodeRef, $context);
            }
        }

        if (!$consistent) {
            $cacheItems = $this->cache->getItems($cacheKeys);
            if ($cacheItems instanceof \Traversable) {
                $cacheItems = iterator_to_array($cacheItems);
            }

            /** @var NodeRef[] $nodeRefs */
            foreach ($nodeRefs as $idx => $nodeRef) {
                $cacheKey = $cacheKeys[$nodeRef->toString()];
                if (!isset($cacheItems[$cacheKey])) {
                    continue;
                }

                $cacheItem = $cacheItems[$cacheKey];
                if (!$cacheItem->isHit()) {
                    continue;
                }

                $cachedNode = $cacheItem->get();
                // if it's not a Node, it's a corrupt key
                if ($cachedNode instanceof Node) {
                    $cachedNodes[$nodeRef->toString()] = $cachedNode;
                    unset($nodeRefs[$idx]);
                }
            }
        }

        $nodes = empty($nodeRefs) ? [] : $this->next->getNodes($nodeRefs, $consistent, $context);

        if ($this->readThrough && !empty($nodes)) {
            if ($consistent) {
                // all items must be cached and no diff check is needed
                $cacheItems = $this->cache->getItems($cacheKeys);
                if ($cacheItems instanceof \Traversable) {
                    $cacheItems = iterator_to_array($cacheItems);
                }

                foreach ($cacheKeys as $nodeRef => $cacheKey) {
                    if (!isset($cacheItems[$cacheKey]) || !isset($nodes[$nodeRef])) {
                        continue;
                    }

                    $this->beforeSaveCacheItem($cacheItems[$cacheKey], $nodes[$nodeRef]);
                    $this->cache->saveDeferred($cacheItems[$cacheKey]->set($nodes[$nodeRef]));
                }
            } else {
                // psr6 really needs a method to just create a cache item without incurring a lookup
                $missingCacheKeys = array_values(array_diff($cacheKeys, array_keys($cacheItems)));
                $missingCacheItems = $this->cache->getItems($missingCacheKeys);
                if ($missingCacheItems instanceof \Traversable) {
                    $missingCacheItems = iterator_to_array($missingCacheItems);
                }
                $cacheItems += $missingCacheItems;

                foreach ($nodes as $nodeRef => $node) {
                    $cacheKey = $cacheKeys[$nodeRef];
                    if (!isset($cacheItems[$cacheKey])) {
                        continue;
                    }

                    $this->beforeSaveCacheItem($cacheItems[$cacheKey], $node);
                    $this->cache->saveDeferred($cacheItems[$cacheKey]->set($node));
                }
            }
        }

        if (!empty($cachedNodes)) {
            $nodes += $cachedNodes;
        }

        return $nodes;
    }

    /**
     * {@inheritdoc}
     */
    final public function putNode(Node $node, ?string $expectedEtag = null, array $context = []): void
    {
        $this->next->putNode($node, $expectedEtag, $context);
        $nodeRef = NodeRef::fromNode($node);
        $cacheItem = $this->cache->getItem($this->getCacheKey($nodeRef, $context));
        $this->beforeSaveCacheItem($cacheItem, $node);
        $this->cache->save($cacheItem->set($node));
    }

    /**
     * {@inheritdoc}
     */
    final public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        $this->next->deleteNode($nodeRef, $context);
        $this->cache->deleteItem($this->getCacheKey($nodeRef, $context));
    }

    /**
     * {@inheritdoc}
     */
    final public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        return $this->next->findNodeRefs($query, $context);
    }

    /**
     * {@inheritdoc}
     */
    final public function pipeNodes(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        $this->next->pipeNodes($qname, $receiver, $context);
    }

    /**
     * {@inheritdoc}
     */
    final public function pipeNodeRefs(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        $this->next->pipeNodeRefs($qname, $receiver, $context);
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
     * @param NodeRef $nodeRef
     * @param array   $context
     *
     * @return string
     */
    protected function getCacheKey(NodeRef $nodeRef, array $context): string
    {
        return str_replace('-', '_', sprintf(
            '%s.%s.%s.php',
            $nodeRef->getVendor(),
            $nodeRef->getLabel(),
            md5($nodeRef->getId())
        ));
    }

    /**
     * Prepares the cache item before it's saved into the cache pool.
     * By default, all nodes are stored in cache forever, to change this behavior
     * either set the default ttl on the cache provider, or tweak the expiry on
     * a per item basis by overriding this method.
     *
     * @param CacheItemInterface $cacheItem
     * @param Node               $node
     */
    protected function beforeSaveCacheItem(CacheItemInterface $cacheItem, Node $node): void
    {
        $cacheItem->expiresAfter(null)->expiresAt(null);
    }
}
