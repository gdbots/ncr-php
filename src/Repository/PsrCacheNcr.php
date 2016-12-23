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

// fixme: implement PsrCacheNcr
class PsrCacheNcr implements Ncr
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
    final public function createStorage(SchemaQName $qname, array $hints = [])
    {
        $this->next->createStorage($qname, $hints);
    }

    /**
     * {@inheritdoc}
     */
    final public function describeStorage(SchemaQName $qname, array $hints = []): string
    {
        return $this->next->describeStorage($qname, $hints);
    }

    /**
     * {@inheritdoc}
     */
    final public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): bool
    {
        if (!$consistent) {
            $cacheKey = $this->getCacheKey($nodeRef, $hints);
            $cacheItem = $this->cache->getItem($cacheKey);
            if ($cacheItem->isHit()) {
                return true;
            }
        }

        return $this->next->hasNode($nodeRef, $consistent, $hints);
    }

    /**
     * {@inheritdoc}
     */
    final public function getNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): Node
    {
        $cacheKey = $this->getCacheKey($nodeRef, $hints);
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

        $node = $this->next->getNode($nodeRef, $consistent, $hints);

        if ($this->readThrough) {
            if (null === $cacheItem) {
                $cacheItem = $this->cache->getItem($cacheKey);
            }

            $this->cache->saveDeferred($cacheItem->set($node)->expiresAfter(null)->expiresAt(null));
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    final public function getNodes(array $nodeRefs, bool $consistent = false, array $hints = []): array
    {
        if (empty($nodeRefs)) {
            return [];
        } elseif (count($nodeRefs) === 1) {
            try {
                $nodeRef = array_shift($nodeRefs);
                return [(string)$nodeRef => $this->getNode($nodeRef, $consistent, $hints)];
            } catch (NodeNotFound $e) {
                return [];
            } catch (\Exception $e) {
                throw $e;
            }
        }

        echo __CLASS__ . ' called' . PHP_EOL;
        $cachedNodes = [];
        $cacheKeys = [];
        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = [];

        if (!$consistent || $this->readThrough) {
            foreach ($nodeRefs as $nodeRef) {
                $cacheKeys[$nodeRef->toString()] = $this->getCacheKey($nodeRef, $hints);
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
//var_dump($cacheItem->getKey());
//var_dump($cacheItem->get());
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

        $nodes = empty($nodeRefs) ? [] : $this->next->getNodes($nodeRefs, $consistent, $hints);
        echo json_encode($nodeRefs, JSON_PRETTY_PRINT);

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

                    $this->cache->saveDeferred(
                        $cacheItems[$cacheKey]->set($nodes[$nodeRef])->expiresAfter(null)->expiresAt(null)
                    );
                }
            } else {
                // psr6 really needs a method to just create a cache item without incurring a lookup
                // Returns an array containing all the entries from array1 that are not present in any of the other arrays.
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

                    $this->cache->saveDeferred($cacheItems[$cacheKey]->set($node)->expiresAfter(null)->expiresAt(null));
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
    final public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void
    {
        $this->next->putNode($node, $expectedEtag, $hints);
        $nodeRef = NodeRef::fromNode($node);
        $cacheItem = $this->cache->getItem($this->getCacheKey($nodeRef, $hints));
        $this->cache->save($cacheItem->set($node)->expiresAfter(null)->expiresAt(null));
    }

    /**
     * {@inheritdoc}
     */
    final public function deleteNode(NodeRef $nodeRef, array $hints = []): void
    {
        $this->next->deleteNode($nodeRef, $hints);
        $this->cache->deleteItem($this->getCacheKey($nodeRef, $hints));
    }

    /**
     * {@inheritdoc}
     */
    final public function findNodeRefs(IndexQuery $query, array $hints = []): IndexQueryResult
    {
        return $this->next->findNodeRefs($query, $hints);
    }

    /**
     * {@inheritdoc}
     */
    final public function streamNodes(SchemaQName $qname, callable $callback, array $hints = []): void
    {
        $this->next->streamNodes($qname, $callback, $hints);
    }

    /**
     * {@inheritdoc}
     */
    final public function streamNodeRefs(SchemaQName $qname, callable $callback, array $hints = []): void
    {
        $this->next->streamNodeRefs($qname, $callback, $hints);
    }

    /**
     * Returns the cache key to use for the provided NodeRef.
     * This must be compliant with psr6 "Key" definition.
     *
     * @link http://www.php-fig.org/psr/psr-6/#definitions
     *
     * The "-php" suffix here is used because the cache item
     * will be stored as serialized php.
     *
     * @param NodeRef $nodeRef
     * @param array   $hints
     *
     * @return string
     */
    protected function getCacheKey(NodeRef $nodeRef, array $hints = []): string
    {
        return str_replace('-', '_', sprintf(
            '%s.%s.%s.php',
            $nodeRef->getVendor(),
            $nodeRef->getLabel(),
            md5($nodeRef->getId())
        ));
    }
}
