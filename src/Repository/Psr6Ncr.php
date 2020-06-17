<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

class Psr6Ncr implements Ncr
{
    private const NODE_NOT_FOUND = 'nnf';
    private const NODE_NOT_FOUND_TTL = 600;

    private Ncr $next;
    private CacheItemPoolInterface $cache;

    /**
     * If true, the cache pool will be updated when a cache miss occurs.
     *
     * @var bool
     */
    private bool $readThrough;

    public function __construct(Ncr $next, CacheItemPoolInterface $cache, bool $readThrough = true)
    {
        $this->next = $next;
        $this->cache = $cache;
        $this->readThrough = $readThrough;
    }

    final public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $this->next->createStorage($qname, $context);
    }

    final public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        return $this->next->describeStorage($qname, $context);
    }

    final public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        if (!$consistent) {
            $cacheKey = $this->getCacheKey($nodeRef, $context);
            $cacheItem = $this->cache->getItem($cacheKey);
            if ($cacheItem->isHit()) {
                $node = $cacheItem->get();
                if ($node instanceof Message) {
                    return true;
                }

                if (self::NODE_NOT_FOUND === $node) {
                    return false;
                }
            }
        }

        return $this->next->hasNode($nodeRef, $consistent, $context);
    }

    final public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Message
    {
        $cacheKey = $this->getCacheKey($nodeRef, $context);
        $cacheItem = null;

        if (!$consistent) {
            $cacheItem = $this->cache->getItem($cacheKey);
            if ($cacheItem->isHit()) {
                $node = $cacheItem->get();
                if ($node instanceof Message) {
                    return $node;
                }

                if (self::NODE_NOT_FOUND === $node) {
                    throw NodeNotFound::forNodeRef($nodeRef);
                }
            }
        }

        try {
            $node = $this->next->getNode($nodeRef, $consistent, $context);
        } catch (NodeNotFound $nf) {
            if ($this->readThrough) {
                if (null === $cacheItem) {
                    $cacheItem = $this->cache->getItem($cacheKey);
                }

                $cacheItem->set(self::NODE_NOT_FOUND)->expiresAfter(self::NODE_NOT_FOUND_TTL);
                $this->cache->saveDeferred($cacheItem);
            }

            throw $nf;
        } catch (\Throwable $e) {
            throw $e;
        }

        if ($this->readThrough) {
            if (null === $cacheItem) {
                $cacheItem = $this->cache->getItem($cacheKey);
            }

            $this->beforeSaveCacheItem($cacheItem, $node);
            $this->cache->saveDeferred($cacheItem->set($node));
        }

        return $node;
    }

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

        foreach ($nodeRefs as $nodeRef) {
            $cacheKeys[$nodeRef->toString()] = $this->getCacheKey($nodeRef, $context);
        }

        if (!$consistent || $this->readThrough) {
            $cacheItems = $this->cache->getItems($cacheKeys);
            if ($cacheItems instanceof \Traversable) {
                $cacheItems = iterator_to_array($cacheItems);
            }
        }

        if (!$consistent) {
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
                if ($cachedNode instanceof Message) {
                    $cachedNodes[$nodeRef->toString()] = $cachedNode;
                    unset($nodeRefs[$idx]);
                    continue;
                }

                if (self::NODE_NOT_FOUND === $cachedNode) {
                    unset($nodeRefs[$idx]);
                }
            }
        }

        if (empty($nodeRefs)) {
            return $cachedNodes;
        }

        $nodes = $this->next->getNodes($nodeRefs, $consistent, $context);

        if ($this->readThrough) {
            foreach ($nodeRefs as $nodeRef) {
                $nodeRefStr = $nodeRef->toString();
                $cacheKey = $cacheKeys[$nodeRefStr];
                if (!isset($cacheItems[$cacheKey])) {
                    continue;
                }

                $cacheItem = $cacheItems[$cacheKey];
                if (!isset($nodes[$nodeRefStr])) {
                    $cacheItem->set(self::NODE_NOT_FOUND)->expiresAfter(self::NODE_NOT_FOUND_TTL);
                    $this->cache->saveDeferred($cacheItem);
                    continue;
                }

                $this->beforeSaveCacheItem($cacheItem, $nodes[$nodeRefStr]);
                $this->cache->saveDeferred($cacheItem->set($nodes[$nodeRefStr]));
            }
        }

        if (!empty($cachedNodes)) {
            $nodes += $cachedNodes;
        }

        return $nodes;
    }

    final public function putNode(Message $node, ?string $expectedEtag = null, array $context = []): void
    {
        $this->next->putNode($node, $expectedEtag, $context);
        $nodeRef = NodeRef::fromNode($node);
        $cacheItem = $this->cache->getItem($this->getCacheKey($nodeRef, $context));
        $this->beforeSaveCacheItem($cacheItem, $node);
        $this->cache->save($cacheItem->set($node));
    }

    final public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        $this->next->deleteNode($nodeRef, $context);
        $this->cache->deleteItem($this->getCacheKey($nodeRef, $context));
    }

    final public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        return $this->next->findNodeRefs($query, $context);
    }

    final public function pipeNodes(SchemaQName $qname, array $context = []): \Generator
    {
        return $this->next->pipeNodes($qname, $context);
    }

    final public function pipeNodeRefs(SchemaQName $qname, array $context = []): \Generator
    {
        return $this->next->pipeNodeRefs($qname, $context);
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
     * @param Message            $node
     */
    protected function beforeSaveCacheItem(CacheItemInterface $cacheItem, Message $node): void
    {
        $cacheItem->expiresAfter(null)->expiresAt(null);
    }
}
