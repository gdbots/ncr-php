<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

trait MemoizingNcrTrait
{
    /**
     * Array of nodes keyed by their NodeRef that have been fetched
     * in the given request.  This is NOT an identity map and the
     * NCR is NOT an ORM.  This is designed to eliminate a network
     * request to fetch a node if it's already been loaded.
     *
     * This local node cache should not be used when the request
     * is asking for a consistent result.
     *
     * All nodes in cache are frozen and fetching nodes from the
     * cache gives you a clone of the object so later there is
     * no dirty checking/merging/etc.  This means that:
     *
     * $nodeRef = NodeRef::fromString('acme:article:123');
     * $ncr->getNode($nodeRef) !== $ncr->getNode($nodeRef);
     *
     * But, if you need to check equality, use the message interface:
     *
     * $node1 = $ncr->getNode($nodeRef);
     * $node2 = $ncr->getNode($nodeRef);
     * $node->equals($node2); // returns true if their data is the same
     *
     * @var Node[]
     */
    private $nodeCache;

    /**
     * The maximum number of nodes to keep in nodeCache.
     *
     * @var int
     */
    private $maxNodeCacheItems = 500;

    /**
     * When new items are added to cache the cache will be
     * pruned if this is enabled.
     *
     * @var bool
     */
    private $enableCachePruning = true;

    /**
     * @param NodeRef $nodeRef
     *
     * @return bool
     */
    private function isInNodeCache(NodeRef $nodeRef): bool
    {
        return isset($this->nodeCache[$nodeRef->toString()]);
    }

    /**
     * Returns a clone of the node stored in local node cache.
     *
     * @param NodeRef $nodeRef
     *
     * @return Node
     */
    private function getFromNodeCache(NodeRef $nodeRef): Node
    {
        $node = $this->nodeCache[$nodeRef->toString()];
        return $node->isFrozen() ? clone $node : $node;
    }

    /**
     * @param NodeRef $nodeRef
     * @param Node    $node
     * @param bool    $pruneCache
     */
    private function addToNodeCache(NodeRef $nodeRef, Node $node, bool $pruneCache = true): void
    {
        if ($this->enableCachePruning && $pruneCache) {
            $this->pruneNodeCache();
        }

        $this->nodeCache[$nodeRef->toString()] = $node;
    }

    /**
     * @param NodeRef $nodeRef
     */
    private function removeFromNodeCache(NodeRef $nodeRef): void
    {
        unset($this->nodeCache[$nodeRef->toString()]);
    }

    /**
     * Prunes node cache by removing 20% of the cache if it is full.
     */
    private function pruneNodeCache(): void
    {
        if (count($this->nodeCache) >= $this->maxNodeCacheItems) {
            $this->nodeCache = array_slice($this->nodeCache, $this->maxNodeCacheItems * 0.2);
        }
    }
}
