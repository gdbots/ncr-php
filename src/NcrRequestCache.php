<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

/**
 * NcrRequestCache is used to cache all nodes returned from the NCR
 * during a request.  This cache is used during Pbjx request processing
 * or if the NCR is running in the current process and is using the
 * MemoizingNcr.
 *
 * This cache should not be used when asking for a consistent result.
 *
 * NcrRequestCache is NOT an identity map and the NCR is NOT an ORM.
 * In some cases you may get the same exact object but it's not a
 * guarantee so don't do something like this:
 *  $nodeRef = NodeRef::fromString('acme:article:123');
 *  $cache->getNode($nodeRef) !== $cache->getNode($nodeRef);
 *
 * If you need to check equality, use the message interface:
 *
 * $node1 = $cache->getNode($nodeRef);
 * $node2 = $cache->getNode($nodeRef);
 * $node->equals($node2); // returns true if their data is the same
 *
 */
class NcrRequestCache
{
    /**
     * Array of nodes keyed by their NodeRef.
     *
     * @var Node[]
     */
    private $nodes;

    /**
     * The maximum number of items to keep in cache.
     *  0 means unlimited
     *
     * @var int
     */
    private $maxItems = 500;

    /**
     * @param int $maxItems
     */
    public function __construct(int $maxItems = 500)
    {
        $this->maxItems = $maxItems;
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to check for in the NcrRequestCache.
     *
     * @return bool
     */
    final public function hasNode(NodeRef $nodeRef): bool
    {
        return isset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to get from the NcrRequestCache.
     *
     * @return Node
     */
    final public function getNode(NodeRef $nodeRef): Node
    {
        if (!$this->hasNode($nodeRef)) {
            throw NodeNotFound::forNodeRef($nodeRef);
        }

        $node = $this->nodes[$nodeRef->toString()];
        if ($node->isFrozen()) {
            $node = $this->nodes[$nodeRef->toString()] = clone $node;
        }

        return $node;
    }

    /**
     * @param Node $node The Node to put into the NcrRequestCache.
     */
    final public function putNode(Node $node): void
    {
        $this->pruneNodeCache();
        $nodeRef = NodeRef::fromNode($node);
        $this->nodes[$nodeRef->toString()] = $node;
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NcrRequestCache.
     */
    final public function deleteNode(NodeRef $nodeRef): void
    {
        unset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * Clears the NcrRequestCache.
     */
    final public function clear(): void
    {
        $this->nodes = [];
    }

    /**
     * Prunes node cache by removing 20% of the cache if it is full.
     */
    private function pruneNodeCache(): void
    {
        if ($this->maxItems > 0 && count($this->nodes) >= $this->maxItems) {
            $this->nodes = array_slice($this->nodes, $this->maxItems * 0.2);
        }
    }
}
