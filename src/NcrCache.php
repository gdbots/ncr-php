<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\GetNodeBatchRequest\GetNodeBatchRequest;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;

/**
 * NcrCache is used to cache all nodes returned from get node request(s).
 * This cache is used during Pbjx request processing or if the NCR is
 * running in the current process and is using the MemoizingNcr.
 *
 * This cache should not be used when asking for a consistent result.
 *
 * NcrCache is NOT an identity map and the NCR is NOT an ORM. In some
 * cases you may get the same exact object but it's not a guarantee so
 * don't do something like this:
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
final class NcrCache
{
    /** @var Pbjx */
    private $pbjx;

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
     * When loadNodesDeferred method is used a request is created
     * and populated with the node refs.  Only when a cache miss
     * occurs on getNode will this request be processed.
     *
     * @var GetNodeBatchRequest
     */
    private $deferredRequest;

    /**
     * @param Pbjx $pbjx
     * @param int  $maxItems
     */
    public function __construct(Pbjx $pbjx, int $maxItems = 500)
    {
        $this->pbjx = $pbjx;
        $this->maxItems = $maxItems;
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to check for in the NcrCache.
     *
     * @return bool
     */
    public function hasNode(NodeRef $nodeRef): bool
    {
        return isset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to get from the NcrCache.
     *
     * @return Node
     */
    public function getNode(NodeRef $nodeRef): Node
    {
        if (!$this->hasNode($nodeRef)) {
            $this->processDeferredRequest();
            if (!$this->hasNode($nodeRef)) {
                throw NodeNotFound::forNodeRef($nodeRef);
            }
        }

        $node = $this->nodes[$nodeRef->toString()];
        if ($node->isFrozen()) {
            $node = $this->nodes[$nodeRef->toString()] = clone $node;
        }

        return $node;
    }

    /**
     * @param Node $node The Node to put into the NcrCache.
     */
    public function putNode(Node $node): void
    {
        $this->pruneNodeCache();
        $nodeRef = NodeRef::fromNode($node);
        $this->nodes[$nodeRef->toString()] = $node;
        $this->removeNodesDeferred([$nodeRef]);
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NcrCache.
     */
    public function deleteNode(NodeRef $nodeRef): void
    {
        unset($this->nodes[$nodeRef->toString()]);
        $this->removeNodesDeferred([$nodeRef]);
    }

    /**
     * Clears the NcrCache.
     */
    public function clear(): void
    {
        $this->nodes = [];
        $this->deferredRequest = null;
    }

    /**
     * Adds an array of NodeRefs that should be loaded at some point later
     * ONLY if there is a request for a NodeRef that is not already
     * available in cache.
     *
     * @param NodeRef[] $nodeRefs
     * @param Message   $causator
     * @param array     $hints
     */
    public function loadNodesDeferred(array $nodeRefs, Message $causator, array $hints = []): void
    {
        if (null === $this->deferredRequest) {
            $this->deferredRequest = GetNodeBatchRequestV1::create();
        }

        $this->deferredRequest->addToSet('node_refs', $nodeRefs);
        $this->pbjx->copyContext($causator, $this->deferredRequest);

        foreach ($hints as $k => $v) {
            $this->deferredRequest->addToMap('hints', (string)$k, (string)$v);
        }
    }

    /**
     * Removes an array of NodeRefs from the deferred request.
     *
     * @param NodeRef[] $nodeRefs
     */
    private function removeNodesDeferred(array $nodeRefs): void
    {
        if (null === $this->deferredRequest) {
            return;
        }

        $this->deferredRequest->removeFromSet('node_refs', $nodeRefs);
    }

    /**
     * Processes the deferrered request which should populate the
     * NcrCache once complete.  At least for any nodes that exist.
     *
     * todo: split up requests with > 100 node_refs?
     */
    private function processDeferredRequest()
    {
        if (null === $this->deferredRequest) {
            return;
        }

        if (!$this->deferredRequest->has('node_refs')) {
            return;
        }

        try {
            $this->pbjx->request($this->deferredRequest);
        } catch (\Exception $e) {
        }

        $this->deferredRequest = null;
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
