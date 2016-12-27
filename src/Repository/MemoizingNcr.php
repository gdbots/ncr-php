<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Ncr\NcrCache;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

final class MemoizingNcr implements Ncr
{
    /** @var Ncr */
    private $next;

    /** @var NcrCache */
    private $cache;

    /**
     * If true, the NcrCache will be updated when a cache miss occurs.
     * When the Pbjx request bus is in memory, then you'd want this
     * to be false so there aren't two processes updating the NcrCache.
     * One is this memoizer and the other are event listeners which are
     * updating cache after successful get node requests.
     *
     * @var bool
     */
    private $readThrough = false;

    /**
     * @param Ncr      $next
     * @param NcrCache $cache
     * @param bool     $readThrough
     */
    public function __construct(Ncr $next, NcrCache $cache, bool $readThrough = false)
    {
        $this->next = $next;
        $this->cache = $cache;
        $this->readThrough = $readThrough;
    }

    /**
     * {@inheritdoc}
     */
    public function createStorage(SchemaQName $qname, array $hints = [])
    {
        $this->next->createStorage($qname, $hints);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $hints = []): string
    {
        return $this->next->describeStorage($qname, $hints);
    }

    /**
     * {@inheritdoc}
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): bool
    {
        if (!$consistent && $this->cache->hasNode($nodeRef)) {
            return true;
        }

        return $this->next->hasNode($nodeRef, $consistent, $hints);
    }

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): Node
    {
        if (!$consistent && $this->cache->hasNode($nodeRef)) {
            return $this->cache->getNode($nodeRef);
        }

        $node = $this->next->getNode($nodeRef, $consistent, $hints);

        if ($this->readThrough) {
            $this->cache->addNode($node);
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $hints = []): array
    {
        if (empty($nodeRefs)) {
            return [];
        }

        $cachedNodes = [];

        if (!$consistent) {
            /** @var NodeRef[] $nodeRefs */
            foreach ($nodeRefs as $idx => $nodeRef) {
                if ($this->cache->hasNode($nodeRef)) {
                    $cachedNodes[$nodeRef->toString()] = $this->cache->getNode($nodeRef);
                    unset($nodeRefs[$idx]);
                }
            }
        }

        $nodes = empty($nodeRefs) ? [] : $this->next->getNodes($nodeRefs, $consistent, $hints);

        if ($this->readThrough && !empty($nodes)) {
            $this->cache->addNodes($nodes);
        }

        if (!empty($cachedNodes)) {
            $nodes += $cachedNodes;
        }

        return $nodes;
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void
    {
        $this->next->putNode($node, $expectedEtag, $hints);
        $this->cache->addNode($node);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $hints = []): void
    {
        $this->next->deleteNode($nodeRef, $hints);
        $this->cache->removeNode($nodeRef);
    }

    /**
     * {@inheritdoc}
     */
    public function findNodeRefs(IndexQuery $query, array $hints = []): IndexQueryResult
    {
        return $this->next->findNodeRefs($query, $hints);
    }

    /**
     * {@inheritdoc}
     */
    public function streamNodes(SchemaQName $qname, callable $callback, array $hints = []): void
    {
        $this->next->streamNodes($qname, $callback, $hints);
    }

    /**
     * {@inheritdoc}
     */
    public function streamNodeRefs(SchemaQName $qname, callable $callback, array $hints = []): void
    {
        $this->next->streamNodeRefs($qname, $callback, $hints);
    }
}
