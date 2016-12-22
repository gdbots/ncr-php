<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Psr\Cache\CacheItemPoolInterface;

// fixme: implement PsrCacheNcr
final class PsrCacheNcr implements Ncr
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
    private $readThrough = false;

    /**
     * @param Ncr      $next
     * @param CacheItemPoolInterface $cache
     * @param bool     $readThrough
     */
    public function __construct(Ncr $next, CacheItemPoolInterface $cache, bool $readThrough = false)
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
            $this->cache->putNode($node);
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

        $cached = [];

        if (!$consistent) {
            /** @var NodeRef[] $nodeRefs */
            foreach ($nodeRefs as $idx => $nodeRef) {
                if ($this->cache->hasNode($nodeRef)) {
                    $cached[$nodeRef->toString()] = $this->cache->getNode($nodeRef);
                    unset($nodeRefs[$idx]);
                }
            }
        }

        $nodes = empty($nodeRefs) ? [] : $this->next->getNodes($nodeRefs, $consistent, $hints);

        if ($this->readThrough && !empty($nodes)) {
            $this->cache->putNodes($nodes);
        }

        if (!empty($cached)) {
            $nodes += $cached;
        }

        return $nodes;
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void
    {
        $this->next->putNode($node, $expectedEtag, $hints);
        $this->cache->putNode($node);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $hints = []): void
    {
        $this->next->deleteNode($nodeRef, $hints);
        $this->cache->deleteNode($nodeRef);
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
