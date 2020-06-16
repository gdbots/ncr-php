<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Ncr\NcrCache;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;

final class MemoizingNcr implements Ncr
{
    private Ncr $next;
    private NcrCache $cache;

    /**
     * If true, the NcrCache will be updated when a cache miss occurs.
     * When the Pbjx request bus is in memory, then you'd want this
     * to be false so there aren't two processes updating the NcrCache.
     * One is this memoizer and the other are event listeners which are
     * updating cache after successful get node requests.
     *
     * @var bool
     */
    private bool $readThrough = false;

    public function __construct(Ncr $next, NcrCache $cache, bool $readThrough = false)
    {
        $this->next = $next;
        $this->cache = $cache;
        $this->readThrough = $readThrough;
    }

    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $this->next->createStorage($qname, $context);
    }

    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        return $this->next->describeStorage($qname, $context);
    }

    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        if (!$consistent && $this->cache->hasNode($nodeRef)) {
            return true;
        }

        return $this->next->hasNode($nodeRef, $consistent, $context);
    }

    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Message
    {
        if (!$consistent && $this->cache->hasNode($nodeRef)) {
            try {
                return $this->cache->getNode($nodeRef);
            } catch (\Throwable $e) {
            }
        }

        $node = $this->next->getNode($nodeRef, $consistent, $context);

        if ($this->readThrough) {
            $this->cache->addNode($node);
        }

        return $node;
    }

    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        if (empty($nodeRefs)) {
            return [];
        }

        $cachedNodes = [];

        if (!$consistent) {
            /** @var NodeRef[] $nodeRefs */
            foreach ($nodeRefs as $idx => $nodeRef) {
                if ($this->cache->hasNode($nodeRef)) {
                    try {
                        $cachedNodes[$nodeRef->toString()] = $this->cache->getNode($nodeRef);
                        unset($nodeRefs[$idx]);
                    } catch (\Throwable $e) {
                    }
                }
            }
        }

        $nodes = empty($nodeRefs) ? [] : $this->next->getNodes($nodeRefs, $consistent, $context);

        if ($this->readThrough && !empty($nodes)) {
            $this->cache->addNodes($nodes);
        }

        if (!empty($cachedNodes)) {
            $nodes += $cachedNodes;
        }

        return $nodes;
    }

    public function putNode(Message $node, ?string $expectedEtag = null, array $context = []): void
    {
        $this->next->putNode($node, $expectedEtag, $context);
        $this->cache->addNode($node);
    }

    public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        $this->next->deleteNode($nodeRef, $context);
        $this->cache->removeNode($nodeRef);
    }

    public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        return $this->next->findNodeRefs($query, $context);
    }

    public function pipeNodes(SchemaQName $qname, array $context = []): \Generator
    {
        return $this->next->pipeNodes($qname, $context);
    }

    public function pipeNodeRefs(SchemaQName $qname, array $context = []): \Generator
    {
        return $this->next->pipeNodeRefs($qname, $context);
    }
}
