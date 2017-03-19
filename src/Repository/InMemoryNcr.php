<?php
declare(strict_types = 1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Common\Util\NumberUtils;
use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\IndexQueryFilterProcessor;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Serializer\PhpArraySerializer;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

/**
 * NCR which runs entirely in memory, typically used for unit tests.
 */
final class InMemoryNcr implements Ncr
{
    /** @var PhpArraySerializer */
    private $serializer;

    /** @var IndexQueryFilterProcessor */
    private $filterProcessor;

    /**
     * Array of nodes keyed by their NodeRef.
     *
     * @var Node[]
     */
    private $nodes = [];

    /**
     * @param Node[]|array $nodes
     */
    public function __construct(array $nodes = [])
    {
        foreach ($nodes as $node) {
            try {
                if (!$node instanceof Node) {
                    $node = $this->createNodeFromArray($node);
                }

                $nodeRef = NodeRef::fromNode($node);
                $this->nodes[$nodeRef->toString()] = $node;
            } catch (\Exception $e) {
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createStorage(SchemaQName $qname, array $context = []): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $count = count($this->nodes);
        $nodeRefs = implode(PHP_EOL, array_keys($this->nodes));
        return <<<TEXT
InMemoryNcr

Count: {$count}
NodeRefs:
{$nodeRefs}

TEXT;
    }

    /**
     * {@inheritdoc}
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        return isset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Node
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
     * {@inheritdoc}
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        $keys = array_map('strval', $nodeRefs);
        $nodes = array_intersect_key($this->nodes, array_flip($keys));

        /** @var Node[] $nodes */
        foreach ($nodes as $nodeRef => $node) {
            if ($node->isFrozen()) {
                $nodes[$nodeRef] = $this->nodes[$nodeRef] = clone $node;
            }
        }

        return $nodes;
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $context = []): void
    {
        $nodeRef = NodeRef::fromNode($node);

        if (null !== $expectedEtag) {
            if (!$this->hasNode($nodeRef)) {
                throw new OptimisticCheckFailed(
                    sprintf('NodeRef [%s] did not have expected etag [%s] (not found).', $nodeRef, $expectedEtag)
                );
            }

            if ($this->nodes[$nodeRef->toString()]->get('etag') !== $expectedEtag) {
                throw new OptimisticCheckFailed(
                    sprintf('NodeRef [%s] did not have expected etag [%s].', $nodeRef, $expectedEtag)
                );
            }
        }

        $this->nodes[$nodeRef->toString()] = $node->freeze();
    }

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        unset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * {@inheritdoc}
     */
    public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        if (null === $this->filterProcessor) {
            $this->filterProcessor = new IndexQueryFilterProcessor();
        }

        /*
         * The InMemoryNcr must treat the index query alias (the primary filter)
         * like all other filters because it has no native way to provide an index.
         */
        $filters = $query->getFilters();
        $filters[] = new IndexQueryFilter($query->getAlias(), IndexQueryFilterOperator::EQUAL_TO(), $query->getValue());
        $nodes = $this->filterProcessor->filter($this->nodes, $filters);

        $nodeRefs = [];
        foreach ($nodes as $nodeRef => $node) {
            $nodeRefs[$nodeRef] = NodeRef::fromString($nodeRef);
        }

        $count = count($nodeRefs);

        if ($query->sortAsc()) {
            ksort($nodeRefs);
        } else {
            krsort($nodeRefs);
        }

        $offset = NumberUtils::bound($query->getCursor(), 0, $count);
        $nodeRefs = array_slice(array_values($nodeRefs), $offset, $query->getCount());
        $nextCursor = $offset + $query->getCount();
        $nextCursor = $nextCursor >= $count ? null : (string)$nextCursor;

        return new IndexQueryResult($query, $nodeRefs, $nextCursor);
    }

    /**
     * {@inheritdoc}
     */
    public function pipeNodes(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        foreach ($this->nodes as $nodeRef => $node) {
            if ($node->isFrozen()) {
                $this->nodes[$nodeRef] = clone $node;
            }

            $receiver($this->nodes[$nodeRef]);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function pipeNodeRefs(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        foreach ($this->nodes as $nodeRef => $node) {
            $receiver(NodeRef::fromString($nodeRef));
        }
    }

    /**
     * @param array $data
     *
     * @return Node
     */
    private function createNodeFromArray(array $data = []): Node
    {
        if (null === $this->serializer) {
            $this->serializer = new PhpArraySerializer();
        }

        /** @var Node $node */
        $node = $this->serializer->deserialize($data);
        return $node;
    }
}
