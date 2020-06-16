<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\IndexQueryFilterProcessor;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Serializer\PhpArraySerializer;
use Gdbots\Pbj\Util\NumberUtil;
use Gdbots\Pbj\WellKnown\NodeRef;

/**
 * NCR which runs entirely in memory, typically used for unit tests.
 */
final class InMemoryNcr implements Ncr
{
    private ?PhpArraySerializer $serializer = null;
    private ?IndexQueryFilterProcessor $filterProcessor = null;

    /**
     * Array of nodes keyed by their NodeRef.
     *
     * @var Message[]
     */
    private array $nodes = [];

    /**
     * @param Message[]|array $nodes
     */
    public function __construct(array $nodes = [])
    {
        foreach ($nodes as $node) {
            try {
                if (!$node instanceof Message) {
                    $node = $this->createNodeFromArray($node);
                }

                $nodeRef = NodeRef::fromNode($node);
                $this->nodes[$nodeRef->toString()] = $node;
            } catch (\Throwable $e) {
            }
        }
    }

    public function createStorage(SchemaQName $qname, array $context = []): void
    {
    }

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

    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        return isset($this->nodes[$nodeRef->toString()]);
    }

    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Message
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

    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        $keys = array_map('strval', $nodeRefs);
        $nodes = array_intersect_key($this->nodes, array_flip($keys));

        foreach ($nodes as $nodeRef => $node) {
            if ($node->isFrozen()) {
                $nodes[$nodeRef] = $this->nodes[$nodeRef] = clone $node;
            }
        }

        return $nodes;
    }

    public function putNode(Message $node, ?string $expectedEtag = null, array $context = []): void
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

    public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        unset($this->nodes[$nodeRef->toString()]);
    }

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

        $offset = NumberUtil::bound((int)$query->getCursor(), 0, $count);
        $nodeRefs = array_slice(array_values($nodeRefs), $offset, $query->getCount());
        $nextCursor = $offset + $query->getCount();
        $nextCursor = $nextCursor >= $count ? null : (string)$nextCursor;

        return new IndexQueryResult($query, $nodeRefs, $nextCursor);
    }

    public function pipeNodes(SchemaQName $qname, array $context = []): \Generator
    {
        foreach ($this->nodes as $nodeRef => $node) {
            if ($node->isFrozen()) {
                $this->nodes[$nodeRef] = clone $node;
            }

            if ($node::schema()->getQName() !== $qname) {
                continue;
            }

            yield $this->nodes[$nodeRef];
        }
    }

    public function pipeNodeRefs(SchemaQName $qname, array $context = []): \Generator
    {
        foreach ($this->nodes as $nodeRef => $node) {
            if ($node::schema()->getQName() !== $qname) {
                continue;
            }

            yield NodeRef::fromString($nodeRef);
        }
    }

    private function createNodeFromArray(array $data = []): Message
    {
        if (null === $this->serializer) {
            $this->serializer = new PhpArraySerializer();
        }

        return $this->serializer->deserialize($data);
    }
}
