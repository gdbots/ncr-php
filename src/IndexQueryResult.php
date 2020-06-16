<?php
declare(strict_types=1);

namespace Gdbots\Ncr;


use Gdbots\Pbj\WellKnown\NodeRef;

final class IndexQueryResult implements \JsonSerializable, \IteratorAggregate, \Countable
{
    private IndexQuery $query;

    /** @var NodeRef[] */
    private array $nodeRefs;
    private bool $hasMore;
    private ?string $nextCursor;

    /**
     * @param IndexQuery $query
     * @param NodeRef[]  $nodeRefs
     * @param string     $nextCursor
     */
    public function __construct(IndexQuery $query, array $nodeRefs = [], ?string $nextCursor = null)
    {
        $this->query = $query;
        $this->nodeRefs = $nodeRefs;
        $this->hasMore = !empty($nextCursor);
        $this->nextCursor = $nextCursor;
    }

    /**
     * @return NodeRef[]
     */
    public function getNodeRefs(): array
    {
        return $this->nodeRefs;
    }

    public function hasMore(): bool
    {
        return $this->hasMore;
    }

    public function getNextCursor(): ?string
    {
        return $this->nextCursor;
    }

    public function toArray(): array
    {
        return [
            'query'       => $this->query,
            'node_refs'   => $this->nodeRefs,
            'has_more'    => $this->hasMore,
            'next_cursor' => $this->nextCursor,
        ];
    }

    public function jsonSerialize()
    {
        return $this->toArray();
    }

    public function getIterator()
    {
        return new \ArrayIterator($this->nodeRefs);
    }

    public function count()
    {
        return count($this->nodeRefs);
    }
}
