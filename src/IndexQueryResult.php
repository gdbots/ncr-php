<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Common\ToArray;
use Gdbots\Schemas\Ncr\NodeRef;

final class IndexQueryResult implements ToArray, \JsonSerializable, \IteratorAggregate, \Countable
{
    /** @var IndexQuery */
    private $query;

    /** @var NodeRef[] */
    private $nodeRefs;

    /** @var bool */
    private $hasMore = false;

    /** @var string */
    private $nextCursor;

    /**
     * @param IndexQuery  $query
     * @param NodeRef[]   $nodeRefs
     * @param string|null $nextCursor
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

    /**
     * @return bool
     */
    public function hasMore(): bool
    {
        return $this->hasMore;
    }

    /**
     * @return string|null
     */
    public function getNextCursor(): ?string
    {
        return $this->nextCursor;
    }

    /**
     * {@inheritdoc}
     */
    public function toArray()
    {
        return [
            'query'       => $this->query,
            'node_refs'   => $this->nodeRefs,
            'has_more'    => $this->hasMore,
            'next_cursor' => $this->nextCursor,
        ];
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->nodeRefs);
    }

    /**
     * @return int
     */
    public function count()
    {
        return count($this->nodeRefs);
    }
}
