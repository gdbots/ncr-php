<?php

namespace Gdbots\Ncr;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Pbj\SchemaQName;

final class IndexQueryBuilder
{
    /** @var SchemaQName */
    private $qname;

    /** @var string */
    private $alias;

    /** @var string */
    private $value;

    /**
     * The number of NodeRefs to fetch in this query.
     *
     * @var int
     */
    private $count = 25;

    /**
     * When paging/scrolling through results, use this value to
     * determine the next set of data to load.  Assumed to be an
     * exclusive value, as in, load data "after" this cursor.
     *
     * @var string
     */
    private $cursor;

    /**
     * When true, the index (if it supports it), will be sorted
     * in ascending order.  The sort key is determined by the
     * index itself.
     *
     * @var bool
     */
    private $sortAsc = true;

    /**
     * An array of filters to use when running this query.
     * The filters are comparisons against fields in the index
     * which may or may not be fields on the actual nodes
     * because an index may derive its own values (composite keys).
     *
     * @var IndexQueryFilter[]
     */
    private $filters = [];

    /**
     * @param SchemaQName $qname
     * @param string      $alias
     * @param string      $value
     */
    public function __construct(SchemaQName $qname, string $alias, string $value)
    {
        $this->qname = $qname;
        $this->alias = $alias;
        $this->value = $value;
    }

    /**
     * @param SchemaQName $qname
     * @param string      $alias
     * @param string      $value
     *
     * @return self
     */
    public static function create(SchemaQName $qname, string $alias, string $value): self
    {
        return new self($qname, $alias, $value);
    }

    /**
     * @param SchemaQName $qname
     *
     * @return self
     */
    public function setQName(SchemaQName $qname): self
    {
        $this->qname = $qname;
        return $this;
    }

    /**
     * @param string $alias
     *
     * @return self
     */
    public function setAlias(string $alias): self
    {
        $this->alias = $alias;
        return $this;
    }

    /**
     * @param string $value
     *
     * @return self
     */
    public function setValue(string $value): self
    {
        $this->value = $value;
        return $this;
    }

    /**
     * @param int $count
     *
     * @return self
     */
    public function setCount(int $count = 25)
    {
        $this->count = $count;
        return $this;
    }

    /**
     * @param string|null $cursor
     *
     * @return self
     */
    public function setCursor(?string $cursor = null): self
    {
        $this->cursor = $cursor;
        return $this;
    }

    /**
     * @param bool $sortAsc
     *
     * @return self
     */
    public function sortAsc(bool $sortAsc = true): self
    {
        $this->sortAsc = $sortAsc;
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterEq(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::EQUAL_TO(), $value);
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterNe(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::NOT_EQUAL_TO(), $value);
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterGt(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::GREATER_THAN(), $value);
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterGte(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO(), $value);
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterLt(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::LESS_THAN(), $value);
        return $this;
    }

    /**
     * @param string $field
     * @param mixed  $value
     *
     * @return self
     */
    public function filterLte(string $field, $value): self
    {
        $this->filters[] = new IndexQueryFilter($field, IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO(), $value);
        return $this;
    }

    /**
     * @return self
     */
    public function clearFilters(): self
    {
        $this->filters = [];
        return $this;
    }

    /**
     * @return IndexQuery
     */
    public function build(): IndexQuery
    {
        return new IndexQuery(
            $this->qname,
            $this->alias,
            $this->value,
            $this->count,
            $this->cursor,
            $this->sortAsc,
            $this->filters
        );
    }
}
