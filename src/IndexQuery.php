<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\ToArray;
use Gdbots\Common\Util\NumberUtils;
use Gdbots\Pbj\SchemaQName;

final class IndexQuery implements ToArray, \JsonSerializable
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
    private $filters;

    /**
     * An array of fields referenced which are extracted from
     * the filters used on this query.
     *
     * @var string
     */
    private $fields = [];

    /**
     * @param SchemaQName        $qname
     * @param string             $alias
     * @param string             $value
     * @param int                $count
     * @param string|null        $cursor
     * @param bool               $sortAsc
     * @param IndexQueryFilter[] $filters
     */
    public function __construct(
        SchemaQName $qname,
        string $alias,
        string $value,
        int $count = 25,
        ?string $cursor = null,
        bool $sortAsc = true,
        array $filters = []
    ) {
        $this->qname = $qname;
        $this->alias = $alias;
        $this->value = $value;
        $this->count = NumberUtils::bound($count, 1, 500);
        $this->cursor = $cursor;
        $this->sortAsc = $sortAsc;
        $this->filters = $filters;

        foreach ($this->filters as $filter) {
            $this->fields[$filter->getField()] = true;
        }
    }

    /**
     * @return SchemaQName
     */
    public function getQName(): SchemaQName
    {
        return $this->qname;
    }

    /**
     * @return string
     */
    public function getAlias(): string
    {
        return $this->alias;
    }

    /**
     * @return string
     */
    public function getValue(): string
    {
        return $this->value;
    }

    /**
     * @return int
     */
    public function getCount(): int
    {
        return $this->count;
    }

    /**
     * @return bool
     */
    public function hasCursor(): bool
    {
        return null !== $this->cursor;
    }

    /**
     * @return string
     */
    public function getCursor(): ?string
    {
        return $this->cursor;
    }

    /**
     * @return bool
     */
    public function sortAsc(): bool
    {
        return $this->sortAsc;
    }

    /**
     * @return bool
     */
    public function hasFilters(): bool
    {
        return empty($this->filters);
    }

    /**
     * @return IndexQueryFilter[]
     */
    public function getFilters(): array
    {
        return $this->filters;
    }

    /**
     * @return string[]
     */
    public function getFieldsUsed(): array
    {
        return array_keys($this->fields);
    }

    /**
     * Return true if the provided field is used in a filter.
     *
     * @param string $field
     *
     * @return bool
     */
    public function hasFilterForField(string $field): bool
    {
        return isset($this->fields[$field]);
    }

    /**
     * @param string $field
     *
     * @return array
     */
    public function getFiltersForField(string $field): array
    {
        $filters = [];
        foreach ($this->filters as $filter) {
            if ($field === $filter->getField()) {
                $filters[] = $filter;
            }
        }

        return $filters;
    }

    /**
     * {@inheritdoc}
     */
    public function toArray()
    {
        return [
            'qname'    => $this->qname->toString(),
            'alias'    => $this->alias,
            'value'    => $this->value,
            'count'    => $this->count,
            'cursor'   => $this->cursor,
            'sort_asc' => $this->sortAsc,
            'filters'  => $this->filters,
        ];
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }
}
