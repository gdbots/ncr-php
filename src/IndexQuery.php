<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\NumberUtil;

final class IndexQuery implements \JsonSerializable
{
    private SchemaQName $qname;
    private string $alias;
    private string $value;

    /**
     * The number of NodeRefs to fetch in this query.
     *
     * @var int
     */
    private int $count = 25;

    /**
     * When paging/scrolling through results, use this value to
     * determine the next set of data to load.  Assumed to be an
     * exclusive value, as in, load data "after" this cursor.
     *
     * @var string
     */
    private ?string $cursor = null;

    /**
     * When true, the index (if it supports it), will be sorted
     * in ascending order.  The sort key is determined by the
     * index itself.
     *
     * @var bool
     */
    private bool $sortAsc = true;

    /**
     * An array of filters to use when running this query.
     * The filters are comparisons against fields in the index
     * which may or may not be fields on the actual nodes
     * because an index may derive its own values (composite keys).
     *
     * @var IndexQueryFilter[]
     */
    private array $filters;

    /**
     * An array of fields referenced which are extracted from
     * the filters used on this query.
     *
     * @var string[]
     */
    private array $fields = [];

    /**
     * IndexQuery constructor.
     *
     * @param SchemaQName        $qname
     * @param string             $alias
     * @param string             $value
     * @param int                $count
     * @param string             $cursor
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
        $this->count = NumberUtil::bound($count, 1, 500);
        $this->cursor = $cursor;
        $this->sortAsc = $sortAsc;
        $this->filters = $filters;

        foreach ($this->filters as $filter) {
            $this->fields[$filter->getField()] = true;
        }
    }

    public function getQName(): SchemaQName
    {
        return $this->qname;
    }

    public function getAlias(): string
    {
        return $this->alias;
    }

    public function getValue(): string
    {
        return $this->value;
    }

    public function getCount(): int
    {
        return $this->count;
    }

    public function hasCursor(): bool
    {
        return null !== $this->cursor;
    }

    public function getCursor(): ?string
    {
        return $this->cursor;
    }

    public function sortAsc(): bool
    {
        return $this->sortAsc;
    }

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

    public function getFieldsUsed(): array
    {
        return array_keys($this->fields);
    }

    public function hasFilterForField(string $field): bool
    {
        return isset($this->fields[$field]);
    }

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

    public function toArray(): array
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

    public function jsonSerialize()
    {
        return $this->toArray();
    }
}
