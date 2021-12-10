<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Pbj\Message;

/**
 * Provides in memory processing of IndexQueryFilter objects
 * when the underlying repository provider doesn't support them.
 *
 * @internal
 */
class IndexQueryFilterProcessor
{
    /**
     * @param array              $items
     * @param IndexQueryFilter[] $filters
     *
     * @return array
     */
    public function filter(array $items, array $filters): array
    {
        if (empty($filters)) {
            // no filters means return all items
            return $items;
        }

        return array_filter($items, function ($item) use ($filters) {
            foreach ($filters as $filter) {
                $value = $this->extractValue($item, $filter->getField());
                if (!$this->valueMatchesFilter($value, $filter)) {
                    // all filters must pass so any failure means we
                    // can stop checking this item immediately.
                    return false;
                }
            }

            return true;
        });
    }

    protected function extractValue(mixed $item, string $field): mixed
    {
        if ($item instanceof Message && $item->has($field)) {
            return $item->fget($field);
        }

        return null;
    }

    protected function valueMatchesFilter(mixed $value, IndexQueryFilter $filter): bool
    {
        switch ($filter->getOperator()) {
            case IndexQueryFilterOperator::EQUAL_TO:
                return $value == $filter->getValue();

            case IndexQueryFilterOperator::NOT_EQUAL_TO:
                return $value != $filter->getValue();

            case IndexQueryFilterOperator::GREATER_THAN:
                return $value > $filter->getValue();

            case IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO:
                return $value >= $filter->getValue();

            case IndexQueryFilterOperator::LESS_THAN:
                return $value < $filter->getValue();

            case IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO:
                return $value <= $filter->getValue();
        }
    }
}
