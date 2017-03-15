<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Assert\InvalidArgumentException;
use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Pbj\Assertion;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;

class IndexQueryFilterProcessor
{
    /**
     * @param Node[]             $items
     * @param IndexQueryFilter[] $filters
     *
     * @return array
     */
    public function filter(array $items, array $filters = []): array
    {
        if (empty($filters)) {
            return [];
        }

        return array_filter($items, function($item) use ($filters) {
            return $this->assertValue($item->toArray(), $filters);
        });
    }

    /**
     * @param array              $item
     * @param IndexQueryFilter[] $filters
     *
     * @return bool
     */
    protected function assertValue(array $item, array $filters = []): bool
    {
        if (empty($filters)) {
            return false;
        }

        $check = 0;

        foreach ($filters as $filter) {
            if (isset($item[$filter->getField()])) {
                $value = $item[$filter->getField()];
                $value2 = $filter->getValue();

                try {
                    switch ($filter->getOperator()) {
                        case IndexQueryFilterOperator::EQUAL_TO:
                            Assertion::eq($value, $value2);
                            $check++;
                            break;

                        case IndexQueryFilterOperator::NOT_EQUAL_TO:
                            Assertion::notEq($value, $value2);
                            $check++;
                            break;

                        case IndexQueryFilterOperator::GREATER_THAN:
                            Assertion::greaterThan($value, $value2);
                            $check++;
                            break;

                        case IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO:
                            Assertion::greaterOrEqualThan($value, $value2);
                            $check++;
                            break;

                        case IndexQueryFilterOperator::LESS_THAN:
                            Assertion::lessThan($value, $value2);
                            $check++;
                            break;

                        case IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO:
                            Assertion::lessOrEqualThan($value, $value2);
                            $check++;
                            break;
                    }
                } catch (InvalidArgumentException $e) {
                    // do nothing
                }
            }
        }

        return $check === count($filters);
    }
}
