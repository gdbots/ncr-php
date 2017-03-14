<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Assert\InvalidArgumentException;
use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Pbj\Assertion;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;

final class IndexQueryFilterProcessor
{
    /**
     * @param Node[]             $nodes
     * @param IndexQueryFilter[] $filters
     *
     * @return Node[]
     */
    public static function filter(array $nodes, array $filters): array
    {
        $nodes = array_filter($nodes, function($node) use ($filters) {
            $check = 0;

            foreach ($filters as $filter) {
                if ($node->has($filter->getField())) {
                    $value = $node->get($filter->getField());
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
        });

        return array_values($nodes);
    }
}
