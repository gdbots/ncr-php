<?php

namespace Gdbots\Ncr\Enum;

use Gdbots\Common\Enum;

/**
 * @method static IndexQueryFilterOperator EQUAL_TO()
 * @method static IndexQueryFilterOperator NOT_EQUAL_TO()
 * @method static IndexQueryFilterOperator GREATER_THAN()
 * @method static IndexQueryFilterOperator GREATER_THAN_OR_EQUAL_TO()
 * @method static IndexQueryFilterOperator LESS_THAN()
 * @method static IndexQueryFilterOperator LESS_THAN_OR_EQUAL_TO()
 *
 * // future update
 * method static IndexQueryFilterOperator EXISTS()
 * method static IndexQueryFilterOperator IN()
 * method static IndexQueryFilterOperator CONTAINS()
 * method static IndexQueryFilterOperator NOT_CONTAINS()
 */
final class IndexQueryFilterOperator extends Enum
{
    const EQUAL_TO = 'eq';
    const NOT_EQUAL_TO = 'ne';
    const GREATER_THAN = 'gt';
    const GREATER_THAN_OR_EQUAL_TO = 'gte';
    const LESS_THAN = 'lt';
    const LESS_THAN_OR_EQUAL_TO = 'lte';
    /*
    const EXISTS = 'exists';
    const IN = 'in';
    const CONTAINS = 'contains';
    const NOT_CONTAINS = 'not_contains';
    */
}
