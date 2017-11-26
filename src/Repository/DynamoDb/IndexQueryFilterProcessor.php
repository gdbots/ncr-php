<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\IndexQueryFilterProcessor as BaseIndexQueryFilterProcessor;

final class IndexQueryFilterProcessor extends BaseIndexQueryFilterProcessor
{
    /**
     * {@inheritdoc}
     */
    protected function extractValue($item, string $field)
    {
        if (!is_array($item) || !isset($item[$field])) {
            return null;
        }

        return current($item[$field]);
    }
}
