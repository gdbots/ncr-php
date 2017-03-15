<?php
declare(strict_types = 1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\DynamoDb\Marshaler;
use Gdbots\Ncr\IndexQueryFilterProcessor as BaseIndexQueryFilterProcessor;

final class IndexQueryFilterProcessor extends BaseIndexQueryFilterProcessor
{
    /**
     * {@inheritdoc}
     */
    public function filter(array $items, array $filters = []): array
    {
        if (empty($filters)) {
            return [];
        }

        $marshaler = new Marshaler();

        return array_filter($items, function($item) use ($marshaler, $filters) {
            return $this->assertValue($marshaler->unmarshalItem($item), $filters);
        });
    }
}
