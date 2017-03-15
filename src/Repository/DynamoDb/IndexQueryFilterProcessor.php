<?php
declare(strict_types = 1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\IndexQueryFilterProcessor;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Psr\Log\LoggerInterface;

final class IndexQueryFilterProcessor extends IndexQueryFilterProcessor
{
    /** @var LoggerInterface */
    private $logger;

    /** @var ItemMarshaler */
    private $marshaler;

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(?LoggerInterface $logger = null): void
    {
        $this->logger = $logger ?: new NullLogger();
    }

    /**
     * @param ItemMarshaler $marshaler
     */
    public function setMarshaler(ItemMarshaler $marshaler): void
    {
        $this->marshaler = new ItemMarshaler();
    }

    /**
     * {@inheritdoc}
     */
    public function filter(array $items, array $filters = []): array
    {
        if (empty($filters)) {
            return [];
        }

        return array_filter($items, function($item) use ($marshaler, $filters) {
            try {
                /** @var Node $node */
                $node = $this->marshaler->unmarshal($item);
            } catch (\Exception $e) {
                $this->logger->error(
                    'Item returned from DynamoDb table could not be unmarshaled.',
                    [
                        'exception' => $e,
                        'item' => $item,
                    ]
                );
            }

            return $this->assertValue($node->toArray(), $filters);
        });
    }
}
