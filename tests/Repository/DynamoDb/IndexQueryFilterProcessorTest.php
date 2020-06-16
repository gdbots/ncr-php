<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\Repository\DynamoDb\IndexQueryFilterProcessor;
use Gdbots\Ncr\Repository\DynamoDb\NodeTable;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Tests\Ncr\Fixtures\SimpsonsTrait;
use PHPUnit\Framework\TestCase;

class IndexQueryFilterProcessorTest extends TestCase
{
    use SimpsonsTrait;

    protected IndexQueryFilterProcessor $processor;

    public function setUp(): void
    {
        $this->processor = new IndexQueryFilterProcessor();
    }

    /**
     * @dataProvider getSimpsonsIndexQueryFilterTests
     *
     * @param string             $name
     * @param IndexQueryFilter[] $filters
     * @param NodeRef[]          $expected
     */
    public function testFilters(string $name, array $filters, array $expected)
    {
        $items = $this->processor->filter($this->getSimpsonsAsDynamoDbItems(), $filters);
        $this->assertEquals($expected, $this->getNodeRefs($items), "Test filter [{$name}] failed.");
    }

    /**
     * @param array $items
     *
     * @return NodeRef[]
     */
    protected function getNodeRefs(array $items): array
    {
        return array_map(fn(array $item) => NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']), $items);
    }
}
