<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\Repository\DynamoDb\IndexQueryFilterProcessor;
use Gdbots\Ncr\Repository\DynamoDb\NodeTable;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Tests\Ncr\Fixtures\SimpsonsTrait;

class IndexQueryFilterProcessorTest extends \PHPUnit_Framework_TestCase
{
    use SimpsonsTrait;

    /** @var IndexQueryFilterProcessor */
    protected $processor;

    public function setUp()
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
        $nodeRefs = [];
        foreach ($items as $item) {
            $nodeRefs[] = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
        }

        return $nodeRefs;
    }
}
