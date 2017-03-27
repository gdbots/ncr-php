<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr;

use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\IndexQueryFilterProcessor;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
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
        $nodes = $this->processor->filter($this->getSimpsonsAsNodes(), $filters);
        $this->assertEquals($expected, $this->getNodeRefs($nodes), "Test filter [{$name}] failed.");
    }

    /**
     * @param Node[] $nodes
     *
     * @return NodeRef[]
     */
    protected function getNodeRefs(array $nodes): array
    {
        $nodeRefs = [];
        foreach ($nodes as $node) {
            $nodeRefs[] = NodeRef::fromNode($node);
        }

        return $nodeRefs;
    }
}
