<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\IndexQueryFilterProcessor;
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
    public function testFilters(string $name, array $filters, array $expected): void
    {
        $nodes = $this->processor->filter($this->getSimpsonsAsNodes(), $filters);
        $this->assertEquals($expected, $this->getNodeRefs($nodes), "Test filter [{$name}] failed.");
    }

    /**
     * @param Message[] $nodes
     *
     * @return NodeRef[]
     */
    protected function getNodeRefs(array $nodes): array
    {
        return array_map(fn (Message $node) => $node->generateNodeRef(), $nodes);
    }
}
