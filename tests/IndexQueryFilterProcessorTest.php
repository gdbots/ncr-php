<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr;

use Assert\InvalidArgumentException;
use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\IndexQueryFilterProcessor;
use Gdbots\Tests\Ncr\Fixtures\FakeNode;

class IndexQueryFilterProcessorTest extends \PHPUnit_Framework_TestCase
{
    public function testFilterEq()
    {
        $result = IndexQueryFilterProcessor::filter(
            $this->getNodes(),
            [new IndexQueryFilter('string_value', IndexQueryFilterOperator::EQUAL_TO(), 'homer')]
        );

        $this->assertCount(1, $result);
    }

    public function testFilterGreaterOrEqualThan()
    {
        $result = IndexQueryFilterProcessor::filter(
            $this->getNodes(),
            [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO(), 300)]
        );

        $this->assertCount(2, $result);
    }

    public function testFilterGreaterThan()
    {
        $result = IndexQueryFilterProcessor::filter(
            $this->getNodes(),
            [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 1000)]
        );

        $this->assertEmpty($result);
    }

    /**
     * @return array
     */
    public function getNodes()
    {
        return [
            FakeNode::create()->set('string_value', 'homer'),
            FakeNode::create()->set('string_value', 'jonny'),
            FakeNode::create()->set('int_value', 200),
            FakeNode::create()->set('int_value', 300),
            FakeNode::create()->set('int_value', 1000),
        ];
    }
}
