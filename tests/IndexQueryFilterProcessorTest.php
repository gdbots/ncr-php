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
    public function testFilter()
    {
        $nodes = $this->getNodes();

        // eq
        $result = IndexQueryFilterProcessor::filter(
            $nodes,
            [new IndexQueryFilter('string_value', IndexQueryFilterOperator::EQUAL_TO(), 'homer')]
        );
        $this->assertCount(2, $result);
        $this->assertSame('homer', $result[0]->get('string_value'));
        $this->assertSame(500, $result[1]->get('int_value'));

        // gte
        $result = IndexQueryFilterProcessor::filter(
            $nodes,
            [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO(), 300)]
        );
        $this->assertCount(3, $result);
        $this->assertSame(
            [300, 1000, 500],
            [
                $result[0]->get('int_value'),
                $result[1]->get('int_value'),
                $result[2]->get('int_value')
            ]
        );

        // gt
        $result = IndexQueryFilterProcessor::filter(
            $nodes,
            [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 1000)]
        );
        $this->assertEmpty($result);

        // gt && lte
        $result = IndexQueryFilterProcessor::filter(
            $nodes,
            [
                new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 200),
                new IndexQueryFilter('int_value', IndexQueryFilterOperator::LESS_THAN(), 1000),
            ]
        );
        $this->assertCount(2, $result);
        $this->assertSame(300, $result[0]->get('int_value'));

        // eq && neq && gt
        $result = IndexQueryFilterProcessor::filter(
            $nodes,
            [
                new IndexQueryFilter('string_value', IndexQueryFilterOperator::EQUAL_TO(), 'homer'),
                new IndexQueryFilter('int_value', IndexQueryFilterOperator::NOT_EQUAL_TO(), 300),
                new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 100),
            ]
        );
        $this->assertCount(1, $result);
        $this->assertSame(['homer', 500], [$result[0]->get('string_value'), $result[0]->get('int_value')]);
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
            FakeNode::create()->set('string_value', 'homer')->set('int_value', 500),
        ];
    }
}
