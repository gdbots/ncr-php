<?php
declare(strict_types = 1);

namespace Gdbots\Tests\Ncr\Repository;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\Repository\DynamoDb\IndexQueryFilterProcessor;
use Gdbots\Tests\Ncr\Fixtures\FakeNode;

class IndexQueryFilterProcessorTest extends \PHPUnit_Framework_TestCase
{
    /** @var IndexQueryFilterProcessor */
    protected $processor;

    public function setUp()
    {
        $this->processor = new IndexQueryFilterProcessor();
    }

    public function testFilter()
    {
        $items = $this->getItems();

        // eq
        $result = array_values(
            $this->processor->filter(
                $items,
                [new IndexQueryFilter('string_value', IndexQueryFilterOperator::EQUAL_TO(), 'homer')]
            )
        );
        $this->assertCount(2, $result);
        $this->assertSame('homer', $result[0]['string_value']['S']);
        $this->assertSame('500', $result[1]['int_value']['N']);

        // gte
        $result = array_values(
            $this->processor->filter(
                $items,
                [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO(), 300)]
            )
        );
        $this->assertCount(3, $result);
        $this->assertSame(
            ['300', '1000', '500'],
            [
                $result[0]['int_value']['N'],
                $result[1]['int_value']['N'],
                $result[2]['int_value']['N']
            ]
        );

        // gt
        $result = array_values(
            $this->processor->filter(
                $items,
                [new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 1000)]
            )
        );
        $this->assertEmpty($result);

        // gt && lte
        $result = array_values(
            $this->processor->filter(
                $items,
                [
                    new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 200),
                    new IndexQueryFilter('int_value', IndexQueryFilterOperator::LESS_THAN(), 1000),
                ]
            )
        );
        $this->assertCount(2, $result);
        $this->assertSame('300', $result[0]['int_value']['N']);

        // eq && neq && gt
        $result = array_values(
            $this->processor->filter(
                $items,
                [
                    new IndexQueryFilter('string_value', IndexQueryFilterOperator::EQUAL_TO(), 'homer'),
                    new IndexQueryFilter('int_value', IndexQueryFilterOperator::NOT_EQUAL_TO(), 300),
                    new IndexQueryFilter('int_value', IndexQueryFilterOperator::GREATER_THAN(), 100),
                ]
            )
        );
        $this->assertCount(1, $result);
        $this->assertSame(['homer', '500'], [$result[0]['string_value']['S'], $result[0]['int_value']['N']]);
    }

    /**
     * @return array
     */
    public function getItems()
    {
        return [
            ['string_value' => ['S' => 'homer']],
            ['string_value' => ['S' => 'jonny']],
            ['int_value' => ['N' => '200']],
            ['int_value' => ['N' => '300']],
            ['int_value' => ['N' => '1000']],
            [
                'string_value' => ['S' => 'homer'],
                'int_value' => ['N' => '500'],
            ],
        ];
    }
}
