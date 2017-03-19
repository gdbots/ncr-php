<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr\Fixtures;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\IndexQueryFilter;
use Gdbots\Ncr\Repository\DynamoDb\NodeTable;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\Type as T;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

// methods to create a small ncr of simpson characters for unit testing
trait SimpsonsTrait
{
    /**
     * @return Node[]
     */
    protected function getSimpsonsAsNodes(): array
    {
        $nodes = [
            FakeNode::fromArray(['_id' => 'homer', 'relation' => 'self', 'age' => 39]),
            FakeNode::fromArray(['_id' => 'abraham', 'relation' => 'father', 'age' => 83]),
            FakeNode::fromArray(['_id' => 'marge', 'relation' => 'wife', 'age' => 36]),
            FakeNode::fromArray(['_id' => 'bart', 'relation' => 'son', 'age' => 10, 'is_child' => true]),
            FakeNode::fromArray(['_id' => 'lisa', 'relation' => 'daughter', 'age' => 8, 'is_child' => true]),
            FakeNode::fromArray(['_id' => 'maggie', 'relation' => 'daughter', 'age' => 1, 'is_child' => true]),
            FakeNode::fromArray(['_id' => 'milhouse', 'relation' => 'none', 'age' => 10, 'is_child' => false]),
        ];

        foreach ($nodes as $node) {
            $node->set('title', (string)$node->get('_id'));
        }

        return $nodes;
    }

    /**
     * @return array
     */
    protected function getSimpsonsAsDynamoDbItems(): array
    {
        $items = [];
        $marshaler = new ItemMarshaler();
        $nodeTable = new NodeTable();

        foreach ($this->getSimpsonsAsNodes() as $node) {
            $item = $marshaler->marshal($node);
            $item[NodeTable::HASH_KEY_NAME] = ['S' => NodeRef::fromNode($node)];
            $nodeTable->beforePutItem($item, $node);
            $items[] = $item;
        }

        return $items;
    }

    /**
     * @return array
     */
    public function getSimpsonsIndexQueryFilterTests(): array
    {
        return [
            [
                'name'     => 'return just homer',
                'filters'  => [
                    new IndexQueryFilter('title', IndexQueryFilterOperator::EQUAL_TO(), 'homer'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:homer'),
                ],
            ],

            [
                'name'     => 'return homer\'s kids',
                'filters'  => [
                    new IndexQueryFilter('is_child', IndexQueryFilterOperator::EQUAL_TO(), true),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:bart'),
                    NodeRef::fromString('gdbots:fake-node:lisa'),
                    NodeRef::fromString('gdbots:fake-node:maggie'),
                ],
            ],

            [
                'name'     => 'return homer\'s wife',
                'filters'  => [
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::EQUAL_TO(), 'wife'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:marge'),
                ],
            ],

            [
                'name'     => 'return homer\'s daughters',
                'filters'  => [
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::EQUAL_TO(), 'daughter'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:lisa'),
                    NodeRef::fromString('gdbots:fake-node:maggie'),
                ],
            ],

            [
                'name'     => 'return simpsons younger than homer',
                'filters'  => [
                    new IndexQueryFilter('status', IndexQueryFilterOperator::EQUAL_TO(), 'draft'),
                    new IndexQueryFilter('age', IndexQueryFilterOperator::LESS_THAN(), 39),
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::NOT_EQUAL_TO(), 'none'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:marge'),
                    NodeRef::fromString('gdbots:fake-node:bart'),
                    NodeRef::fromString('gdbots:fake-node:lisa'),
                    NodeRef::fromString('gdbots:fake-node:maggie'),
                ],
            ],

            [
                'name'     => 'return everyone younger than homer',
                'filters'  => [
                    new IndexQueryFilter('status', IndexQueryFilterOperator::EQUAL_TO(), 'draft'),
                    new IndexQueryFilter('age', IndexQueryFilterOperator::LESS_THAN(), 39),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:marge'),
                    NodeRef::fromString('gdbots:fake-node:bart'),
                    NodeRef::fromString('gdbots:fake-node:lisa'),
                    NodeRef::fromString('gdbots:fake-node:maggie'),
                    NodeRef::fromString('gdbots:fake-node:milhouse'),
                ],
            ],

            [
                'name'     => 'return simpsons younger or as old as than homer',
                'filters'  => [
                    new IndexQueryFilter('status', IndexQueryFilterOperator::EQUAL_TO(), 'draft'),
                    new IndexQueryFilter('age', IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO(), 39),
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::NOT_EQUAL_TO(), 'none'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:homer'),
                    NodeRef::fromString('gdbots:fake-node:marge'),
                    NodeRef::fromString('gdbots:fake-node:bart'),
                    NodeRef::fromString('gdbots:fake-node:lisa'),
                    NodeRef::fromString('gdbots:fake-node:maggie'),
                ],
            ],

            [
                'name'     => 'return simpsons older than homer',
                'filters'  => [
                    new IndexQueryFilter('status', IndexQueryFilterOperator::EQUAL_TO(), 'draft'),
                    new IndexQueryFilter('age', IndexQueryFilterOperator::GREATER_THAN(), 39),
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::NOT_EQUAL_TO(), 'none'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:abraham'),
                ],
            ],

            [
                'name'     => 'return simpsons older or as old as than homer',
                'filters'  => [
                    new IndexQueryFilter('status', IndexQueryFilterOperator::EQUAL_TO(), 'draft'),
                    new IndexQueryFilter('age', IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO(), 39),
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::NOT_EQUAL_TO(), 'none'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:homer'),
                    NodeRef::fromString('gdbots:fake-node:abraham'),
                ],
            ],

            [
                'name'     => 'return homer\'s son',
                'filters'  => [
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::EQUAL_TO(), 'son'),
                    new IndexQueryFilter('is_child', IndexQueryFilterOperator::EQUAL_TO(), true),
                    new IndexQueryFilter('relation', IndexQueryFilterOperator::NOT_EQUAL_TO(), 'none'),
                ],
                'expected' => [
                    NodeRef::fromString('gdbots:fake-node:bart'),
                ],
            ],
        ];
    }
}
