<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\Exception\IndexQueryNotSupported;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;

abstract class AbstractIndex implements GlobalSecondaryIndex
{
    /**
     * {@inheritdoc}
     */
    public function getName(): string
    {
        return "{$this->getAlias()}_index";
    }

    /**
     * {@inheritdoc}
     */
    public function getRangeKeyName(): ?string
    {
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function getFilterableAttributes(): array
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function getProjection(): array
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function beforePutItem(array &$item, Node $node): void
    {
    }

    /**
     * {@inheritdoc}
     */
    final public function createQuery(IndexQuery $query): array
    {
        $filterables = $this->getFilterableAttributes();
        $params = [
            'IndexName'                 => $this->getName(),
            'ScanIndexForward'          => $query->sortAsc(),
            'Limit'                     => $query->getCount(),
            'ExpressionAttributeNames'  => [
                '#hash'     => $this->getHashKeyName(),
                '#node_ref' => NodeTable::HASH_KEY_NAME,
            ],
            'ExpressionAttributeValues' => [
                ':v_hash'  => ['S' => $query->getValue()],
                ':v_qname' => ['S' => $query->getQName()->toString()],
            ],
            // special key to deal with filters that must be
            // processed AFTER the query runs due to limitation of range key
            // only allowing for one expression.
            'unprocessed_filters'       => [],
        ];

        $keyConditionExpressions = ['#hash = :v_hash'];
        $filterExpressions = ['contains(#node_ref, :v_qname)'];
        $rangeKeyName = $this->getRangeKeyName();
        $usedRange = false;
        $i = 0;

        foreach ($query->getFilters() as $filter) {
            $i++;
            $fieldName = $filter->getField();

            if (!isset($filterables[$fieldName])) {
                throw new IndexQueryNotSupported(
                    sprintf(
                        'Field [%s] is not supported on index [%s].  Supported fields: %s',
                        $fieldName,
                        $this->getAlias(),
                        implode(', ', array_keys($filterables))
                    )
                );
            }

            $filterable = $filterables[$fieldName];

            $ean = "#n_{$filterable['AttributeName']}";
            $eav = ":v_{$filterable['AttributeName']}_{$i}";

            $params['ExpressionAttributeNames'][$ean] = $filterable['AttributeName'];
            $params['ExpressionAttributeValues'][$eav] = [
                $filterable['AttributeType'] => $this->marshalValue($filterable['AttributeType'], $filter->getValue()),
            ];

            $operator = $this->getDynamoDbOperator($filter->getOperator());

            if ($rangeKeyName === $fieldName) {
                if ($usedRange) {
                    $params['unprocessed_filters'][] = $filter;
                    unset($params['ExpressionAttributeValues'][$eav]);
                    continue;
                }

                $keyConditionExpressions[] = "$ean $operator $eav";
                $usedRange = true;
                continue;
            }

            $filterExpressions[] = "$ean $operator $eav";
        }

        if ($query->hasCursor()) {
            $params['ExclusiveStartKey'] = json_decode(base64_decode($query->getCursor()), true);
        }

        $params['KeyConditionExpression'] = implode(' AND ', $keyConditionExpressions);
        $params['FilterExpression'] = implode(' AND ', $filterExpressions);

        if (empty($params['FilterExpression'])) {
            unset($params['FilterExpression']);
        }

        if (empty($params['unprocessed_filters'])) {
            unset($params['unprocessed_filters']);
        }

        return $params;
    }

    /**
     * @param string $attributeType
     * @param mixed  $value
     *
     * @return mixed
     */
    private function marshalValue(string $attributeType, $value)
    {
        switch ($attributeType) {
            case 'BOOL':
                return filter_var($value, FILTER_VALIDATE_BOOLEAN);

            default:
                return (string)$value;
        }
    }

    /**
     * @param IndexQueryFilterOperator $operator
     *
     * @return string
     */
    private function getDynamoDbOperator(IndexQueryFilterOperator $operator): string
    {
        switch ($operator->getValue()) {
            case IndexQueryFilterOperator::EQUAL_TO:
                return '=';

            case IndexQueryFilterOperator::NOT_EQUAL_TO:
                return '<>';

            case IndexQueryFilterOperator::GREATER_THAN:
                return '>';

            case IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO:
                return '>=';

            case IndexQueryFilterOperator::LESS_THAN:
                return '<';

            case IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO:
                return '<=';

            default:
                return '=';
        }
    }
}
