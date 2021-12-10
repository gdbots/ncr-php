<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;
use Gdbots\Ncr\Exception\IndexQueryNotSupported;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Pbj\Message;

abstract class AbstractIndex implements GlobalSecondaryIndex
{
    public function getName(): string
    {
        return "{$this->getAlias()}_index";
    }

    public function getRangeKeyName(): ?string
    {
        return null;
    }

    public function getFilterableAttributes(): array
    {
        return [];
    }

    public function getProjection(): array
    {
        return [];
    }

    public function beforePutItem(array &$item, Message $node): void
    {
    }

    final public function createQuery(IndexQuery $query): array
    {
        $filterables = $this->getFilterableAttributes();
        $params = [
            'IndexName'                 => $this->getName(),
            'ScanIndexForward'          => $query->sortAsc(),
            //'Limit'                     => $query->getCount(),
            'ExpressionAttributeNames'  => [
                '#hash'     => $this->getHashKeyName(),
                '#node_ref' => NodeTable::HASH_KEY_NAME,
            ],
            'ExpressionAttributeValues' => [
                ':v_hash'  => ['S' => $query->getValue()],
                ':v_qname' => ['S' => $query->getQName()->toString() . ':'],
            ],
            // special key to deal with filters that must be
            // processed AFTER the query runs due to limitation of range key
            // only allowing for one expression.
            'unprocessed_filters'       => [],
        ];

        $keyConditionExpressions = ['#hash = :v_hash'];
        $filterExpressions = ['begins_with(#node_ref, :v_qname)'];
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

    private function marshalValue(string $attributeType, mixed $value): bool|string
    {
        return match ($attributeType) {
            'BOOL' => filter_var($value, FILTER_VALIDATE_BOOLEAN),
            default => (string)$value,
        };
    }

    private function getDynamoDbOperator(IndexQueryFilterOperator $operator): string
    {
        return match ($operator) {
            IndexQueryFilterOperator::NOT_EQUAL_TO => '<>',
            IndexQueryFilterOperator::GREATER_THAN => '>',
            IndexQueryFilterOperator::GREATER_THAN_OR_EQUAL_TO => '>=',
            IndexQueryFilterOperator::LESS_THAN => '<',
            IndexQueryFilterOperator::LESS_THAN_OR_EQUAL_TO => '<=',
            default => '=',
        };
    }
}
