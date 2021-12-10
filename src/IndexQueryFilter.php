<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Enum\IndexQueryFilterOperator;

final class IndexQueryFilter implements \JsonSerializable
{
    private string $field;
    private IndexQueryFilterOperator $operator;
    private mixed $value;

    public function __construct(string $field, IndexQueryFilterOperator $operator, mixed $value)
    {
        $this->field = $field;
        $this->operator = $operator;
        $this->value = $value instanceof \BackedEnum ? $value->value : $value;
    }

    public function getField(): string
    {
        return $this->field;
    }

    public function getOperator(): IndexQueryFilterOperator
    {
        return $this->operator;
    }

    public function getValue(): mixed
    {
        return $this->value;
    }

    public function toArray(): array
    {
        return [
            'field'    => $this->field,
            'operator' => $this->operator->value,
            'value'    => $this->value,
        ];
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }
}
