<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\ToArray;
use Gdbots\Ncr\Enum\IndexQueryFilterOperator;

final class IndexQueryFilter implements ToArray, \JsonSerializable
{
    /** @var string */
    private $field;

    /** @var IndexQueryFilterOperator */
    private $operator;

    /** @var mixed */
    private $value;

    /**
     * @param string                   $field
     * @param IndexQueryFilterOperator $operator
     * @param mixed                    $value
     */
    public function __construct(string $field, IndexQueryFilterOperator $operator, $value)
    {
        $this->field = $field;
        $this->operator = $operator;
        $this->value = $value;
    }

    /**
     * @return string
     */
    public function getField(): string
    {
        return $this->field;
    }

    /**
     * @return IndexQueryFilterOperator
     */
    public function getOperator(): IndexQueryFilterOperator
    {
        return $this->operator;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * {@inheritdoc}
     */
    public function toArray()
    {
        return [
            'field'    => $this->field,
            'operator' => $this->operator->getValue(),
            'value'    => $this->value,
        ];
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }
}
