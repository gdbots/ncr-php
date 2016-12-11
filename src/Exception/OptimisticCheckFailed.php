<?php

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class OptimisticCheckFailed extends \RuntimeException implements GdbotsNcrException
{
    /**
     * @param string          $message
     * @param \Exception|null $previous
     */
    public function __construct(string $message = '', ?\Exception $previous = null)
    {
        parent::__construct($message, Code::FAILED_PRECONDITION, $previous);
    }
}
