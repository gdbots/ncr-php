<?php

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class RepositoryOperationFailed extends \RuntimeException implements GdbotsNcrException
{
    /**
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message = '', $code = Code::INTERNAL, \Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
