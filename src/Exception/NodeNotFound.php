<?php

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class NodeNotFound extends \RuntimeException implements GdbotsNcrException
{
    /**
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message = '', $code = Code::NOT_FOUND, \Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
