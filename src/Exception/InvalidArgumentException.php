<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class InvalidArgumentException extends \InvalidArgumentException implements GdbotsNcrException
{
    /**
     * @param string     $message
     * @param \Throwable $previous
     */
    public function __construct(string $message = '', ?\Throwable $previous = null)
    {
        parent::__construct($message, Code::INVALID_ARGUMENT, $previous);
    }
}
