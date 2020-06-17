<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class LogicException extends \LogicException implements GdbotsNcrException
{
    public function __construct(string $message = '', int $code = Code::INTERNAL, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
