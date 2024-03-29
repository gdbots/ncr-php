<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class IndexQueryNotSupported extends \LogicException implements GdbotsNcrException
{
    public function __construct(string $message, ?\Throwable $previous = null)
    {
        parent::__construct($message, Code::UNIMPLEMENTED->value, $previous);
    }
}
