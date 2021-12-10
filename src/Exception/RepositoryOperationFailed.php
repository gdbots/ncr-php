<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class RepositoryOperationFailed extends \RuntimeException implements GdbotsNcrException
{
    public function __construct(string $message = '', int $code = 13, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
