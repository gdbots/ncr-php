<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class OptimisticCheckFailed extends \RuntimeException implements GdbotsNcrException
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
