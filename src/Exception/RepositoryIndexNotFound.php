<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class RepositoryIndexNotFound extends RepositoryOperationFailed
{
    /**
     * @param string     $message
     * @param \Exception $previous
     */
    public function __construct(string $message, ?\Exception $previous = null)
    {
        parent::__construct($message, Code::UNIMPLEMENTED, $previous);
    }
}
