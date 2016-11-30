<?php

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class RepositoryIndexNotFound extends RepositoryOperationFailed
{
    /**
     * @param string $message
     * @param \Exception|null $previous
     */
    public function __construct($message, \Exception $previous = null)
    {
        parent::__construct($message, Code::UNIMPLEMENTED, $previous);
    }
}
