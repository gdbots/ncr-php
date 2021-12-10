<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class NodeAlreadyLocked extends \RuntimeException implements GdbotsNcrException
{
    public function __construct(string $message = 'Node already locked.')
    {
        parent::__construct($message, Code::FAILED_PRECONDITION->value);
    }
}
