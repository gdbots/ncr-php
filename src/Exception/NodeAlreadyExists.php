<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

final class NodeAlreadyExists extends \RuntimeException implements GdbotsNcrException
{
    public function __construct(string $message = 'Node already exists.')
    {
        parent::__construct($message, Code::ALREADY_EXISTS->value);
    }
}
