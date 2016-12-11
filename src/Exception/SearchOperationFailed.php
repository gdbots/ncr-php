<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Schemas\Pbjx\Enum\Code;

class SearchOperationFailed extends \RuntimeException implements GdbotsNcrException
{
    /**
     * @param string          $message
     * @param int             $code
     * @param \Exception|null $previous
     */
    public function __construct(string $message = '', int $code = Code::INTERNAL, ?\Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
