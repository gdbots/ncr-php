<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Exception;

use Gdbots\Pbj\Exception\HasEndUserMessage;
use Gdbots\Schemas\Pbjx\Enum\Code;

final class NodeAlreadyLocked extends \RuntimeException implements GdbotsNcrException, HasEndUserMessage
{
    /**
     * @param string $message
     */
    public function __construct(string $message = 'Node already locked.')
    {
        parent::__construct($message, Code::FAILED_PRECONDITION);
    }

    /**
     * {@inheritdoc}
     */
    public function getEndUserMessage()
    {
        return $this->getMessage();
    }

    /**
     * {@inheritdoc}
     */
    public function getEndUserHelpLink()
    {
        return null;
    }
}
