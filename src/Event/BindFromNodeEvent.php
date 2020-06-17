<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Event;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Event\PbjxEvent;

class BindFromNodeEvent extends PbjxEvent
{
    protected Message $node;

    public function __construct(Message $message, Message $node)
    {
        parent::__construct($message);
        $this->node = $node;
    }

    public function getNode(): Message
    {
        return $this->node;
    }

    public function supportsRecursion(): bool
    {
        return false;
    }
}
