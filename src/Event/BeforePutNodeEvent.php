<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Event;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Event\PbjxEvent;

class BeforePutNodeEvent extends PbjxEvent
{
    protected Message $lastEvent;

    public function __construct(Message $node, Message $lastEvent)
    {
        parent::__construct($node);
        $this->lastEvent = $lastEvent;
    }

    public function getLastEvent(): Message
    {
        return $this->lastEvent;
    }

    public function supportsRecursion(): bool
    {
        return false;
    }
}
