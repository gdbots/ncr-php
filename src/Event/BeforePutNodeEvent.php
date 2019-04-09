<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Event;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Event\PbjxEvent;

class BeforePutNodeEvent extends PbjxEvent
{
    /** @var Message */
    protected $lastEvent;

    /**
     * @param Message $node
     * @param Message $lastEvent
     */
    public function __construct(Message $node, Message $lastEvent)
    {
        parent::__construct($node);
        $this->lastEvent = $lastEvent;
    }

    /**
     * @return Message
     */
    public function getLastEvent(): Message
    {
        return $this->lastEvent;
    }

    /**
     * @return bool
     */
    public function supportsRecursion(): bool
    {
        return false;
    }
}
