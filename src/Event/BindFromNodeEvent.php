<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Event;

use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;

class BindFromNodeEvent extends PbjxEvent
{
    /** @var Node */
    protected $node;

    /**
     * @param Message $message
     * @param Node    $node
     */
    public function __construct(Message $message, Node $node)
    {
        parent::__construct($message);
        $this->node = $node;
    }

    /**
     * @return Node
     */
    public function getNode(): Node
    {
        return $this->node;
    }

    /**
     * @return bool
     */
    public function supportsRecursion(): bool
    {
        return false;
    }
}
