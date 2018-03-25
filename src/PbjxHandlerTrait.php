<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Event\BindFromNodeEvent;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Pbjx\StreamId;

trait PbjxHandlerTrait
{
    /**
     * The handler generally has the current node which provides
     * an opportunity to bind data to the event, response, etc.
     *
     * @param Message $message
     * @param Node    $node
     * @param Pbjx    $pbjx
     */
    protected function bindFromNode(Message $message, Node $node, Pbjx $pbjx): void
    {
        $node->freeze();
        $pbjxEvent = new BindFromNodeEvent($message, $node);
        $pbjx->trigger($message, 'bind_from_node', $pbjxEvent, false);
    }

    /**
     * @param Message  $message
     * @param StreamId $streamId
     *
     * @return array
     */
    protected function createEventStoreContext(Message $message, StreamId $streamId): array
    {
        return [];
    }

    /**
     * @param Message $message
     *
     * @return array
     */
    protected function createNcrContext(Message $message): array
    {
        return [];
    }
}
