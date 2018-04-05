<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Event\BindFromNodeEvent;
use Gdbots\Ncr\Exception\InvalidArgumentException;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaCurie;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\GetNodeRequest\GetNodeRequest;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Mixin\Request\Request;
use Gdbots\Schemas\Pbjx\Mixin\Response\Response;
use Gdbots\Schemas\Pbjx\StreamId;

trait PbjxHelperTrait
{
    /**
     * @param Node $node
     *
     * @throws InvalidArgumentException
     */
    protected function assertIsNodeSupported(Node $node): void
    {
        if (!$this->isNodeSupported($node)) {
            $class = ClassUtils::getShortName(static::class);
            throw new InvalidArgumentException(
                "Node [{$node::schema()->getCurie()}] not supported by [{$class}]."
            );
        }
    }

    /**
     * Determines if the given node can or should be handled by this handler.
     * A sanity/security check to ensure mismatched node refs are not given
     * maliciously or otherwise to unsuspecting handlers.
     *
     * Override this method in your own handler, binder, validator, etc.
     *
     * @param Node $node
     *
     * @return bool
     */
    protected function isNodeSupported(Node $node): bool
    {
        return false;
    }

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
     * A conventional factory to create a get node request from an existing message
     * and node ref.  Various pbjx lifecycle events need to fetch the node for the
     * operation currently executing. For example, when attempting to update a node
     * we fetch the current node in order to make some validations possible.
     *
     * Override this method if the convention doesn't match or needs to have a special
     * case handled.  You can also override when you need to populate data on the
     * get node request before it's processed (multi-tenant apps sometimes need this).
     *
     * @param Message $message
     * @param NodeRef $nodeRef
     * @param Pbjx    $pbjx
     *
     * @return GetNodeRequest
     */
    protected function createGetNodeRequest(Message $message, NodeRef $nodeRef, Pbjx $pbjx): GetNodeRequest
    {
        $curie = $message::schema()->getCurie();

        /** @var GetNodeRequest $class */
        $class = MessageResolver::resolveCurie(SchemaCurie::fromString(
            "{$curie->getVendor()}:{$curie->getPackage()}:request:get-{$nodeRef->getLabel()}-request"
        ));

        return $class::create();
    }

    /**
     * Conventionally the response messages are named the same as the request but
     * with a "-response" suffix.  Override when needed.
     *
     * @param Request $request
     * @param Pbjx    $pbjx
     *
     * @return Response
     */
    protected function createResponseFromRequest(Request $request, Pbjx $pbjx): Response
    {
        $curie = str_replace('-request', '-response', $request::schema()->getCurie()->toString());
        /** @var Response $class */
        $class = MessageResolver::resolveCurie(SchemaCurie::fromString($curie));
        return $class::create();
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

    /**
     * @param Message $message
     *
     * @return array
     */
    protected function createNcrSearchContext(Message $message): array
    {
        return [];
    }
}
