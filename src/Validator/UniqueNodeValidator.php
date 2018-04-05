<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Ncr\PbjxHelperTrait;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\StreamId;

class UniqueNodeValidator implements EventSubscriber, PbjxValidator
{
    use PbjxHelperTrait;

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('node'), 'Field "node" is required.', 'node');
        $node = $command->get('node');
        $nodeRef = NodeRef::fromNode($node);

        if ($node->has('slug')) {
            $this->ensureSlugIsAvailable($pbjxEvent, $nodeRef, $node->get('slug'));
        }

        $this->ensureStreamDoesNotExist($pbjxEvent, $nodeRef);
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function validateRenameNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('node_ref'), 'Field "node_ref" is required.', 'node_ref');
        Assertion::true($command->has('new_slug'), 'Field "new_slug" is required.', 'new_slug');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->ensureSlugIsAvailable($pbjxEvent, $nodeRef, $command->get('new_slug'));
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function validateUpdateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('new_node'), 'Field "new_node" is required.', 'new_node');
        $newNode = $command->get('new_node');

        /*
         * An update SHOULD NOT change the slug, so copy the slug from
         * the old node if it's present. To change the slug, use proper
         * "rename" command.
         */
        if ($command->has('old_node')) {
            $oldNode = $command->get('old_node');

            if ($oldNode->has('slug')) {
                $newNode->set('slug', $oldNode->get('slug'));
            }
        }
    }

    /**
     * @param PbjxEvent $pbjxEvent
     * @param NodeRef   $nodeRef
     * @param string    $slug
     *
     * @throws NodeAlreadyExists
     */
    protected function ensureSlugIsAvailable(PbjxEvent $pbjxEvent, NodeRef $nodeRef, string $slug): void
    {
        $pbjx = $pbjxEvent::getPbjx();
        $command = $pbjxEvent->getMessage();

        try {
            $request = $this->createGetNodeRequest($command, $nodeRef, $pbjx)
                ->set('consistent_read', true)
                ->set('qname', $nodeRef->getQName()->toString())
                ->set('slug', $slug);

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND === $e->getResponse()->get('error_code')) {
                // this is what we want
                return;
            }

            throw $e;
        } catch (\Throwable $t) {
            throw $t;
        }

        /** @var Node $node */
        $node = $response->get('node');

        if ($nodeRef->getId() === (string)$node->get('_id')) {
            // this is the same node.
            return;
        }

        throw new NodeAlreadyExists(
            sprintf(
                'The [%s] with slug [%s] already exists so [%s] cannot continue.',
                $node::schema()->getCurie()->getMessage(),
                $slug,
                $command->generateMessageRef()
            )
        );
    }

    /**
     * @param PbjxEvent $pbjxEvent
     * @param NodeRef   $nodeRef
     *
     * @throws NodeAlreadyExists
     */
    protected function ensureStreamDoesNotExist(PbjxEvent $pbjxEvent, NodeRef $nodeRef): void
    {
        $pbjx = $pbjxEvent::getPbjx();
        $message = $pbjxEvent->getMessage();

        $streamId = $this->createStreamId($nodeRef);
        $slice = $pbjx->getEventStore()->getStreamSlice(
            $streamId,
            null,
            1,
            true,
            true,
            $this->createEventStoreContext($message, $streamId)
        );

        if ($slice->count()) {
            throw new NodeAlreadyExists(
                sprintf(
                    'The [%s] with id [%s] already exists so [%s] cannot continue.',
                    $nodeRef->getLabel(),
                    $nodeRef->getId(),
                    $message->generateMessageRef()
                )
            );
        }
    }

    /**
     * @param NodeRef $nodeRef
     *
     * @return StreamId
     */
    protected function createStreamId(NodeRef $nodeRef): StreamId
    {
        return StreamId::fromString(sprintf('%s.history:%s', $nodeRef->getLabel(), $nodeRef->getId()));
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:create-node.validate' => 'validateCreateNode',
            'gdbots:ncr:mixin:rename-node.validate' => 'validateRenameNode',
            'gdbots:ncr:mixin:update-node.validate' => 'validateUpdateNode',
        ];
    }
}
