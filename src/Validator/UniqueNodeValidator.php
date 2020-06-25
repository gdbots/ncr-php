<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Command\RenameNodeV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeV1;
use Gdbots\Schemas\Ncr\Mixin\CreateNode\CreateNodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\RenameNode\RenameNodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\SluggableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\UpdateNode\UpdateNodeV1Mixin;
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\StreamId;

class UniqueNodeValidator implements EventSubscriber, PbjxValidator
{
    public static function getSubscribedEvents()
    {
        return [
            CreateNodeV1::SCHEMA_CURIE . '.validate'      => 'validateCreateNode',
            RenameNodeV1::SCHEMA_CURIE . '.validate'      => 'validateRenameNode',
            UpdateNodeV1::SCHEMA_CURIE . '.validate'      => 'validateUpdateNode',
            // deprecated mixins, will be removed in 3.x
            CreateNodeV1Mixin::SCHEMA_CURIE . '.validate' => 'validateCreateNode',
            RenameNodeV1Mixin::SCHEMA_CURIE . '.validate' => 'validateRenameNode',
            UpdateNodeV1Mixin::SCHEMA_CURIE . '.validate' => 'validateUpdateNode',
        ];
    }

    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has(CreateNodeV1::NODE_FIELD), 'Field "node" is required.', 'node');
        $node = $command->get(CreateNodeV1::NODE_FIELD);
        $nodeRef = NodeRef::fromNode($node);

        if ($node->has(SluggableV1Mixin::SLUG_FIELD)) {
            $this->ensureSlugIsAvailable($pbjxEvent, $nodeRef, $node->get(SluggableV1Mixin::SLUG_FIELD));
        }

        $this->ensureStreamDoesNotExist($pbjxEvent, $nodeRef);
    }

    public function validateRenameNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has(RenameNodeV1::NODE_REF_FIELD), 'Field "node_ref" is required.', 'node_ref');
        Assertion::true($command->has(RenameNodeV1::NEW_SLUG_FIELD), 'Field "new_slug" is required.', 'new_slug');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(RenameNodeV1::NODE_REF_FIELD);
        $this->ensureSlugIsAvailable($pbjxEvent, $nodeRef, $command->get(RenameNodeV1::NEW_SLUG_FIELD));
    }

    public function validateUpdateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has(UpdateNodeV1::NEW_NODE_FIELD), 'Field "new_node" is required.', 'new_node');
        $newNode = $command->get(UpdateNodeV1::NEW_NODE_FIELD);

        /*
         * An update SHOULD NOT change the slug, so copy the slug from
         * the old node if it's present. To change the slug, use proper
         * "rename" command.
         */
        if ($command->has(UpdateNodeV1::OLD_NODE_FIELD)) {
            $oldNode = $command->get(UpdateNodeV1::OLD_NODE_FIELD);

            if ($oldNode->has(SluggableV1Mixin::SLUG_FIELD)) {
                $newNode->set(SluggableV1Mixin::SLUG_FIELD, $oldNode->get(SluggableV1Mixin::SLUG_FIELD));
            }
        }
    }

    protected function ensureSlugIsAvailable(PbjxEvent $pbjxEvent, NodeRef $nodeRef, string $slug): void
    {
        $pbjx = $pbjxEvent::getPbjx();
        $command = $pbjxEvent->getMessage();

        try {
            $request = GetNodeRequestV1::create()
                ->set(GetNodeRequestV1::CONSISTENT_READ_FIELD, true)
                ->set(GetNodeRequestV1::QNAME_FIELD, $nodeRef->getQName()->toString())
                ->set(GetNodeRequestV1::SLUG_FIELD, $slug);

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND === $e->getCode()) {
                // this is what we want
                return;
            }

            throw $e;
        } catch (\Throwable $t) {
            throw $t;
        }

        /** @var Message $node */
        $node = $response->get($response::NODE_FIELD);

        if ($nodeRef->getId() === $node->fget(NodeV1Mixin::_ID_FIELD)) {
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

    protected function ensureStreamDoesNotExist(PbjxEvent $pbjxEvent, NodeRef $nodeRef): void
    {
        $pbjx = $pbjxEvent::getPbjx();
        $message = $pbjxEvent->getMessage();

        $slice = $pbjx->getEventStore()->getStreamSlice(
            StreamId::fromNodeRef($nodeRef),
            null,
            1,
            true,
            true,
            ['causator' => $message]
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
}
