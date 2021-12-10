<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\StreamId;

class UniqueNodeValidator implements EventSubscriber, PbjxValidator
{
    public static function getSubscribedEvents(): array
    {
        return [
            'gdbots:ncr:command:create-node.validate' => 'validateCreateNode',
            'gdbots:ncr:command:rename-node.validate' => 'validateRenameNode',
            'gdbots:ncr:command:update-node.validate' => 'validateUpdateNode',
            // deprecated mixins, will be removed in 4.x.
            'gdbots:ncr:mixin:create-node.validate'   => 'validateCreateNode',
            'gdbots:ncr:mixin:rename-node.validate'   => 'validateRenameNode',
            'gdbots:ncr:mixin:update-node.validate'   => 'validateUpdateNode',
        ];
    }

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

    public function validateRenameNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('node_ref'), 'Field "node_ref" is required.', 'node_ref');
        Assertion::true($command->has('new_slug'), 'Field "new_slug" is required.', 'new_slug');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $this->ensureSlugIsAvailable($pbjxEvent, $nodeRef, $command->get('new_slug'));
    }

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

    protected function ensureSlugIsAvailable(PbjxEvent $pbjxEvent, NodeRef $nodeRef, string $slug): void
    {
        $pbjx = $pbjxEvent::getPbjx();
        $command = $pbjxEvent->getMessage();

        try {
            $request = GetNodeRequestV1::create()
                ->set('consistent_read', true)
                ->set('qname', $nodeRef->getQName()->toString())
                ->set('slug', $slug);

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND->value === $e->getCode()) {
                // this is what we want
                return;
            }

            throw $e;
        } catch (\Throwable $t) {
            throw $t;
        }

        /** @var Message $node */
        $node = $response->get('node');

        if ($nodeRef->getId() === $node->fget('_id')) {
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
