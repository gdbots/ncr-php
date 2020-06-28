<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\DependencyInjection\PbjxBinder;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Schemas\Ncr\Command\RenameNodeV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeV1;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\RenameNode\RenameNodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Sluggable\SluggableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\UpdateNode\UpdateNodeV1Mixin;
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Pbjx\Enum\Code;

class NodeCommandBinder implements EventSubscriber, PbjxBinder
{
    public static function getSubscribedEvents()
    {
        return [
            RenameNodeV1::SCHEMA_CURIE . '.bind'      => 'bindRenameNode',
            UpdateNodeV1::SCHEMA_CURIE . '.bind'      => 'bindUpdateNode',
            // deprecated mixins, will be removed in 3.x
            RenameNodeV1Mixin::SCHEMA_CURIE . '.bind' => 'bindRenameNode',
            UpdateNodeV1Mixin::SCHEMA_CURIE . '.bind' => 'bindUpdateNode',
        ];
    }

    public function bindRenameNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has(RenameNodeV1::NEW_SLUG_FIELD), 'Field "new_slug" is required.', 'new_slug');

        $node = $this->getNode($pbjxEvent);
        $command
            ->set(RenameNodeV1::NODE_STATUS_FIELD, $node->get(NodeV1Mixin::STATUS_FIELD))
            ->set(RenameNodeV1::OLD_SLUG_FIELD, $node->get(SluggableV1Mixin::SLUG_FIELD))
            ->set(RenameNodeV1::EXPECTED_ETAG_FIELD, $node->get(NodeV1Mixin::ETAG_FIELD));
    }

    public function bindUpdateNode(PbjxEvent $pbjxEvent): void
    {
        $node = $this->getNode($pbjxEvent);
        $pbjxEvent->getMessage()
            ->set(UpdateNodeV1::OLD_NODE_FIELD, $node)
            ->set(UpdateNodeV1::EXPECTED_ETAG_FIELD, $node->get(NodeV1Mixin::ETAG_FIELD));
    }

    protected function getNode(PbjxEvent $pbjxEvent): Message
    {
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has(UpdateNodeV1::NODE_REF_FIELD), 'Field "node_ref" is required.', 'node_ref');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get(UpdateNodeV1::NODE_REF_FIELD);
        $pbjx = $pbjxEvent::getPbjx();

        try {
            $request = GetNodeRequestV1::create()
                ->set(GetNodeRequestV1::CONSISTENT_READ_FIELD, true)
                ->set(GetNodeRequestV1::NODE_REF_FIELD, $nodeRef)
                ->set(GetNodeRequestV1::QNAME_FIELD, $nodeRef->getQName()->toString());

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND === $e->getCode()) {
                throw NodeNotFound::forNodeRef($nodeRef, $e);
            }

            throw $e;
        } catch (\Throwable $e) {
            throw $e;
        }

        $expectedEtag = $command->get(UpdateNodeV1::EXPECTED_ETAG_FIELD);
        $actualEtag = $response->get($response::NODE_FIELD)->get(NodeV1Mixin::ETAG_FIELD);
        if (null !== $expectedEtag && $expectedEtag !== $actualEtag) {
            throw new OptimisticCheckFailed(
                sprintf('NodeRef [%s] did not have expected etag [%s].', $nodeRef, $expectedEtag)
            );
        }

        return $response->get($response::NODE_FIELD)->freeze();
    }
}
