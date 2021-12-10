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
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Pbjx\Enum\Code;

class NodeCommandBinder implements EventSubscriber, PbjxBinder
{
    public static function getSubscribedEvents(): array
    {
        return [
            'gdbots:ncr:command:rename-node.bind' => 'bindRenameNode',
            'gdbots:ncr:command:update-node.bind' => 'bindUpdateNode',
            // deprecated mixins, will be removed in 4.x.
            'gdbots:ncr:mixin:rename-node.bind'   => 'bindRenameNode',
            'gdbots:ncr:mixin:update-node.bind'   => 'bindUpdateNode',
        ];
    }

    public function bindRenameNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has('new_slug'), 'Field "new_slug" is required.', 'new_slug');

        $node = $this->getNode($pbjxEvent);
        $command
            ->set('node_status', $node->get('status'))
            ->set('old_slug', $node->get('slug'))
            ->set('expected_etag', $node->get('etag'));
    }

    public function bindUpdateNode(PbjxEvent $pbjxEvent): void
    {
        $node = $this->getNode($pbjxEvent);
        $pbjxEvent->getMessage()
            ->set('old_node', $node)
            ->set('expected_etag', $node->get('etag'));
    }

    protected function getNode(PbjxEvent $pbjxEvent): Message
    {
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has('node_ref'), 'Field "node_ref" is required.', 'node_ref');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $pbjx = $pbjxEvent::getPbjx();

        try {
            $request = GetNodeRequestV1::create()
                ->set('consistent_read', true)
                ->set('node_ref', $nodeRef)
                ->set('qname', $nodeRef->getQName()->toString());

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND->value === $e->getCode()) {
                throw NodeNotFound::forNodeRef($nodeRef, $e);
            }

            throw $e;
        } catch (\Throwable $e) {
            throw $e;
        }

        $expectedEtag = $command->get('expected_etag');
        $actualEtag = $response->get('node')->get('etag');
        if (null !== $expectedEtag && $expectedEtag !== $actualEtag) {
            throw new OptimisticCheckFailed(
                sprintf('NodeRef [%s] did not have expected etag [%s].', $nodeRef, $expectedEtag)
            );
        }

        return $response->get('node')->freeze();
    }
}
