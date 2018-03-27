<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Binder;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaCurie;
use Gdbots\Pbjx\DependencyInjection\PbjxBinder;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\GetNodeRequest\GetNodeRequest;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\Mixin\Command\Command;

class NodeCommandBinder implements EventSubscriber, PbjxBinder
{
    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function bindUpdateNode(PbjxEvent $pbjxEvent): void
    {
        /** @var Command $command */
        $command = $pbjxEvent->getMessage();

        $node = $this->getNodeForCommand($command, $pbjxEvent::getPbjx());
        $command
            ->set('old_node', $node)
            ->set('expected_etag', $node->get('etag'));
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function bindRenameNode(PbjxEvent $pbjxEvent): void
    {
        /** @var Command $command */
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('new_slug'), 'Field "new_slug" is required.', 'new_slug');
        $node = $this->getNodeForCommand($command, $pbjxEvent::getPbjx());
        $command
            ->set('node_status', $node->get('status'))
            ->set('old_slug', $node->get('slug'))
            ->set('expected_etag', $node->get('etag'));
    }

    /**
     * @param Command $command
     * @param Pbjx    $pbjx
     *
     * @return Node
     *
     * @throws \Throwable
     */
    protected function getNodeForCommand(Command $command, Pbjx $pbjx): Node
    {
        Assertion::true($command->has('node_ref'), 'Field "node_ref" is required.', 'node_ref');
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');

        try {
            $request = $this->createGetNodeRequest($command, $pbjx)
                ->set('consistent_read', true)
                ->set('node_ref', $nodeRef)
                ->set('qname', $nodeRef->getQName()->toString());

            $response = $pbjx->copyContext($command, $request)->request($request);
        } catch (RequestHandlingFailed $e) {
            if (Code::NOT_FOUND === $e->getResponse()->get('error_code')) {
                throw NodeNotFound::forNodeRef($nodeRef, $e);
            }

            throw $e;
        } catch (\Throwable $e) {
            throw $e;
        }

        if ($command->has('expected_etag') && $command->get('expected_etag') !== $response->get('node')->get('etag')) {
            throw new OptimisticCheckFailed(
                sprintf('NodeRef [%s] did not have expected etag [%s].', $nodeRef, $command->get('expected_etag'))
            );
        }

        return $response->get('node')->freeze();
    }

    /**
     * @param Command $command
     * @param Pbjx    $pbjx
     *
     * @return GetNodeRequest
     */
    protected function createGetNodeRequest(Command $command, Pbjx $pbjx): GetNodeRequest
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $curie = $command::schema()->getCurie();

        /** @var GetNodeRequest $class */
        $class = MessageResolver::resolveCurie(SchemaCurie::fromString(
            "{$curie->getVendor()}:{$curie->getPackage()}:request:get-{$nodeRef->getLabel()}-request"
        ));

        /** @var GetNodeRequest $request */
        $request = $class::schema()->createMessage();
        return $request;
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:update-node.bind' => 'bindUpdateNode',
            'gdbots:ncr:mixin:rename-node.bind' => 'bindRenameNode',
        ];
    }
}
