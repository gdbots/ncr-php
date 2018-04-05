<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Binder;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\PbjxHelperTrait;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbjx\DependencyInjection\PbjxBinder;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Pbjx\Exception\RequestHandlingFailed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Gdbots\Schemas\Pbjx\Mixin\Command\Command;

class NodeCommandBinder implements EventSubscriber, PbjxBinder
{
    use PbjxHelperTrait;

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function bindRenameNode(PbjxEvent $pbjxEvent): void
    {
        /** @var Command $command */
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has('new_slug'), 'Field "new_slug" is required.', 'new_slug');

        $node = $this->getNode($pbjxEvent);
        $command
            ->set('node_status', $node->get('status'))
            ->set('old_slug', $node->get('slug'))
            ->set('expected_etag', $node->get('etag'));
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function bindUpdateNode(PbjxEvent $pbjxEvent): void
    {
        $node = $this->getNode($pbjxEvent);
        $pbjxEvent->getMessage()
            ->set('old_node', $node)
            ->set('expected_etag', $node->get('etag'));
    }

    /**
     * @param PbjxEvent $pbjxEvent
     *
     * @return Node
     *
     * @throws \Throwable
     */
    protected function getNode(PbjxEvent $pbjxEvent): Node
    {
        /** @var Command $command */
        $command = $pbjxEvent->getMessage();
        Assertion::true($command->has('node_ref'), 'Field "node_ref" is required.', 'node_ref');

        /** @var NodeRef $nodeRef */
        $nodeRef = $command->get('node_ref');
        $pbjx = $pbjxEvent::getPbjx();

        try {
            $request = $this->createGetNodeRequest($command, $nodeRef, $pbjx)
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

        $expectedEtag = $command->get('expected_etag');
        if (null !== $expectedEtag && $expectedEtag !== $response->get('node')->get('etag')) {
            throw new OptimisticCheckFailed(
                sprintf('NodeRef [%s] did not have expected etag [%s].', $nodeRef, $expectedEtag)
            );
        }

        return $response->get('node')->freeze();
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:rename-node.bind' => 'bindRenameNode',
            'gdbots:ncr:mixin:update-node.bind' => 'bindUpdateNode',
        ];
    }
}
