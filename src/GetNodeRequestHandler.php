<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeResponseV1;

class GetNodeRequestHandler implements RequestHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        return [
            'gdbots:ncr:request:get-node-request',
        ];
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        $consistent = $request->get('consistent_read');
        $response = $this->createGetNodeResponse($request, $pbjx);
        $context = ['causator' => $request];

        if ($request->has('node_ref')) {
            /** @var NodeRef $nodeRef */
            $nodeRef = $request->get('node_ref');

            try {
                $node = $this->ncr->getNode($nodeRef, $consistent, $context);
            } catch (NodeNotFound $nf) {
                if (!$consistent) {
                    throw $nf;
                }

                $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNodeRef($nodeRef, $pbjx);
                $aggregate->sync($context);
                $node = $aggregate->getNode();
                if (!$node->has('last_event_ref')) {
                    // created from empty stream
                    throw $nf;
                }
                return $response->set('node', $node);
            } catch (\Throwable $e) {
                throw $e;
            }
        } elseif ($request->has('slug')) {
            $qname = SchemaQName::fromString($request->get('qname'));
            $query = IndexQueryBuilder::create($qname, 'slug', $request->get('slug'))
                ->setCount(1)
                ->build();
            $result = $this->ncr->findNodeRefs($query, $context);
            if (!$result->count()) {
                throw new NodeNotFound("Unable to locate {$qname->getMessage()}.");
            }

            $node = $this->ncr->getNode($result->getNodeRefs()[0], $consistent, $context);
            $nodeRef = $node->generateNodeRef();
        } else {
            throw new NodeNotFound('No method to locate node.');
        }

        if ($consistent) {
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
            $aggregate->sync($context);
            $node = $aggregate->getNode();
        }

        return $response->set('node', $node);
    }

    protected function createGetNodeResponse(Message $request, Pbjx $pbjx): Message
    {
        return GetNodeResponseV1::create();
    }
}
