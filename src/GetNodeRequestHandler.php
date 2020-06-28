<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeResponseV1;

// todo: add handling for when node hasn't been projected yet
class GetNodeRequestHandler implements RequestHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        return [
            GetNodeRequestV1::SCHEMA_CURIE,
        ];
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $request->get(GetNodeRequestV1::NODE_REF_FIELD);
        $consistent = $request->get(GetNodeRequestV1::CONSISTENT_READ_FIELD);
        $response = $this->createGetNodeResponse($request, $pbjx);
        $context = ['causator' => $request];

        if ($request->has(GetNodeRequestV1::NODE_REF_FIELD)) {
            $node = $this->ncr->getNode(
                $request->get(GetNodeRequestV1::NODE_REF_FIELD),
                $consistent,
                $context
            );
        } elseif ($request->has(GetNodeRequestV1::SLUG_FIELD)) {
            $qname = SchemaQName::fromString($request->get(GetNodeRequestV1::QNAME_FIELD));
            $query = IndexQueryBuilder::create($qname, 'slug', $request->get(GetNodeRequestV1::SLUG_FIELD))
                ->setCount(1)
                ->build();
            $result = $this->ncr->findNodeRefs($query, $context);
            if (!$result->count()) {
                throw new NodeNotFound("Unable to locate {$qname->getMessage()}.");
            }

            $node = $this->ncr->getNode($result->getNodeRefs()[0], $consistent, $context);
        } else {
            throw new NodeNotFound('No method to locate node.');
        }

        if ($consistent) {
            $aggregate = AggregateResolver::resolve($nodeRef->getQName())::fromNode($node, $pbjx);
            $aggregate->sync($context);
            $node = $aggregate->getNode();
        }

        return $response->set($response::NODE_FIELD, $node);
    }

    protected function createGetNodeResponse(Message $request, Pbjx $pbjx): Message
    {
        return GetNodeResponseV1::create();
    }
}
