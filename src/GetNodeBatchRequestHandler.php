<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;

// fixme: add NcrPolicy logic here or in binder/validator?
class GetNodeBatchRequestHandler implements RequestHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        return [
            GetNodeBatchRequestV1::SCHEMA_CURIE,
        ];
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        $nodeRefs = $request->get(GetNodeBatchRequestV1::NODE_REFS_FIELD);
        $response = GetNodeBatchResponseV1::create();

        if (empty($nodeRefs)) {
            return $response;
        }

        $context = $request->get(GetNodeBatchRequestV1::CONTEXT_FIELD, []);
        $context['causator'] = $request;
        $consistent = $request->get(GetNodeBatchRequestV1::CONSISTENT_READ_FIELD);
        $nodes = $this->ncr->getNodes($nodeRefs, $consistent, $context);

        foreach ($nodes as $nodeRef => $node) {
            $response->addToMap($response::NODES_FIELD, $nodeRef, $node);
        }

        $missing = array_keys(array_diff_key(array_flip(array_map('strval', $nodeRefs)), $nodes));
        $missing = array_map(fn(string $str) => NodeRef::fromString($str), $missing);
        $response->addToSet($response::MISSING_NODE_REFS_FIELD, $missing);

        return $response;
    }
}
