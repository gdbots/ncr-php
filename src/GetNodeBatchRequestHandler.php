<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;

class GetNodeBatchRequestHandler implements RequestHandler
{
    protected Ncr $ncr;

    public static function handlesCuries(): array
    {
        return [
            'gdbots:ncr:request:get-node-batch-request',
        ];
    }

    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        $nodeRefs = $request->get('node_refs');
        $response = $this->createGetNodeBatchResponse($request, $pbjx);

        if (empty($nodeRefs)) {
            return $response;
        }

        $context = $request->get('context', []);
        $context['causator'] = $request;
        $consistent = $request->get('consistent_read');
        $nodes = $this->ncr->getNodes($nodeRefs, $consistent, $context);

        foreach ($nodes as $nodeRef => $node) {
            $response->addToMap('nodes', $nodeRef, $node);
        }

        $missing = array_keys(array_diff_key(array_flip(array_map('strval', $nodeRefs)), $nodes));
        $missing = array_map(fn(string $str) => NodeRef::fromString($str), $missing);
        $response->addToSet('missing_node_refs', $missing);

        return $response;
    }

    protected function createGetNodeBatchResponse(Message $request, Pbjx $pbjx): Message
    {
        return GetNodeBatchResponseV1::create();
    }
}
