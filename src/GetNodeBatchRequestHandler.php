<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\DependencyInjection\NcrAware;
use Gdbots\Ncr\DependencyInjection\NcrAwareTrait;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\RequestHandlerTrait;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequest;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponse;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;

class GetNodeBatchRequestHandler implements RequestHandler, NcrAware
{
    use RequestHandlerTrait;
    use NcrAwareTrait;

    /**
     * @param GetNodeBatchRequest $request
     * @param Pbjx                $pbjx
     *
     * @return GetNodeBatchResponse
     */
    protected function handle(GetNodeBatchRequest $request, Pbjx $pbjx): GetNodeBatchResponse
    {
        $nodeRefs = $request->get('node_refs');
        $response = GetNodeBatchResponseV1::create();

        if (empty($nodeRefs)) {
            return $response;
        }

        $nodes = $this->ncr->getNodes($nodeRefs, $request->get('consistent_read'), $request->get('context', []));
        foreach ($nodes as $nodeRef => $node) {
            $response->addToMap('nodes', $nodeRef, $node);
        }

        $missing = array_keys(array_diff_key(array_flip(array_map('strval', $nodeRefs)), $nodes));
        $missing = array_map(function ($str) { return NodeRef::fromString($str); }, $missing);
        $response->addToSet('missing_node_refs', $missing);

        return $response;
    }
}
