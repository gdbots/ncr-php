<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\GetNodeBatchRequest\GetNodeBatchRequest;
use Gdbots\Schemas\Ncr\Mixin\GetNodeBatchResponse\GetNodeBatchResponse;
use Gdbots\Schemas\Ncr\NodeRef;

abstract class AbstractGetNodeBatchRequestHandler extends AbstractRequestHandler
{
    /** @var Ncr */
    protected $ncr;

    /**
     * @param Ncr $ncr
     */
    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    /**
     * @param GetNodeBatchRequest $request
     * @param Pbjx                $pbjx
     *
     * @return GetNodeBatchResponse
     */
    protected function handle(GetNodeBatchRequest $request, Pbjx $pbjx): GetNodeBatchResponse
    {
        $response = $this->createGetNodeBatchResponse($request, $pbjx);
        $nodeRefs = $request->get('node_refs');

        if (empty($nodeRefs)) {
            return $response;
        }

        $context = $this->createNcrContext($request);
        $nodes = $this->ncr->getNodes($nodeRefs, $request->get('consistent_read'), $context);
        foreach ($nodes as $nodeRef => $node) {
            $this->assertIsNodeSupported($node);
            $response->addToMap('nodes', $nodeRef, $node);
        }

        $missing = array_keys(array_diff_key(array_flip(array_map('strval', $nodeRefs)), $nodes));
        $missing = array_map(function ($str) {
            return NodeRef::fromString($str);
        }, $missing);
        $response->addToSet('missing_node_refs', $missing);

        return $response;
    }

    /**
     * @param GetNodeBatchRequest $request
     * @param Pbjx                $pbjx
     *
     * @return GetNodeBatchResponse
     */
    protected function createGetNodeBatchResponse(GetNodeBatchRequest $request, Pbjx $pbjx): GetNodeBatchResponse
    {
        /** @var GetNodeBatchResponse $response */
        $response = $this->createResponseFromRequest($request);
        return $response;
    }
}
