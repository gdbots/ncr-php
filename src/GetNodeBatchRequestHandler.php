<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\RequestHandlerTrait;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequest;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponse;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchResponseV1;

/**
 * This handler SHOULD only be used internally.
 * Clients should not be able to make this request as it
 * allows for access to batch request any kind of node.
 */
final class GetNodeBatchRequestHandler implements RequestHandler
{
    use RequestHandlerTrait;

    /** @var Ncr */
    private $ncr;

    /**
     * @param Ncr $ncr
     */
    public function __construct(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }

    /**
     * @param GetNodeBatchRequest $request
     *
     * @return GetNodeBatchResponse
     */
    protected function handle(GetNodeBatchRequest $request): GetNodeBatchResponse
    {
        $nodeRefs = $request->get('node_refs');
        $response = GetNodeBatchResponseV1::create();

        if (empty($nodeRefs)) {
            return $response;
        }

        $nodes = $this->ncr->getNodes(
            $nodeRefs,
            $request->get('consistent_read'),
            $request->get('context', [])
        );

        foreach ($nodes as $nodeRef => $node) {
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
     * {@inheritdoc}
     */
    public static function handlesCuries(): array
    {
        return [
            GetNodeBatchRequestV1::schema()->getCurie(),
        ];
    }
}
