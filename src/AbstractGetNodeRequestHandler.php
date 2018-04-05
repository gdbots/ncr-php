<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\Mixin\GetNodeRequest\GetNodeRequest;
use Gdbots\Schemas\Ncr\Mixin\GetNodeResponse\GetNodeResponse;

abstract class AbstractGetNodeRequestHandler extends AbstractRequestHandler
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
     * @param GetNodeRequest $request
     * @param Pbjx           $pbjx
     *
     * @return GetNodeResponse
     *
     * @throws NodeNotFound
     */
    protected function handle(GetNodeRequest $request, Pbjx $pbjx): GetNodeResponse
    {
        $context = $this->createNcrContext($request);

        if ($request->has('node_ref')) {
            $node = $this->ncr->getNode(
                $request->get('node_ref'),
                $request->get('consistent_read'),
                $context
            );
        } elseif ($request->has('slug')) {
            $qname = SchemaQName::fromString($request->get('qname'));
            $query = IndexQueryBuilder::create($qname, 'slug', $request->get('slug'))
                ->setCount(1)
                ->build();
            $result = $this->ncr->findNodeRefs($query, $context);
            if (!$result->count()) {
                throw new NodeNotFound("Unable to locate {$qname->getMessage()}.");
            }

            $node = $this->ncr->getNode(
                $result->getNodeRefs()[0],
                $request->get('consistent_read'),
                $context
            );
        } else {
            throw new NodeNotFound('No method to locate node.');
        }

        $this->assertIsNodeSupported($node);
        return $this->createGetNodeResponse($request, $pbjx)->set('node', $node);
    }

    /**
     * @param GetNodeRequest $request
     * @param Pbjx           $pbjx
     *
     * @return GetNodeResponse
     */
    protected function createGetNodeResponse(GetNodeRequest $request, Pbjx $pbjx): GetNodeResponse
    {
        /** @var GetNodeResponse $response */
        $response = $this->createResponseFromRequest($request);
        return $response;
    }
}
