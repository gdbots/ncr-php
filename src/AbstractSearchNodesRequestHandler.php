<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbjx\Pbjx;
use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\Node\Field;
use Gdbots\QueryParser\Node\Word;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequest;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesResponse\SearchNodesResponse;

abstract class AbstractSearchNodesRequestHandler extends AbstractRequestHandler
{
    /** @var NcrSearch */
    protected $ncrSearch;

    /**
     * @param NcrSearch $ncrSearch
     */
    public function __construct(NcrSearch $ncrSearch)
    {
        $this->ncrSearch = $ncrSearch;
    }

    /**
     * @param SearchNodesRequest $request
     * @param Pbjx               $pbjx
     *
     * @return SearchNodesResponse
     */
    protected function handle(SearchNodesRequest $request, Pbjx $pbjx): SearchNodesResponse
    {
        $response = $this->createSearchNodesResponse($request, $pbjx);
        $parsedQuery = ParsedQuery::fromArray(json_decode(
            $request->get('parsed_query_json', '{}'),
            true
        ));

        $prohibited = BoolOperator::PROHIBITED();

        // if status is not specified in some way, default to not
        // showing any deleted nodes.
        if (!$request->has('status')
            && !$request->has('statuses')
            && !$request->isInSet('fields_used', 'status')
        ) {
            $parsedQuery->addNode(
                new Field('status', new Word(NodeStatus::DELETED, $prohibited), $prohibited)
            );
        }

        $qnames = $this->createQNamesForSearchNodes($request, $parsedQuery);
        $context = $this->createNcrSearchContext($request);

        $this->beforeSearchNodes($request, $parsedQuery);
        $this->ncrSearch->searchNodes($request, $parsedQuery, $response, $qnames, $context);
        return $response;
    }

    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     */
    protected function beforeSearchNodes(SearchNodesRequest $request, ParsedQuery $parsedQuery): void
    {
        // override to customize the parsed query before search nodes runs.
    }

    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     *
     * @return SchemaQName[]
     */
    protected function createQNamesForSearchNodes(SearchNodesRequest $request, ParsedQuery $parsedQuery): array
    {
        $curie = $request::schema()->getCurie();
        $vendor = $curie->getVendor();
        // convert search-articles-request to "article"
        $label = str_replace(['search-', 's-request'], '', $curie->getMessage());

        // todo: use an inflector here for better default qname factory
        if ('people' === $label) {
            $label = 'person';
        } elseif ('categories' === $label) {
            $label = 'category';
        }

        return [SchemaQName::fromString("{$vendor}:{$label}")];
    }

    /**
     * @param SearchNodesRequest $request
     * @param Pbjx               $pbjx
     *
     * @return SearchNodesResponse
     */
    protected function createSearchNodesResponse(SearchNodesRequest $request, Pbjx $pbjx): SearchNodesResponse
    {
        /** @var SearchNodesResponse $response */
        $response = $this->createResponseFromRequest($request);
        return $response;
    }
}
