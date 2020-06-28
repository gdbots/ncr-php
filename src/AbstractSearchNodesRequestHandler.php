<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\Node\Field;
use Gdbots\QueryParser\Node\Word;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequestV1Mixin;

abstract class AbstractSearchNodesRequestHandler implements RequestHandler
{
    protected NcrSearch $ncrSearch;

    public function __construct(NcrSearch $ncrSearch)
    {
        $this->ncrSearch = $ncrSearch;
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        $response = $this->createSearchNodesResponse($request, $pbjx);
        $parsedQuery = ParsedQuery::fromArray(json_decode(
            $request->get(SearchNodesRequestV1Mixin::PARSED_QUERY_JSON_FIELD, '{}'),
            true
        ));

        $prohibited = BoolOperator::PROHIBITED();

        // if status is not specified in some way, default to not
        // showing any deleted nodes.
        if (!$request->has(SearchNodesRequestV1Mixin::STATUS_FIELD)
            && !$request->has(SearchNodesRequestV1Mixin::STATUSES_FIELD)
            && !$request->isInSet(
                SearchNodesRequestV1Mixin::FIELDS_USED_FIELD,
                SearchNodesRequestV1Mixin::STATUS_FIELD
            )
        ) {
            $parsedQuery->addNode(
                new Field(
                    SearchNodesRequestV1Mixin::STATUS_FIELD,
                    new Word(NodeStatus::DELETED, $prohibited),
                    $prohibited
                )
            );
        }

        $qnames = $this->createQNamesForSearchNodes($request, $parsedQuery);
        $context = ['causator' => $request];
        $this->beforeSearchNodes($request, $parsedQuery);
        $this->ncrSearch->searchNodes($request, $parsedQuery, $response, $qnames, $context);
        return $response;
    }

    protected function beforeSearchNodes(Message $request, ParsedQuery $parsedQuery): void
    {
        // override to customize the parsed query before search nodes runs.
    }

    /**
     * @param Message     $request
     * @param ParsedQuery $parsedQuery
     *
     * @return SchemaQName[]
     */
    protected function createQNamesForSearchNodes(Message $request, ParsedQuery $parsedQuery): array
    {
        $curie = $request::schema()->getCurie();
        $vendor = MessageResolver::getDefaultVendor();
        // converts search-articles-request to "article"
        // converts search-categories-request to "category"
        $label = str_replace(['search-', 'ies-request', 's-request'], ['', 'y', ''], $curie->getMessage());

        // todo: use an inflector here for better default qname factory
        if ('people-request' === $label) {
            $label = 'person';
        }

        return [SchemaQName::fromString("{$vendor}:{$label}")];
    }

    abstract protected function createSearchNodesResponse(Message $request, Pbjx $pbjx): Message;
}
