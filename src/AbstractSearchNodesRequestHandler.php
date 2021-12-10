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
            $request->get('parsed_query_json', '{}'),
            true
        ));

        $prohibited = BoolOperator::PROHIBITED;

        // if status is not specified in some way, default to not
        // showing any deleted nodes.
        if (!$request->has('status')
            && !$request->has('statuses')
            && !$request->isInSet('fields_used', 'status')
        ) {
            $parsedQuery->addNode(
                new Field('status', new Word(NodeStatus::DELETED->value, $prohibited), $prohibited)
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
