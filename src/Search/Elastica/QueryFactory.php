<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Query;
use Elastica\Query\AbstractQuery;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\DateUtil;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\QueryParser\Builder\ElasticaQueryBuilder;
use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\Enum\ComparisonOperator;
use Gdbots\QueryParser\Node\Field;
use Gdbots\QueryParser\Node\Numbr;
use Gdbots\QueryParser\Node\Word;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Publishable\PublishableV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequestV1Mixin;

class QueryFactory
{
    /**
     * @param Message       $request     Search request containing pagination, date filters, etc.
     * @param ParsedQuery   $parsedQuery Parsed version of the search query (the "q" field of the request).
     * @param SchemaQName[] $qnames      An array of qnames that the search should limit its search to.
     *
     * @return Query
     */
    final public function create(Message $request, ParsedQuery $parsedQuery, array $qnames = []): Query
    {
        $this->applyDateFilters($request, $parsedQuery);
        $this->applyStatus($request, $parsedQuery);

        $method = $request::schema()->getHandlerMethodName(false, 'for');
        if (is_callable([$this, $method])) {
            $query = $this->$method($request, $parsedQuery, $qnames);
        } else {
            $query = $this->forSearchNodesRequest($request, $parsedQuery, $qnames);
        }

        return Query::create($query);
    }

    protected function forSearchNodesRequest(Message $request, ParsedQuery $parsedQuery, array $qnames): AbstractQuery
    {
        $builder = new ElasticaQueryBuilder();
        $builder->setDefaultFieldName(MappingBuilder::ALL_FIELD)->addParsedQuery($parsedQuery);
        $query = $builder->getBoolQuery();

        $this->filterDates($request, $query);
        $this->filterQNames($request, $query, $qnames);
        $this->filterStatuses($request, $query);

        return $query;
    }

    protected function applyDateFilters(Message $request, ParsedQuery $parsedQuery): void
    {
        $required = BoolOperator::REQUIRED();

        $dateFilters = [
            [
                'query'    => SearchNodesRequestV1Mixin::CREATED_AFTER_FIELD,
                'field'    => NodeV1Mixin::CREATED_AT_FIELD,
                'operator' => ComparisonOperator::GT(),
            ],
            [
                'query'    => SearchNodesRequestV1Mixin::CREATED_BEFORE_FIELD,
                'field'    => NodeV1Mixin::CREATED_AT_FIELD,
                'operator' => ComparisonOperator::LT(),
            ],
            [
                'query'    => SearchNodesRequestV1Mixin::UPDATED_AFTER_FIELD,
                'field'    => NodeV1Mixin::UPDATED_AT_FIELD,
                'operator' => ComparisonOperator::GT(),
            ],
            [
                'query'    => SearchNodesRequestV1Mixin::UPDATED_BEFORE_FIELD,
                'field'    => NodeV1Mixin::UPDATED_AT_FIELD,
                'operator' => ComparisonOperator::LT(),
            ],
        ];

        foreach ($dateFilters as $f) {
            if ($request->has($f['query'])) {
                $parsedQuery->addNode(
                    new Field(
                        $f['field'],
                        new Numbr(
                            (float)Microtime::fromDateTime($request->get($f['query']))->toString(),
                            $f['operator']
                        ),
                        $required
                    )
                );
            }
        }
    }

    protected function applyStatus(Message $request, ParsedQuery $parsedQuery): void
    {
        if (!$request->has(SearchNodesRequestV1Mixin::STATUS_FIELD)) {
            return;
        }

        $required = BoolOperator::REQUIRED();
        $parsedQuery->addNode(new Field(
            NodeV1Mixin::STATUS_FIELD,
            new Word((string)$request->get(SearchNodesRequestV1Mixin::STATUS_FIELD), $required),
            $required
        ));
    }

    /**
     * Add any dates as filters directly to the elastica search query.
     * This is different from applyDateFilters because ISO dates are not supported
     * by the query parser.
     *
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
     *
     * @param Message         $request
     * @param Query\BoolQuery $query
     */
    protected function filterDates(Message $request, Query\BoolQuery $query): void
    {
        $dateFilters = [
            [
                'query'    => SearchNodesRequestV1Mixin::PUBLISHED_AFTER_FIELD,
                'field'    => PublishableV1Mixin::PUBLISHED_AT_FIELD,
                'operator' => ComparisonOperator::GT,
            ],
            [
                'query'    => SearchNodesRequestV1Mixin::PUBLISHED_BEFORE_FIELD,
                'field'    => PublishableV1Mixin::PUBLISHED_AT_FIELD,
                'operator' => ComparisonOperator::LT,
            ],
        ];

        foreach ($dateFilters as $f) {
            if ($request->has($f['query'])) {
                $query->addFilter(new Query\Range($f['field'], [
                    $f['operator'] => $request->get($f['query'])->format(DateUtil::ISO8601_ZULU),
                ]));
            }
        }
    }

    /**
     * Add the "types" into one terms query as it's more efficient.
     *
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
     *
     * @param Message         $request
     * @param Query\BoolQuery $query
     * @param SchemaQName[]   $qnames
     */
    protected function filterQNames(Message $request, Query\BoolQuery $query, array $qnames): void
    {
        if (empty($qnames)) {
            return;
        }

        $types = array_map(fn(SchemaQName $qname) => $qname->getMessage(), $qnames);
        $query->addFilter(new Query\Terms(MappingBuilder::TYPE_FIELD, $types));
    }

    /**
     * Add the "statuses" into one terms query as it's more efficient.
     *
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
     *
     * @param Message         $request
     * @param Query\BoolQuery $query
     */
    protected function filterStatuses(Message $request, Query\BoolQuery $query): void
    {
        if (!$request->has(SearchNodesRequestV1Mixin::STATUSES_FIELD)) {
            return;
        }

        $statuses = array_map('strval', $request->get(SearchNodesRequestV1Mixin::STATUSES_FIELD));
        $query->addFilter(new Query\Terms(NodeV1Mixin::STATUS_FIELD, $statuses));
    }
}
