<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Query;
use Elastica\Query\AbstractQuery;
use Gdbots\Pbj\WellKnown\Microtime;
use Gdbots\QueryParser\Builder\ElasticaQueryBuilder;
use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\Enum\ComparisonOperator;
use Gdbots\QueryParser\Node\Field;
use Gdbots\QueryParser\Node\Numbr;
use Gdbots\QueryParser\Node\Word;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequest;

class QueryFactory
{
    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     *
     * @return Query
     */
    final public function create(SearchNodesRequest $request, ParsedQuery $parsedQuery): Query
    {
        $this->applyStatus($request, $parsedQuery);
        $this->applyDateFilters($request, $parsedQuery);

        $method = 'for' . ucfirst($request::schema()->getHandlerMethodName(false));
        if (is_callable([$this, $method])) {
            $query = $this->$method($request, $parsedQuery);
        } else {
            $query = $this->forSearchNodesRequest($request, $parsedQuery);
        }

        return Query::create($query);
    }

    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     */
    protected function applyStatus(SearchNodesRequest $request, ParsedQuery $parsedQuery): void
    {
        if (!$request->has('status')) {
            return;
        }

        $required = BoolOperator::REQUIRED();
        $parsedQuery->addNode(new Field('status', new Word((string)$request->get('status'), $required), $required));
    }

    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     */
    protected function applyDateFilters(SearchNodesRequest $request, ParsedQuery $parsedQuery): void
    {
        $required = BoolOperator::REQUIRED();

        $dateFilters = [
            ['query' => 'created_after', 'field' => 'created_at', 'operator' => ComparisonOperator::GT()],
            ['query' => 'created_before', 'field' => 'created_at', 'operator' => ComparisonOperator::LT()],
            ['query' => 'updated_after', 'field' => 'updated_at', 'operator' => ComparisonOperator::GT()],
            ['query' => 'updated_before', 'field' => 'updated_at', 'operator' => ComparisonOperator::LT()],
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

    /**
     * @param SearchNodesRequest $request
     * @param ParsedQuery        $parsedQuery
     *
     * @return AbstractQuery
     */
    protected function forSearchNodesRequest(SearchNodesRequest $request, ParsedQuery $parsedQuery): AbstractQuery
    {
        $builder = new ElasticaQueryBuilder();
        $builder->setDefaultFieldName('_all')->addParsedQuery($parsedQuery);
        return $builder->getBoolQuery();
    }
}
