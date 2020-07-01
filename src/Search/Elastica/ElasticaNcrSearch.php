<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Document;
use Elastica\Index;
use Elastica\Search;
use Gdbots\Ncr\Exception\SearchOperationFailed;
use Gdbots\Ncr\NcrSearch;
use Gdbots\Pbj\Marshaler\Elastica\DocumentMarshaler;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\Util\DateUtil;
use Gdbots\Pbj\Util\NumberUtil;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Event\EnrichContextEvent;
use Gdbots\Pbjx\PbjxEvents;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequestV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesResponse\SearchNodesResponseV1Mixin;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\EventDispatcher\EventDispatcher;

class ElasticaNcrSearch implements NcrSearch
{
    protected ClientManager $clientManager;
    protected EventDispatcher $dispatcher;
    protected IndexManager $indexManager;
    protected LoggerInterface $logger;
    protected DocumentMarshaler $marshaler;
    protected ?QueryFactory $queryFactory = null;

    /**
     * Used to limit the amount of time a query can take.
     *
     * @var string
     */
    protected string $timeout;

    public function __construct(
        ClientManager $clientManager,
        EventDispatcher $dispatcher,
        IndexManager $indexManager,
        ?LoggerInterface $logger = null,
        ?string $timeout = null
    ) {
        $this->clientManager = $clientManager;
        $this->dispatcher = $dispatcher;
        $this->indexManager = $indexManager;
        $this->logger = $logger ?: new NullLogger();
        $this->timeout = $timeout ?: '100ms';
        $this->marshaler = new DocumentMarshaler();
    }

    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $client = $this->getClientForWrite($context);
        $this->indexManager->createIndex($client, $qname, $context);
    }

    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $client = $this->getClientForWrite($context);
        $index = new Index($client, $this->indexManager->getIndexName($qname, $context));

        $connection = $client->getConnection();
        $url = "https://{$connection->getHost()}:{$connection->getPort()}/{$index->getName()}";

        $result = <<<TEXT

Service:     ElasticSearch
Index Name:  {$index->getName()}
Documents:   {$index->count()}
Index Stats: curl "{$url}/_stats?pretty=true"
Mappings:    curl "{$url}/_mapping?pretty=true"

TEXT;

        return $result;
    }

    public function indexNodes(array $nodes, array $context = []): void
    {
        if (empty($nodes)) {
            return;
        }

        $context = $this->enrichContext(__FUNCTION__, $context);
        $refresh = filter_var($context['consistent_write'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $client = $this->getClientForWrite($context);
        $this->marshaler->skipValidation(false);
        $documents = [];

        /** @var Message $node */
        foreach ($nodes as $node) {
            $nodeRef = $node->generateNodeRef();
            $qname = $nodeRef->getQName();
            $indexName = null;

            try {
                $indexName = $this->indexManager->getIndexName($qname, $context);
                $document = $this->marshaler->marshal($node)
                    ->setId($nodeRef->toString())
                    ->remove(NodeV1Mixin::_ID_FIELD) // the "_id" field must not exist in the source as well
                    ->set(MappingBuilder::TYPE_FIELD, $nodeRef->getLabel())
                    ->set(
                        IndexManager::CREATED_AT_ISO_FIELD_NAME,
                        $node->get(NodeV1Mixin::CREATED_AT_FIELD)->toDateTime()->format(DateUtil::ISO8601_ZULU)
                    )
                    ->setIndex($indexName)
                    ->setRefresh($refresh);
                $this->indexManager->getNodeMapper($qname)->beforeIndex($document, $node);
                $documents[] = $document;
            } catch (\Throwable $e) {
                $message = sprintf(
                    '%s while adding node [{node_ref}] to batch index request ' .
                    'into ElasticSearch [{index_name}].',
                    ClassUtil::getShortName($e)
                );

                $this->logger->error($message, [
                    'exception'  => $e,
                    'index_name' => $indexName,
                    'node_ref'   => $nodeRef->toString(),
                    'pbj'        => $node->toArray(),
                ]);
            }
        }

        if (empty($documents)) {
            return;
        }

        try {
            $response = $client->addDocuments($documents);
            if (!$response->isOk()) {
                throw new \Exception($response->getStatus() . '::' . $response->getError());
            }
        } catch (\Throwable $e) {
            throw new SearchOperationFailed(
                sprintf(
                    '%s while indexing batch into ElasticSearch with message: %s',
                    ClassUtil::getShortName($e),
                    $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    public function deleteNodes(array $nodeRefs, array $context = []): void
    {
        if (empty($nodeRefs)) {
            return;
        }

        $context = $this->enrichContext(__FUNCTION__, $context);
        $refresh = filter_var($context['consistent_write'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $client = $this->getClientForWrite($context);
        $documents = [];

        /** @var NodeRef $nodeRef */
        foreach ($nodeRefs as $nodeRef) {
            $qname = $nodeRef->getQName();
            $indexName = null;

            try {
                $indexName = $this->indexManager->getIndexName($qname, $context);
                $documents[] = (new Document())
                    ->setId($nodeRef->toString())
                    ->set(MappingBuilder::TYPE_FIELD, $nodeRef->getLabel())
                    ->setIndex($indexName)
                    ->setRefresh($refresh);
            } catch (\Throwable $e) {
                $message = sprintf(
                    '%s while adding node [{node_ref}] to batch delete request ' .
                    'from ElasticSearch [{index_name}].',
                    ClassUtil::getShortName($e)
                );

                $this->logger->error($message, [
                    'exception'  => $e,
                    'index_name' => $indexName,
                    'node_ref'   => $nodeRef->toString(),
                ]);
            }
        }

        if (empty($documents)) {
            return;
        }

        try {
            $response = $client->deleteDocuments($documents);
            if (!$response->isOk()) {
                throw new \Exception($response->getStatus() . '::' . $response->getError());
            }
        } catch (\Throwable $e) {
            throw new SearchOperationFailed(
                sprintf(
                    '%s while deleting batch from ElasticSearch with message: %s',
                    ClassUtil::getShortName($e),
                    $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    public function searchNodes(Message $request, ParsedQuery $parsedQuery, Message $response, array $qnames = [], array $context = []): void
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $skipValidation = filter_var($context['skip_validation'] ?? true, FILTER_VALIDATE_BOOLEAN);
        $search = new Search($this->getClientForRead($context));

        if (empty($qnames)) {
            $search->addIndex($this->indexManager->getIndexPrefix($context) . '*');
        } else {
            foreach ($qnames as $qname) {
                $search->addIndex($this->indexManager->getIndexName($qname, $context));
            }
        }

        $page = $request->has(SearchNodesRequestV1Mixin::CURSOR_FIELD) ? 1 : $request->get(SearchNodesRequestV1Mixin::PAGE_FIELD);
        $perPage = $request->get(SearchNodesRequestV1Mixin::COUNT_FIELD);
        $offset = ($page - 1) * $perPage;
        $offset = NumberUtil::bound($offset, 0, 10000);
        $options = [
            Search::OPTION_TIMEOUT                   => $this->timeout,
            Search::OPTION_FROM                      => $offset,
            Search::OPTION_SIZE                      => $perPage,
            Search::OPTION_SEARCH_IGNORE_UNAVAILABLE => true,
        ];

        try {
            $results = $search
                ->setOptionsAndQuery($options, $this->getQueryFactory()->create($request, $parsedQuery, $qnames))
                ->search();
        } catch (\Throwable $e) {
            $this->logger->error(
                'ElasticSearch query [{query}] failed.',
                [
                    'exception'  => $e,
                    'pbj_schema' => $request->schema()->getId()->toString(),
                    'pbj'        => $request->toArray(),
                    'query'      => $request->get(SearchNodesRequestV1Mixin::Q_FIELD),
                ]
            );

            throw new SearchOperationFailed(
                sprintf(
                    'ElasticSearch query [%s] failed with message: %s',
                    $request->get(SearchNodesRequestV1Mixin::Q_FIELD),
                    ClassUtil::getShortName($e) . '::' . $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }

        $nodes = [];
        $this->marshaler->skipValidation($skipValidation);
        foreach ($results->getResults() as $result) {
            try {
                $source = $result->getSource();
                [, , $id] = explode(':', (string)$result->getId(), 3);
                $source['_id'] = $id;
                $nodes[] = $this->marshaler->unmarshal($source);
            } catch (\Throwable $e) {
                $this->logger->error(
                    'Source returned from ElasticSearch could not be unmarshaled.',
                    ['exception' => $e, 'hit' => $result->getHit()]
                );
            }
        }
        $this->marshaler->skipValidation(false);

        $response
            ->set(SearchNodesResponseV1Mixin::TOTAL_FIELD, $results->getTotalHits())
            ->set(SearchNodesResponseV1Mixin::HAS_MORE_FIELD, ($offset + $perPage) < $results->getTotalHits() && $offset < 10000)
            ->set(SearchNodesResponseV1Mixin::TIME_TAKEN_FIELD, (int)($results->getResponse()->getQueryTime() * 1000))
            ->set(SearchNodesResponseV1Mixin::MAX_SCORE_FIELD, (float)$results->getMaxScore())
            ->addToList(SearchNodesResponseV1Mixin::NODES_FIELD, $nodes);
    }

    /**
     * Override to provide your own logic which determines which client
     * to use for a READ operation based on the context provided.
     * Typically used for multi-tenant applications.
     *
     * @param array $context
     *
     * @return Client
     */
    protected function getClientForRead(array $context): Client
    {
        return $this->getClientForWrite($context);
    }

    /**
     * Override to provide your own logic which determines which client
     * to use for a WRITE operation based on the context provided.
     * Typically used for multi-tenant applications.
     *
     * @param array $context
     *
     * @return Client
     */
    protected function getClientForWrite(array $context): Client
    {
        return $this->clientManager->getClient($context['cluster'] ?? 'default');
    }

    protected function enrichContext(string $operation, array $context): array
    {
        if (isset($context['already_enriched'])) {
            return $context;
        }

        $event = new EnrichContextEvent('ncr_search', $operation, $context);
        $context = $this->dispatcher->dispatch($event, PbjxEvents::ENRICH_CONTEXT)->all();
        $context['already_enriched'] = true;
        return $context;
    }

    final protected function getQueryFactory(): QueryFactory
    {
        if (null === $this->queryFactory) {
            $this->queryFactory = $this->doGetQueryFactory();
        }

        return $this->queryFactory;
    }

    protected function doGetQueryFactory(): QueryFactory
    {
        return new QueryFactory();
    }
}
