<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Document;
use Elastica\Index;
use Elastica\Search;
use Elastica\Type;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Common\Util\NumberUtils;
use Gdbots\Ncr\Exception\SearchOperationFailed;
use Gdbots\Ncr\NcrSearch;
use Gdbots\Pbj\Marshaler\Elastica\DocumentMarshaler;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\Schema;
use Gdbots\Pbj\SchemaQName;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ElasticaNcrSearch implements NcrSearch
{
    /** @var ClientManager */
    protected $clientManager;

    /** @var LoggerInterface */
    protected $logger;

    /** @var IndexManager */
    protected $indexManager;

    /** @var DocumentMarshaler */
    protected $marshaler;

    /** @var QueryFactory */
    protected $queryFactory;

    /**
     * Used to limit the amount of time a query can take.
     *
     * @var string
     */
    protected $timeout;

    /**
     * @param ClientManager   $clientManager
     * @param IndexManager    $indexManager
     * @param LoggerInterface $logger
     * @param string          $timeout
     */
    public function __construct(
        ClientManager $clientManager,
        IndexManager $indexManager,
        ?LoggerInterface $logger = null,
        ?string $timeout = null
    ) {
        $this->clientManager = $clientManager;
        $this->indexManager = $indexManager;
        $this->logger = $logger ?: new NullLogger();
        $this->timeout = $timeout ?: '100ms';
        $this->marshaler = new DocumentMarshaler();
    }

    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $client = $this->getClientForWrite($context);
        $index = $this->indexManager->createIndex($client, $qname, $context);
    }

    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $client = $this->getClientForWrite($context);
        $index = new Index($client, $this->indexManager->getIndexName($qname, $context));

        $connection = $client->getConnection();
        $url = "https://{$connection->getHost()}:{$connection->getPort()}/{$index->getName()}";

        $result = <<<TEXT

Service:      ElasticSearch
Index Name:   {$index->getName()}
Documents:    {$index->count()}
Index Stats:  curl "{$url}/_stats?pretty=true"
Mappings:     curl "{$url}/_mapping?pretty=true"

TEXT;

        return $result;
    }

    public function indexNodes(array $nodes, array $context = []): void
    {
        if (empty($nodes)) {
            return;
        }

        $client = $this->getClientForWrite($context);
        $documents = [];
        $refresh = (bool)($context['consistent_write'] ?? false);

        foreach ($nodes as $node) {
            /** @var Schema $schema */
            $schema = $node::schema();
            $qname = $schema->getQName();
            $nodeRef = NodeRef::fromNode($node);
            $indexName = null;
            $typeName = null;

            try {
                $indexName = $this->indexManager->getIndexName($qname, $context);
                $typeName = $this->indexManager->getTypeName($qname);
                $document = $this->marshaler->marshal($node)
                    ->setId($node->get('_id')->toString())
                    ->remove('_id')// the "_id" field must not exist in the source as well
                    ->setType($typeName)
                    ->setIndex($indexName)
                    ->setRefresh($refresh);
                $this->indexManager->getNodeMapper($qname)->beforeIndex($document, $node);
                $documents[] = $document;
            } catch (\Throwable $e) {
                $message = sprintf(
                    '%s while adding node [{node_ref}] to batch index request ' .
                    'into ElasticSearch [{index_name}/{type_name}].',
                    ClassUtils::getShortName($e)
                );

                $this->logger->error($message, [
                    'exception'  => $e,
                    'index_name' => $indexName,
                    'type_name'  => $typeName,
                    'node_ref'   => $nodeRef->toString(),
                    'node'       => $node->toArray(),
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
                    ClassUtils::getShortName($e),
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

        $client = $this->getClientForWrite($context);
        $documents = [];
        $refresh = (bool)($options['consistent_write'] ?? false);

        /** @var NodeRef $nodeRef */
        foreach ($nodeRefs as $nodeRef) {
            $qname = $nodeRef->getQName();
            $indexName = null;
            $typeName = null;

            try {
                $indexName = $this->indexManager->getIndexName($qname, $context);
                $typeName = $this->indexManager->getTypeName($qname);
                $documents[] = (new Document())
                    ->setId((string)$nodeRef->getId())
                    ->setType($typeName)
                    ->setIndex($indexName)
                    ->setRefresh($refresh);
            } catch (\Throwable $e) {
                $message = sprintf(
                    '%s while adding node [{node_ref}] to batch delete request ' .
                    'from ElasticSearch [{index_name}/{type_name}].',
                    ClassUtils::getShortName($e)
                );

                $this->logger->error($message, [
                    'exception'  => $e,
                    'index_name' => $indexName,
                    'type_name'  => $typeName,
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
                    ClassUtils::getShortName($e),
                    $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    public function searchNodes(Message $request, ParsedQuery $parsedQuery, Message $response, array $qnames = [], array $context = []): void
    {
        $search = new Search($this->getClientForRead($context));

        if (empty($qnames)) {
            $search->addIndex($this->indexManager->getIndexPrefix($context) . '*');
        } else {
            foreach ($qnames as $qname) {
                $search->addIndex($this->indexManager->getIndexName($qname, $context));
                $search->addType($this->indexManager->getTypeName($qname));
            }
        }

        $page = $request->has('cursor') ? 1 : $request->get('page');
        $perPage = $request->get('count');
        $offset = ($page - 1) * $perPage;
        $offset = NumberUtils::bound($offset, 0, 10000);
        $options = [
            Search::OPTION_TIMEOUT                   => $this->timeout,
            Search::OPTION_FROM                      => $offset,
            Search::OPTION_SIZE                      => $perPage,
            Search::OPTION_SEARCH_IGNORE_UNAVAILABLE => true,
        ];

        try {
            $results = $search
                ->setOptionsAndQuery($options, $this->getQueryFactory()->create($request, $parsedQuery))
                ->search();
        } catch (\Throwable $e) {
            $this->logger->error(
                'ElasticSearch query [{query}] failed.',
                [
                    'exception'  => $e,
                    'pbj_schema' => $request->schema()->getId()->toString(),
                    'pbj'        => $request->toArray(),
                    'query'      => $request->get('q'),
                ]
            );

            throw new SearchOperationFailed(
                sprintf(
                    'ElasticSearch query [%s] failed with message: %s',
                    $request->get('q'),
                    ClassUtils::getShortName($e) . '::' . $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }

        $nodes = [];
        foreach ($results->getResults() as $result) {
            try {
                $source = $result->getSource();
                $source['_id'] = (string)$result->getId();
                $nodes[] = $this->marshaler->unmarshal($source);
            } catch (\Throwable $e) {
                $this->logger->error(
                    'Source returned from ElasticSearch could not be unmarshaled.',
                    ['exception' => $e, 'hit' => $result->getHit()]
                );
            }
        }

        $response
            ->set('total', $results->getTotalHits())
            ->set('has_more', ($offset + $perPage) < $results->getTotalHits() && $offset < 10000)
            ->set('time_taken', (int)($results->getResponse()->getQueryTime() * 1000))
            ->set('max_score', (float)$results->getMaxScore())
            ->addToList('nodes', $nodes);
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

    /**
     * @return QueryFactory
     */
    final protected function getQueryFactory(): QueryFactory
    {
        if (null === $this->queryFactory) {
            $this->queryFactory = $this->doGetQueryFactory();
        }

        return $this->queryFactory;
    }

    /**
     * @return QueryFactory
     */
    protected function doGetQueryFactory(): QueryFactory
    {
        return new QueryFactory();
    }
}
