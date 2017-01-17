<?php
declare(strict_types = 1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Index;
use Elastica\Type;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Exception\SearchOperationFailed;
use Gdbots\Ncr\NcrSearch;
use Gdbots\Pbj\Marshaler\Elastica\DocumentMarshaler;
use Gdbots\Pbj\Schema;
use Gdbots\Pbj\SchemaQName;
use Gdbots\QueryParser\Builder\ElasticaQueryBuilder;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequest;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesResponse\SearchNodesResponse;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ElasticaNcrSearch implements NcrSearch
{
    /** @var ClientManager */
    protected $clientManager;

    /** @var IndexManager */
    protected $indexManager;

    /** @var LoggerInterface */
    protected $logger;

    /** @var DocumentMarshaler */
    protected $marshaler;

    /** @var ElasticaQueryBuilder */
    protected $queryBuilder;

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
        $this->queryBuilder = new ElasticaQueryBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $client = $this->getClient($context);
        $index = $this->indexManager->createIndex($client, $qname, $context);
        $this->indexManager->createType($index, $qname);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $client = $this->getClient($context);
        $index = new Index($client, $this->indexManager->getIndexName($qname, $context));
        $type = new Type($index, $this->indexManager->getTypeName($qname));

        $connection = $client->getConnection();
        $url = "http://{$connection->getHost()}:{$connection->getPort()}/{$index->getName()}";

        $result = <<<TEXT

Service:      ElasticSearch
Index Name:   {$index->getName()}
Type Name:    {$type->getName()}
Documents:    {$type->count()}
Index Stats:  curl "{$url}/_stats?pretty=1"
Type Mapping: curl "{$url}/{$type->getName()}/_mapping?pretty=1"

TEXT;

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function indexNodes(array $nodes, array $context = []): void
    {
        if (empty($nodes)) {
            return;
        }

        $client = $this->getClient($context);
        $documents = [];

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
                    ->setType($typeName)
                    ->setIndex($indexName);

                $this->indexManager->getNodeMapper($qname)->beforeIndex($document, $node);
                $documents[] = $document;
            } catch (\Exception $e) {
                $message = sprintf(
                    '%s::Failed to add node [{node_ref}] to batch index request ' .
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
        } catch (\Exception $e) {
            throw new SearchOperationFailed(
                sprintf(
                    '%s::Failed to index batch into ElasticSearch with message: %s',
                    ClassUtils::getShortName($e),
                    $e->getMessage()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function searchNodes(
        SearchNodesRequest $request,
        ParsedQuery $parsedQuery,
        SearchNodesResponse $response,
        array $qnames = [],
        array $context = []
    ): SearchNodesResponse {
        // TODO: Implement searchNodes() method.
    }

    /**
     * Override to provide your own logic which determines which client to use
     * based on the context provided.  Typically used for multi-tenant applications.
     *
     * @param array $context
     *
     * @return Client
     */
    protected function getClient(array $context): Client
    {
        // override to provide your own logic for client creation.
        return $this->clientManager->getClient();
    }
}
