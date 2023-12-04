<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\CommandPool;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Aws\ResultInterface;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\Exception\RepositoryIndexNotFound;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\Util\NumberUtil;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Event\EnrichContextEvent;
use Gdbots\Pbjx\PbjxEvents;
use Gdbots\Schemas\Pbjx\Enum\Code;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\EventDispatcher\EventDispatcher;

final class DynamoDbNcr implements Ncr
{
    private DynamoDbClient $client;
    private EventDispatcher $dispatcher;
    private TableManager $tableManager;
    private array $config;
    private LoggerInterface $logger;
    private ItemMarshaler $marshaler;
    private ?IndexQueryFilterProcessor $filterProcessor = null;

    public function __construct(
        DynamoDbClient $client,
        EventDispatcher $dispatcher,
        TableManager $tableManager,
        array $config = [],
        ?LoggerInterface $logger = null
    ) {
        // defaults
        $config += [
            'batch_size'  => 100,
            'concurrency' => 25,
        ];

        $config['batch_size'] = NumberUtil::bound($config['batch_size'], 2, 100);
        $config['concurrency'] = NumberUtil::bound($config['concurrency'], 1, 50);

        $this->client = $client;
        $this->dispatcher = $dispatcher;
        $this->tableManager = $tableManager;
        $this->config = $config;
        $this->logger = $logger ?: new NullLogger();
        $this->marshaler = new ItemMarshaler();
    }

    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($qname, $context);
        $this->tableManager->getNodeTable($qname)->create($this->client, $tableName);
    }

    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($qname, $context);
        return $this->tableManager->getNodeTable($qname)->describe($this->client, $tableName);
    }

    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $response = $this->client->getItem([
                'ConsistentRead'           => $consistent,
                'TableName'                => $tableName,
                'ProjectionExpression'     => '#node_ref',
                'ExpressionAttributeNames' => ['#node_ref' => NodeTable::HASH_KEY_NAME],
                'Key'                      => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Throwable $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    return false;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED->value;
                } else {
                    $code = Code::UNAVAILABLE->value;
                }
            } else {
                $errorName = ClassUtil::getShortName($e);
                $code = Code::INTERNAL->value;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while checking for [%s] in DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }

        return isset($response['Item']);
    }

    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Message
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName'      => $tableName,
                'Key'            => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Throwable $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    throw NodeNotFound::forNodeRef($nodeRef, $e);
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED->value;
                } else {
                    $code = Code::UNAVAILABLE->value;
                }
            } else {
                $errorName = ClassUtil::getShortName($e);
                $code = Code::INTERNAL->value;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while getting [%s] from DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }

        if (!isset($response['Item']) || empty($response['Item'])) {
            throw NodeNotFound::forNodeRef($nodeRef);
        }

        $skipValidation = filter_var($context['skip_validation'] ?? !$consistent, FILTER_VALIDATE_BOOLEAN);
        $this->marshaler->skipValidation($skipValidation);

        try {
            $node = $this->marshaler->unmarshal($response['Item']);
        } catch (\Throwable $e) {
            $this->logger->error(
                'Item returned from DynamoDb table [{table_name}] for [{node_ref}] could not be unmarshaled.',
                [
                    'exception'  => $e,
                    'item'       => $response['Item'],
                    'context'    => $context,
                    'table_name' => $tableName,
                    'node_ref'   => (string)$nodeRef,
                ]
            );

            $this->marshaler->skipValidation(false);
            throw NodeNotFound::forNodeRef($nodeRef, $e);
        }

        $this->marshaler->skipValidation(false);
        return $node;
    }

    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        $context = $this->enrichContext(__FUNCTION__, $context);

        if (empty($nodeRefs)) {
            return [];
        } elseif (count($nodeRefs) === 1) {
            try {
                $nodeRef = array_shift($nodeRefs);
                return [(string)$nodeRef => $this->getNode($nodeRef, $consistent, $context)];
            } catch (NodeNotFound $e) {
                return [];
            } catch (\Throwable $e) {
                throw $e;
            }
        }

        $batch = (new BatchGetItemRequest($this->client))
            ->batchSize($this->config['batch_size'])
            ->concurrency($this->config['concurrency'])
            ->consistentRead($consistent)
            ->onError(function (AwsException $e) use ($context) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                $this->logger->error(
                    sprintf('%s while processing BatchGetItemRequest.', $errorName),
                    ['exception' => $e, 'context' => $context]
                );
            });

        foreach ($nodeRefs as $nodeRef) {
            $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);
            $batch->addItemKey($tableName, [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]);
        }

        $skipValidation = filter_var($context['skip_validation'] ?? !$consistent, FILTER_VALIDATE_BOOLEAN);
        $this->marshaler->skipValidation($skipValidation);
        $nodes = [];

        foreach ($batch->getItems() as $item) {
            try {
                $nodeRef = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
                $nodes[$nodeRef->toString()] = $this->marshaler->unmarshal($item);
            } catch (\Throwable $e) {
                $this->logger->error(
                    'Item returned from DynamoDb table could not be unmarshaled.',
                    ['exception' => $e, 'item' => $item, 'context' => $context]
                );
            }
        }

        $this->marshaler->skipValidation(false);
        return $nodes;
    }

    public function putNode(Message $node, ?string $expectedEtag = null, array $context = []): void
    {
        $node->freeze();
        $nodeRef = $node->generateNodeRef();

        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);
        $table = $this->tableManager->getNodeTable($nodeRef->getQName());
        $this->marshaler->skipValidation(false);

        $params = ['TableName' => $tableName];
        if (null !== $expectedEtag) {
            $params['ExpressionAttributeValues'] = [':v_etag' => ['S' => (string)$expectedEtag]];
            $params['ConditionExpression'] = 'etag = :v_etag';
        }

        try {
            $item = $this->marshaler->marshal($node);
            $item[NodeTable::HASH_KEY_NAME] = ['S' => $nodeRef->toString()];
            $table->beforePutItem($item, $node);
            $params['Item'] = $item;
            $this->client->putItem($params);
        } catch (\Throwable $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                if ('ConditionalCheckFailedException' === $errorName) {
                    throw new OptimisticCheckFailed(
                        sprintf(
                            'NodeRef [%s] in DynamoDb table [%s] did not have expected etag [%s].',
                            $nodeRef,
                            $tableName,
                            $expectedEtag
                        ),
                        $e
                    );
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED->value;
                } else {
                    $code = Code::UNAVAILABLE->value;
                }
            } else {
                $errorName = ClassUtil::getShortName($e);
                $code = Code::INTERNAL->value;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while putting [%s] into DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }
    }

    public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $this->client->deleteItem([
                'TableName' => $tableName,
                'Key'       => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Throwable $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    // if it's already deleted, it's fine
                    return;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED->value;
                } else {
                    $code = Code::UNAVAILABLE->value;
                }
            } else {
                $errorName = ClassUtil::getShortName($e);
                $code = Code::INTERNAL->value;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s while deleting [%s] from DynamoDb table [%s].',
                    $errorName,
                    $nodeRef,
                    $tableName
                ),
                $code,
                $e
            );
        }
    }

    public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $tableName = $this->tableManager->getNodeTableName($query->getQName(), $context);
        $table = $this->tableManager->getNodeTable($query->getQName());

        if (!$table->hasIndex($query->getAlias())) {
            throw new RepositoryIndexNotFound(
                sprintf(
                    '%s::Index [%s] does not exist on table [%s].',
                    ClassUtil::getShortName($table),
                    $query->getAlias(),
                    $tableName
                )
            );
        }

        $params = $table->getIndex($query->getAlias())->createQuery($query);
        $params['TableName'] = $tableName;
        $unprocessedFilters = [];

        /*
         * When unprocessed_filters exist we must fetch the attributes from the
         * index so we can evaluate them in php before returning the result.
         */
        if (isset($params['unprocessed_filters'])) {
            $unprocessedFilters = $params['unprocessed_filters'];
            unset($params['unprocessed_filters']);
        } else {
            $params['ProjectionExpression'] = '#node_ref';
        }

        try {
            $response = $this->client->query($params);
        } catch (\Throwable $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtil::getShortName($e);
                if ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED->value;
                } else {
                    $code = Code::UNAVAILABLE->value;
                }
            } else {
                $errorName = ClassUtil::getShortName($e);
                $code = Code::INTERNAL->value;
            }

            throw new RepositoryOperationFailed(
                sprintf(
                    '%s on IndexQuery [%s] on DynamoDb table [%s].',
                    $errorName,
                    $query->getAlias(),
                    $tableName
                ),
                $code,
                $e
            );
        }

        if (!isset($response['Items']) || empty($response['Items'])) {
            return new IndexQueryResult($query);
        }

        if ($unprocessedFilters) {
            if (null === $this->filterProcessor) {
                $this->filterProcessor = new IndexQueryFilterProcessor();
            }
            $response['Items'] = $this->filterProcessor->filter($response['Items'], $unprocessedFilters);
        }

        $nodeRefs = [];
        foreach ($response['Items'] as $item) {
            try {
                $nodeRefs[] = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
            } catch (\Throwable $e) {
                $this->logger->error(
                    'NodeRef returned from IndexQuery [{index_alias}] on DynamoDb table [{table_name}] is invalid.',
                    [
                        'exception'   => $e,
                        'item'        => $item,
                        'context'     => $context,
                        'index_alias' => $query->getAlias(),
                        'index_query' => $query->toArray(),
                        'table_name'  => $tableName,
                    ]
                );
            }
        }

        if (isset($response['LastEvaluatedKey'])/* && $response['Count'] !== $response['ScannedCount']*/) {
            $nextCursor = base64_encode(json_encode($response['LastEvaluatedKey']));
        } else {
            $nextCursor = null;
        }

        return new IndexQueryResult($query, array_slice($nodeRefs, 0, $query->getCount()), $nextCursor);
    }

    public function pipeNodes(SchemaQName $qname, array $context = []): \Generator
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $context['node_refs_only'] = false;
        $generator = $this->doPipeNodes($qname, $context);

        /** @var \SplQueue $queue */
        $queue = $generator->current();

        do {
            $generator->next();
            while (!$queue->isEmpty()) {
                yield $queue->dequeue();
            }
        } while ($generator->valid());
    }

    public function pipeNodeRefs(SchemaQName $qname, array $context = []): \Generator
    {
        $context = $this->enrichContext(__FUNCTION__, $context);
        $context['node_refs_only'] = true;
        $generator = $this->doPipeNodes($qname, $context);

        /** @var \SplQueue $queue */
        $queue = $generator->current();

        do {
            $generator->next();
            while (!$queue->isEmpty()) {
                yield $queue->dequeue();
            }
        } while ($generator->valid());
    }

    private function doPipeNodes(SchemaQName $qname, array $context): \Generator
    {
        static $alreadyPiped = [];

        $tableName = $context['table_name'] ?? $this->tableManager->getNodeTableName($qname, $context);
        $reindexing = filter_var($context['reindexing'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $reindexAll = filter_var($context['reindex_all'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $skipErrors = filter_var($context['skip_errors'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $skipValidation = filter_var($context['skip_validation'] ?? true, FILTER_VALIDATE_BOOLEAN);
        $totalSegments = NumberUtil::bound($context['total_segments'] ?? 16, 1, 64);
        $concurrency = NumberUtil::bound($context['concurrency'] ?? 25, 1, 100);

        $this->marshaler->skipValidation($skipValidation);
        $queue = new \SplQueue();
        yield $queue;

        if ($reindexing && isset($alreadyPiped[$tableName])) {
            // multiple qnames can be in the same table.
            return;
        }

        $alreadyPiped[$tableName] = true;
        $params = [
            'ExpressionAttributeNames'  => [
                '#node_ref' => NodeTable::HASH_KEY_NAME,
            ],
            'ExpressionAttributeValues' => [
                ':v_qname' => ['S' => $qname->toString() . ':'],
            ],
        ];

        $filterExpressions = $reindexAll ? [] : ['begins_with(#node_ref, :v_qname)'];

        foreach (['s16', 's32', 's64', 's128', 's256'] as $shard) {
            if (isset($context[$shard])) {
                $params['ExpressionAttributeNames']["#{$shard}"] = "__{$shard}";
                $params['ExpressionAttributeValues'][":v_{$shard}"] = ['N' => (string)((int)$context[$shard])];
                $filterExpressions[] = "#{$shard} = :v_{$shard}";
            }
        }

        if (isset($context['status'])) {
            $params['ExpressionAttributeNames']['#status'] = 'status';
            $params['ExpressionAttributeValues'][':v_status'] = ['S' => (string)$context['status']];
            $filterExpressions[] = '#status = :v_status';
        }

        if ($context['node_refs_only']) {
            $params['ProjectionExpression'] = '#node_ref';
        }

        $params['TableName'] = $tableName;
        $params['TotalSegments'] = $totalSegments;
        if (!empty($filterExpressions)) {
            $params['FilterExpression'] = implode(' AND ', $filterExpressions);
        } else {
            unset($params['ExpressionAttributeNames']);
            unset($params['ExpressionAttributeValues']);
        }

        $pending = [];
        $iter2seg = ['prev' => [], 'next' => []];
        for ($segment = 0; $segment < $totalSegments; $segment++) {
            $params['Segment'] = $segment;
            $iter2seg['prev'][] = $segment;
            $pending[] = $this->client->getCommand('Scan', $params);
        }

        $fulfilled = function (ResultInterface $result, int|string $iterKey) use (
            $qname, $queue, $tableName, $context, $params, &$pending, &$iter2seg
        ) {
            $segment = $iter2seg['prev'][$iterKey];

            foreach ($result['Items'] as $item) {
                $node = null;
                try {
                    $nodeRef = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
                    if (!$context['node_refs_only']) {
                        $node = $this->marshaler->unmarshal($item);
                    }
                } catch (\Throwable $e) {
                    $this->logger->error(
                        'Item returned from DynamoDb table [{table_name}] segment [{segment}] ' .
                        'for QName [{qname}] could not be unmarshaled.',
                        [
                            'exception'  => $e,
                            'item'       => $item,
                            'context'    => $context,
                            'table_name' => $tableName,
                            'segment'    => $segment,
                            'qname'      => (string)$qname,
                        ]
                    );

                    continue;
                }

                if ($context['node_refs_only']) {
                    $queue->enqueue($nodeRef);
                } else {
                    $queue->enqueue($node);
                }
            }

            if ($result['LastEvaluatedKey']) {
                $params['Segment'] = $segment;
                $params['ExclusiveStartKey'] = $result['LastEvaluatedKey'];
                $pending[] = $this->client->getCommand('Scan', $params);
                $iter2seg['next'][] = $segment;
            } else {
                $this->logger->info(
                    'Scan of DynamoDb table [{table_name}] segment [{segment}] for QName [{qname}] is complete.',
                    [
                        'context'    => $context,
                        'table_name' => $tableName,
                        'segment'    => $segment,
                        'qname'      => (string)$qname,
                    ]
                );
            }
        };

        $rejected = function (AwsException $exception, int|string $iterKey, PromiseInterface $aggregatePromise) use (
            $qname, $tableName, $context, $skipErrors, &$iter2seg
        ) {
            $segment = $iter2seg['prev'][$iterKey];

            $errorName = $exception->getAwsErrorCode() ?: ClassUtil::getShortName($exception);
            if ('ProvisionedThroughputExceededException' === $errorName) {
                $code = Code::RESOURCE_EXHAUSTED->value;
            } else {
                $code = Code::UNAVAILABLE->value;
            }

            if ($skipErrors) {
                $this->logger->error(
                    sprintf(
                        '%s while scanning DynamoDb table [{table_name}] segment [{segment}] for QName [{qname}].',
                        $errorName
                    ),
                    [
                        'exception'  => $exception,
                        'context'    => $context,
                        'table_name' => $tableName,
                        'segment'    => $segment,
                        'qname'      => (string)$qname,
                    ]
                );

                return;
            }

            $this->marshaler->skipValidation(false);
            $aggregatePromise->reject(
                new RepositoryOperationFailed(
                    sprintf(
                        '%s while scanning DynamoDb table [%s] segment [%s] for QName [%s].',
                        $errorName,
                        $tableName,
                        $segment,
                        (string)$qname
                    ),
                    $code,
                    $exception
                )
            );
        };

        while (count($pending) > 0) {
            $commands = $pending;
            $pending = [];
            $pool = new CommandPool($this->client, $commands, [
                'before'      => function () {
                    gc_collect_cycles();
                },
                'fulfilled'   => $fulfilled,
                'rejected'    => $rejected,
                'concurrency' => $concurrency,
            ]);
            $pool->promise()->wait();
            $iter2seg['prev'] = $iter2seg['next'];
            $iter2seg['next'] = [];
            yield;
        }

        yield;
        $this->marshaler->skipValidation(false);
    }

    protected function enrichContext(string $operation, array $context): array
    {
        if (isset($context['already_enriched'])) {
            return $context;
        }

        $event = new EnrichContextEvent('ncr', $operation, $context);
        $context = $this->dispatcher->dispatch($event, PbjxEvents::ENRICH_CONTEXT)->all();
        $context['already_enriched'] = true;
        return $context;
    }
}
