<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\CommandPool;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Aws\ResultInterface;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Common\Util\NumberUtils;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\Exception\RepositoryIndexNotFound;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Marshaler\DynamoDb\ItemMarshaler;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Pbjx\Enum\Code;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class DynamoDbNcr implements Ncr
{
    /** @var DynamoDbClient */
    private $client;

    /** @var TableManager */
    private $tableManager;

    /** @var array */
    private $config;

    /** @var LoggerInterface */
    private $logger;

    /** @var ItemMarshaler */
    private $marshaler;

    /** @var IndexQueryFilterProcessor */
    private $filterProcessor;

    /**
     * @param DynamoDbClient  $client
     * @param TableManager    $tableManager
     * @param array           $config
     * @param LoggerInterface $logger
     */
    public function __construct(DynamoDbClient $client, TableManager $tableManager, array $config = [], ?LoggerInterface $logger = null)
    {
        // defaults
        $config += [
            'batch_size' => 100,
            'pool_size'  => 25,
        ];

        $config['batch_size'] = NumberUtils::bound($config['batch_size'], 2, 100);
        $config['pool_size'] = NumberUtils::bound($config['pool_size'], 1, 50);

        $this->client = $client;
        $this->tableManager = $tableManager;
        $this->config = $config;
        $this->logger = $logger ?: new NullLogger();
        $this->marshaler = new ItemMarshaler();
    }

    /**
     * {@inheritdoc}
     */
    public function createStorage(SchemaQName $qname, array $context = []): void
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $context);
        $this->tableManager->getNodeTable($qname)->create($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function describeStorage(SchemaQName $qname, array $context = []): string
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $context);
        return $this->tableManager->getNodeTable($qname)->describe($this->client, $tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $response = $this->client->getItem([
                'ConsistentRead'           => $consistent,
                'TableName'                => $tableName,
                'ProjectionExpression'     => '#node_ref',
                'ExpressionAttributeNames' => ['#node_ref' => NodeTable::HASH_KEY_NAME],
                'Key'                      => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    return false;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
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

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Node
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $response = $this->client->getItem([
                'ConsistentRead' => $consistent,
                'TableName'      => $tableName,
                'Key'            => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    throw NodeNotFound::forNodeRef($nodeRef, $e);
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
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

        try {
            /** @var Node $node */
            $node = $this->marshaler->unmarshal($response['Item']);
        } catch (\Exception $e) {
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

            throw NodeNotFound::forNodeRef($nodeRef, $e);
        }

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array
    {
        if (empty($nodeRefs)) {
            return [];
        } elseif (count($nodeRefs) === 1) {
            try {
                $nodeRef = array_shift($nodeRefs);
                return [(string)$nodeRef => $this->getNode($nodeRef, $consistent, $context)];
            } catch (NodeNotFound $e) {
                return [];
            } catch (\Exception $e) {
                throw $e;
            }
        }

        $batch = (new BatchGetItemRequest($this->client))
            ->batchSize($this->config['batch_size'])
            ->poolSize($this->config['pool_size'])
            ->consistentRead($consistent)
            ->onError(function (AwsException $e) use ($context) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                $this->logger->error(
                    sprintf('%s while processing BatchGetItemRequest.', $errorName),
                    ['exception' => $e, 'context' => $context]
                );
            });

        foreach ($nodeRefs as $nodeRef) {
            $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);
            $batch->addItemKey($tableName, [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]]);
        }

        $nodes = [];
        foreach ($batch->getItems() as $item) {
            try {
                $nodeRef = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
                $nodes[$nodeRef->toString()] = $this->marshaler->unmarshal($item);
            } catch (\Exception $e) {
                $this->logger->error(
                    'Item returned from DynamoDb table could not be unmarshaled.',
                    ['exception' => $e, 'item' => $item, 'context' => $context]
                );
            }
        }

        return $nodes;
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $context = []): void
    {
        $node->freeze();
        $nodeRef = NodeRef::fromNode($node);
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);
        $table = $this->tableManager->getNodeTable($nodeRef->getQName());

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
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
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
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
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

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $context = []): void
    {
        $tableName = $this->tableManager->getNodeTableName($nodeRef->getQName(), $context);

        try {
            $this->client->deleteItem([
                'TableName' => $tableName,
                'Key'       => [NodeTable::HASH_KEY_NAME => ['S' => $nodeRef->toString()]],
            ]);
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ResourceNotFoundException' === $errorName) {
                    // if it's already deleted, it's fine
                    return;
                } elseif ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
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

    /**
     * {@inheritdoc}
     */
    public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult
    {
        $tableName = $this->tableManager->getNodeTableName($query->getQName(), $context);
        $table = $this->tableManager->getNodeTable($query->getQName());

        if (!$table->hasIndex($query->getAlias())) {
            throw new RepositoryIndexNotFound(
                sprintf(
                    '%s::Index [%s] does not exist on table [%s].',
                    ClassUtils::getShortName($table),
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
        } catch (\Exception $e) {
            if ($e instanceof AwsException) {
                $errorName = $e->getAwsErrorCode() ?: ClassUtils::getShortName($e);
                if ('ProvisionedThroughputExceededException' === $errorName) {
                    $code = Code::RESOURCE_EXHAUSTED;
                } else {
                    $code = Code::UNAVAILABLE;
                }
            } else {
                $errorName = ClassUtils::getShortName($e);
                $code = Code::INTERNAL;
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
            } catch (\Exception $e) {
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

        return new IndexQueryResult($query, $nodeRefs, $nextCursor);
    }

    /**
     * {@inheritdoc}
     */
    public function pipeNodes(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        $context['node_refs_only'] = false;
        $this->doPipeNodes($qname, $receiver, $context);
    }

    /**
     * {@inheritdoc}
     */
    public function pipeNodeRefs(SchemaQName $qname, callable $receiver, array $context = []): void
    {
        $context['node_refs_only'] = true;
        $this->doPipeNodes($qname, $receiver, $context);
    }

    /**
     * @param SchemaQName $qname
     * @param callable    $receiver
     * @param array       $context
     */
    private function doPipeNodes(SchemaQName $qname, callable $receiver, array $context): void
    {
        $tableName = $this->tableManager->getNodeTableName($qname, $context);
        $skipErrors = filter_var($context['skip_errors'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $reindexing = filter_var($context['reindexing'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $limit = NumberUtils::bound($context['limit'] ?? 100, 1, 500);
        $totalSegments = NumberUtils::bound($context['total_segments'] ?? 16, 1, 64);
        $poolDelay = NumberUtils::bound($context['pool_delay'] ?? 500, 100, 10000);

        $params = [
            'ExpressionAttributeNames'  => [
                '#node_ref' => NodeTable::HASH_KEY_NAME,
            ],
            'ExpressionAttributeValues' => [
                ':v_qname' => ['S' => $qname->toString() . ':'],
            ],
        ];

        $filterExpressions = ['begins_with(#node_ref, :v_qname)'];

        if ($reindexing) {
            $params['ExpressionAttributeNames']['#indexed'] = NodeTable::INDEXED_KEY_NAME;
            $filterExpressions[] = 'attribute_exists(#indexed)';
        }

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
        $params['Limit'] = $limit;
        $params['TotalSegments'] = $totalSegments;
        $params['FilterExpression'] = implode(' AND ', $filterExpressions);

        $pending = [];
        $iter2seg = ['prev' => [], 'next' => []];
        for ($segment = 0; $segment < $totalSegments; $segment++) {
            $params['Segment'] = $segment;
            $iter2seg['prev'][] = $segment;
            $pending[] = $this->client->getCommand('Scan', $params);
        }

        $fulfilled = function (ResultInterface $result, string $iterKey) use (
            $qname, $receiver, $tableName, $context, $params, &$pending, &$iter2seg
        ) {
            $segment = $iter2seg['prev'][$iterKey];

            foreach ($result['Items'] as $item) {
                $node = null;
                try {
                    $nodeRef = NodeRef::fromString($item[NodeTable::HASH_KEY_NAME]['S']);
                    if (!$context['node_refs_only']) {
                        $node = $this->marshaler->unmarshal($item);
                    }
                } catch (\Exception $e) {
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
                    $receiver($nodeRef);
                } else {
                    $receiver($node);
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

        $rejected = function (AwsException $exception, string $iterKey, PromiseInterface $aggregatePromise) use (
            $qname, $tableName, $context, $skipErrors, &$iter2seg
        ) {
            $segment = $iter2seg['prev'][$iterKey];

            $errorName = $exception->getAwsErrorCode() ?: ClassUtils::getShortName($exception);
            if ('ProvisionedThroughputExceededException' === $errorName) {
                $code = Code::RESOURCE_EXHAUSTED;
            } else {
                $code = Code::UNAVAILABLE;
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
            $pool = new CommandPool($this->client, $commands, ['fulfilled' => $fulfilled, 'rejected' => $rejected]);
            $pool->promise()->wait();
            $iter2seg['prev'] = $iter2seg['next'];
            $iter2seg['next'] = [];

            if (count($pending) > 0) {
                $this->logger->info(sprintf('Pausing for %d milliseconds.', $poolDelay));
                usleep($poolDelay * 1000);
            }
        }
    }
}
