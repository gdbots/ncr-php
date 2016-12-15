<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Aws\ResultInterface;
use Gdbots\Common\Util\NumberUtils;

/**
 * The BatchGetItemRequest is an object that is capable of efficiently handling
 * batchGetItem requests.  It processes with the fewest requests to DynamoDB
 * as possible and also re-queues any unprocessed items to ensure that all
 * items are fetched.
 */
final class BatchGetItemRequest
{
    /** @var DynamoDbClient */
    private $client;

    /**
     * Number of items to fetch per request.  (AWS max is 100)
     *
     * @var int
     */
    private $batchSize = 100;

    /**
     * Number of parallel requests to run.
     *
     * @var int
     */
    private $poolSize = 5;

    /**
     * When true, a ConsistentRead is used.
     *
     * @var bool
     */
    private $consistentRead = false;

    /** @var array */
    private $queue;

    /**
     * @param DynamoDbClient $client
     */
    public function __construct(DynamoDbClient $client)
    {
        $this->client = $client;
        $this->queue = [];
    }

    /**
     * @param int $batchSize
     *
     * @return self
     */
    public function withBatchSize(int $batchSize = 100): self
    {
        $this->batchSize = NumberUtils::bound($batchSize, 2, 100);
        return $this;
    }

    /**
     * @param int $poolSize
     *
     * @return self
     */
    public function withPoolSize(int $poolSize = 5): self
    {
        $this->poolSize = NumberUtils::bound($poolSize, 1, 10);
        return $this;
    }

    /**
     * @param bool $consistentRead
     *
     * @return self
     */
    public function usingConsistentRead(bool $consistentRead = false): self
    {
        $this->consistentRead = $consistentRead;
        return $this;
    }

    /**
     * Adds an item key to get in this batch request.
     *
     * @param string $table
     * @param array  $key
     *
     * @return self
     */
    public function addItemKey(string $table, array $key): self
    {
        $this->queue[] = ['table' => $table, 'key' => $key];
        return $this;
    }

    /**
     * Flushes the batch by combining all the queued requests into
     * BatchGetItem commands and executing them. UnprocessedKeys
     * are automatically re-queued.
     *
     * @return self
     */
    public function flush()
    {
        $items = [];
        while ($this->queue) {
            $commands = $this->prepareCommands();
            $pool = new CommandPool($this->client, $commands, [
                'concurrency' => $this->poolSize,
                'fulfilled'   => function (ResultInterface $result) {
                    if ($result->hasKey('UnprocessedKeys')) {
                        $this->retryUnprocessed($result['UnprocessedKeys']);
                    }

                    foreach ($result->get('Responses') as $response) {

                    }
                },
                'rejected'    => function ($reason) {
                    if ($reason instanceof AwsException) {
                        $code = $reason->getAwsErrorCode();
                        if ($code === 'ProvisionedThroughputExceededException') {
                            $this->retryUnprocessed($reason->getCommand()['RequestItems']);
                        } elseif (is_callable($this->config['error'])) {
                            $this->config['error']($reason);
                        }
                    }
                },
            ]);

            $pool->promise()->wait();
        }

        return $this;
    }

    /**
     * Creates BatchGetItem commands from the items in the queue.
     *
     * @return CommandInterface[]
     */
    private function prepareCommands()
    {
        $batches = array_chunk($this->queue, $this->batchSize);
        $this->queue = [];

        $commands = [];
        foreach ($batches as $batch) {
            $requests = [];
            foreach ($batch as $item) {
                if (!isset($requests[$item['table']])) {
                    $requests[$item['table']] = ['Keys' => [], 'ConsistentRead' => $this->consistentRead];
                }
                $requests[$item['table']]['Keys'][] = $item['key'];
            }

            echo json_encode(['RequestItems' => $requests], JSON_PRETTY_PRINT);
            $commands[] = $this->client->getCommand('BatchGetItem', ['RequestItems' => $requests]);
        }

        return $commands;
    }

    /**
     * Re-queues unprocessed results with the correct data.
     *
     * @param array $unprocessed
     */
    private function retryUnprocessed(array $unprocessed)
    {
        foreach ($unprocessed as $table => $requests) {
            foreach ($requests['Keys'] as $key) {
                $this->queue[] = ['table' => $table, 'key' => $key];
            }
        }
    }

}
