<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Exception\AwsException;
use Aws\ResultInterface;
use Gdbots\Pbj\Util\NumberUtil;

/**
 * The BatchGetItemRequest is an object that is capable of efficiently handling
 * batchGetItem requests.  It processes with the fewest requests to DynamoDB
 * as possible and also re-queues any unprocessed items to ensure that all
 * items are fetched.
 */
final class BatchGetItemRequest
{
    private DynamoDbClient $client;

    /**
     * Number of items to fetch per request.  (AWS max is 100)
     *
     * @var int
     */
    private int $batchSize = 100;

    /**
     * Number of parallel requests to run.
     *
     * @var int
     */
    private int $concurrency = 25;

    /**
     * When true, a ConsistentRead is used.
     *
     * @var bool
     */
    private bool $consistentRead = false;

    /**
     * A callable to execute when an error occurs.
     *
     * @var callable
     */
    private $errorFunc;

    /** @var array */
    private array $queue;

    public function __construct(DynamoDbClient $client)
    {
        $this->client = $client;
        $this->queue = [];
    }

    public function batchSize(int $batchSize = 100): self
    {
        $this->batchSize = NumberUtil::bound($batchSize, 2, 100);
        return $this;
    }

    public function poolSize(int $poolSize = 25): self
    {
        return $this->concurrency($poolSize);
    }

    public function concurrency(int $concurrency = 25): self
    {
        $this->concurrency = NumberUtil::bound($concurrency, 1, 50);
        return $this;
    }

    public function consistentRead(bool $consistentRead = false): self
    {
        $this->consistentRead = $consistentRead;
        return $this;
    }

    public function onError(callable $func): self
    {
        $this->errorFunc = $func;
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
     * Processes the batch by combining all the queued requests into
     * BatchGetItem commands and executing them. UnprocessedKeys
     * are automatically re-queued.
     *
     * @return array
     */
    public function getItems(): array
    {
        $allItems = [];

        while ($this->queue) {
            $commands = $this->prepareCommands();
            $pool = new CommandPool($this->client, $commands, [
                'concurrency' => $this->concurrency,
                'fulfilled'   => function (ResultInterface $result) use (&$allItems) {
                    if ($result->hasKey('UnprocessedKeys')) {
                        $this->retryUnprocessed($result['UnprocessedKeys']);
                    }

                    foreach ((array)$result->get('Responses') as $tableName => $items) {
                        $allItems = array_merge($allItems, $items);
                    }
                },
                'rejected'    => function ($reason) {
                    if ($reason instanceof AwsException) {
                        if ('ProvisionedThroughputExceededException' === $reason->getAwsErrorCode()) {
                            $this->retryUnprocessed($reason->getCommand()['RequestItems']);
                            return;
                        }

                        if (is_callable($this->errorFunc)) {
                            $func = $this->errorFunc;
                            $func($reason);
                        }
                    }
                },
            ]);

            $pool->promise()->wait();
        }

        return $allItems;
    }

    /**
     * Creates BatchGetItem commands from the items in the queue.
     *
     * @return CommandInterface[]
     */
    private function prepareCommands(): array
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
            $commands[] = $this->client->getCommand('BatchGetItem', ['RequestItems' => $requests]);
        }

        return $commands;
    }

    /**
     * Re-queues unprocessed results with the correct data.
     *
     * @param array $unprocessed
     */
    private function retryUnprocessed(array $unprocessed): void
    {
        foreach ($unprocessed as $table => $requests) {
            foreach ($requests['Keys'] as $key) {
                $this->queue[] = ['table' => $table, 'key' => $key];
            }
        }
    }
}
