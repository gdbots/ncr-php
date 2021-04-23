<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Connection;
use Gdbots\Ncr\Exception\InvalidArgumentException;
use Psr\Log\LoggerInterface;

class ClientManager
{
    protected const MAX_CLI_CONNECT_ATTEMPTS = 100;
    protected const MAX_CONNECT_ATTEMPTS = 5;

    /**
     * An array of clusters keyed by a name.  This factory will create the elastica
     * client for a given cluster using the settings in the key.
     *
     * The reason for the "clusters" (not just single cluster) config is to make it
     * possible to manage a multi-tenant app or run different clusters for different
     * parts of the same app (logging vs content, etc.).
     *
     *  [
     *      'default' => [
     *          'round_robin' => true,
     *          'timeout' => 300,
     *          'debug' => false,
     *          'persistent' => true
     *          'servers' => [
     *              ['host' => 'localhost', 'port' => 9200]
     *          ]
     *      ],
     *      'client1' => [...],
     *      'client2' => [...],
     *      'logging' => [...],
     *  ]
     *
     * @var array
     */
    protected array $clusters;

    protected ?LoggerInterface $logger;

    /** @var Client[] */
    protected array $clients = [];

    protected bool $isCli = false;

    public function __construct(array $clusters, ?LoggerInterface $logger = null)
    {
        $this->clusters = $clusters;
        $this->logger = $logger;
        $this->isCli = php_sapi_name() === 'cli';
    }

    /**
     * Gets an elastica client using the config from the provided cluster name.
     *
     * All calls with the same cluster name will return the same client instance.
     *
     * @param string $cluster
     *
     * @return Client
     */
    public function getClient(string $cluster = 'default'): Client
    {
        if (isset($this->clients[$cluster])) {
            return $this->clients[$cluster];
        }

        if (!$this->hasCluster($cluster)) {
            throw new InvalidArgumentException(
                sprintf(
                    'The cluster [%s] is not one of the available clusters [%s].',
                    $cluster,
                    implode(',', $this->getAvailableClusters())
                )
            );
        }

        $config = $this->clusters[$cluster];

        if (isset($config['debug'])) {
            $config['log'] = filter_var($config['debug'], FILTER_VALIDATE_BOOLEAN);
            unset($config['debug']);
        } else {
            $config['log'] = false;
        }

        if (isset($config['round_robin'])) {
            $config['roundRobin'] = filter_var($config['round_robin'], FILTER_VALIDATE_BOOLEAN);
            unset($config['round_robin']);
        }

        $config['clusterName'] = $cluster;
        $config['connectAttempts'] = 0;

        if (!isset($config['maxConnectAttempts'])) {
            $config['maxConnectAttempts'] = $this->isCli
                ? static::MAX_CLI_CONNECT_ATTEMPTS
                : static::MAX_CONNECT_ATTEMPTS;
        }

        if (!isset($config['connectTimeout'])) {
            $config['connectTimeout'] = 1;
        }

        $config = $this->configureCluster($cluster, $config);
        $servers = $config['servers'];
        $configuredServers = [];
        unset($config['servers']);

        foreach ($servers as $server) {
            $configuredServers[] = array_merge($config, $server);
        }

        $config['servers'] = $configuredServers;
        return $this->clients[$cluster] = new Client($config, [$this, 'onConnectionFailure'], $config['log'] ? $this->logger : null);
    }

    public function hasCluster(string $cluster): bool
    {
        return isset($this->clusters[$cluster]);
    }

    /**
     * Returns the names of the available clusters.
     *
     * @return string[]
     */
    public function getAvailableClusters(): array
    {
        return array_keys($this->clusters);
    }

    public function onConnectionFailure(Connection $connection, \Throwable $e, Client $client): void
    {
        $attempts = $client->getConfigValue('connectAttempts') + 1;
        $client->setConfigValue('connectAttempts', $attempts);
        $maxAttempts = $client->getConfigValue('maxConnectAttempts');

        if ($attempts > $maxAttempts) {
            return;
        }

        usleep(mt_rand(10, 100) * 1000);
        $connection->setEnabled(true);
    }

    protected function configureCluster(string $cluster, array $config): array
    {
        return $config;
    }
}
