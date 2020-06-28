<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Gdbots\Ncr\Exception\InvalidArgumentException;
use Psr\Log\LoggerInterface;

class ClientManager
{
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
    private array $clusters;

    private ?LoggerInterface $logger;

    /** @var Client[] */
    private array $clients = [];

    public function __construct(array $clusters, ?LoggerInterface $logger = null)
    {
        $this->clusters = $clusters;
        $this->logger = $logger;
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
    final public function getClient(string $cluster = 'default'): Client
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

        $config = $this->configureCluster($cluster, $config);
        $servers = $config['servers'];
        $configuredServers = [];
        unset($config['servers']);

        foreach ($servers as $server) {
            $configuredServers[] = array_merge($config, $server);
        }

        $config['servers'] = $configuredServers;
        return $this->clients[$cluster] = new Client($config, null, $config['log'] ? $this->logger : null);
    }

    final public function hasCluster(string $cluster): bool
    {
        return isset($this->clusters[$cluster]);
    }

    /**
     * Returns the names of the available clusters.
     *
     * @return string[]
     */
    final public function getAvailableClusters(): array
    {
        return array_keys($this->clusters);
    }

    protected function configureCluster(string $cluster, array $config): array
    {
        return $config;
    }
}
