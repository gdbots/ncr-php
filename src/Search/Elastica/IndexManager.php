<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Index;
use Elastica\Type;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Common\Util\NumberUtils;
use Gdbots\Ncr\Exception\SearchOperationFailed;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Pbjx\Enum\Code;

class IndexManager
{
    /** @var string */
    private $prefix;

    /**
     * An array of indexes keyed by index_name. For example:
     * [
     *     'article' => [
     *         'number_of_shards'   => 5,
     *         'number_of_replicas' => 1,
     *     ]
     * ]
     *
     * @var array
     */
    private $indexes = [];

    /**
     * An array keyed by a qname with a value that references the index
     * that should be used and the mapper class to use.
     * [
     *      'acme:article' => [
     *          'mapper_class' => 'Acme\Ncr\Search\Elastica\ArticleMapper',
     *          'index_name'   => 'article',
     *          'type_name'    => 'post' // optional, use if the "message" portion of qname is not desired.
     *      ]
     * ]
     *
     * @var array
     */
    private $types = [];

    /**
     * Array of NodeMapper instances keyed by its class name.
     *
     * @var NodeMapper[]
     */
    private $mappers = [];

    /**
     * @param string $prefix  The prefix to use for all index names.  Typically "app-environment-ncr", e.g. "acme-prod-ncr"
     * @param array  $indexes Index settings keyed by index_name.
     * @param array  $types   Config for types keyed by a qname, e.g. "acme:article"
     */
    public function __construct(string $prefix, array $indexes = [], array $types = [])
    {
        $this->prefix = trim($prefix, '-');
        $this->indexes = $indexes;
        $this->types = $types;

        if (!isset($this->indexes['default'])) {
            $this->indexes['default'] = [];
        }

        if (!isset($this->types['default'])) {
            $this->types['default'] = [];
        }

        $this->types['default'] += [
            'mapper_class' => 'Gdbots\Ncr\Search\Elastica\NodeMapper',
            'index_name'   => 'default',
        ];

        foreach ($this->indexes as $indexName => $settings) {
            $this->indexes[$indexName]['fq_index_name'] = 'default' === $indexName ? $this->prefix : "{$this->prefix}-{$indexName}";
            $this->indexes[$indexName]['number_of_shards'] = NumberUtils::bound($settings['number_of_shards'] ?? 5, 1, 100);
            $this->indexes[$indexName]['number_of_replicas'] = NumberUtils::bound($settings['number_of_replicas'] ?? 1, 1, 100);
        }
    }

    /**
     * Creates the index in ElasticSearch if it doesn't already exist.  It will NOT
     * perform an update as that requires the index be closed and reopened which
     * is not currently supported on AWS ElasticSearch service.
     *
     * @param Client      $client  The Elastica client
     * @param SchemaQName $qname   QName used to derive the index name.
     * @param array       $context Data that helps the NCR Search decide where to read/write data from.
     *
     * @return Index
     */
    final public function createIndex(Client $client, SchemaQName $qname, array $context = []): Index
    {
        static $created = [];

        $indexName = $this->getIndexName($qname, $context);

        if (isset($created[$indexName])) {
            return $created[$indexName];
        }

        $index = $created[$indexName] = $client->getIndex($indexName);
        $type = $this->types[$qname->toString()] ?? $this->types['default'];
        $mapper = $this->getNodeMapper($qname);
        $settings = $this->filterIndexSettings($this->indexes[$type['index_name'] ?? 'default'], $qname, $context);
        unset($settings['fq_index_name']);

        if (true === ($context['destroy'] ?? false)) {
            try {
                $index->delete();
            } catch (\Throwable $e) {
                if (false === strpos($e->getMessage(), 'no such index')) {
                    throw new SearchOperationFailed(
                        sprintf(
                            '%s while deleting index [%s] for qname [%s].',
                            ClassUtils::getShortName($e),
                            $index->getName(),
                            $qname
                        ),
                        Code::INTERNAL,
                        $e
                    );
                }
            }
        }

        try {
            if (!$index->exists()) {
                $settings['analysis'] = [
                    'analyzer'   => $mapper->getCustomAnalyzers(),
                    'normalizer' => $mapper->getCustomNormalizers(),
                ];

                $index->create($settings);
            }
        } catch (\Throwable $e) {
            throw new SearchOperationFailed(
                sprintf(
                    '%s while creating index [%s] for qname [%s].',
                    ClassUtils::getShortName($e),
                    $index->getName(),
                    $qname
                ),
                Code::INTERNAL,
                $e
            );
        }

        return $index;
    }

    /**
     * Creates or updates the Type in ElasticSearch.  This expects the
     * Index to already exist and have any analyzers it needs.
     *
     * @param Index       $index The Elastica Index to create the Type in.
     * @param SchemaQName $qname QName used to derive the type name.
     *
     * @return Type
     */
    final public function createType(Index $index, SchemaQName $qname): Type
    {
        $type = new Type($index, $this->getTypeName($qname));
        $mapping = $this->getNodeMapper($qname)->getMapping($qname);
        $mapping->setType($type);

        try {
            $mapping->send();
        } catch (\Throwable $e) {
            throw new SearchOperationFailed(
                sprintf(
                    '%s while putting mapping for type [%s/%s] into ElasticSearch for qname [%s].',
                    ClassUtils::getShortName($e),
                    $index->getName(),
                    $type->getName(),
                    $qname
                ),
                Code::INTERNAL,
                $e
            );
        }

        return $type;
    }

    /**
     * Returns the index prefix which can be used in wildcard NCR searches that
     * search all indices and types.
     *
     * @param array $context Data that helps the NCR Search decide where to read/write data from.
     *
     * @return string
     */
    final public function getIndexPrefix(array $context = []): string
    {
        return $this->filterIndexName($this->prefix, null, $context);
    }

    /**
     * Returns the fully qualified index name that should be used to read/write from for the given SchemaQName.
     *
     * @param SchemaQName $qname   QName used to derive the index name.
     * @param array       $context Data that helps the NCR Search decide where to read/write data from.
     *
     * @return string
     */
    final public function getIndexName(SchemaQName $qname, array $context = []): string
    {
        $type = $this->types[$qname->toString()] ?? $this->types['default'];
        return $this->filterIndexName($this->indexes[$type['index_name'] ?? 'default']['fq_index_name'], $qname, $context);
    }

    /**
     * Returns the type name that should be used to read/write from for the given SchemaQName.
     *
     * @param SchemaQName $qname QName used to derive the type name.
     *
     * @return string
     */
    final public function getTypeName(SchemaQName $qname): string
    {
        $type = $this->types[$qname->toString()] ?? $this->types['default'];
        return $type['type_name'] ?? $qname->getMessage();
    }

    /**
     * @param SchemaQName $qname
     *
     * @return NodeMapper
     */
    final public function getNodeMapper(SchemaQName $qname): NodeMapper
    {
        $key = $qname->toString();

        if (isset($this->mappers[$key])) {
            return $this->mappers[$key];
        }

        if (isset($this->types[$key], $this->types[$key]['mapper_class'])) {
            $class = $this->types[$key]['mapper_class'];
        } else {
            $class = $this->types['default']['mapper_class'];
        }

        if (isset($this->mappers[$class])) {
            return $this->mappers[$key] = $this->mappers[$class];
        }

        return $this->mappers[$key] = $this->mappers[$class] = new $class;
    }

    /**
     * Filter the index settings before it's returned to the consumer.  Typically used to adjust
     * the sharding/replica config using the context.
     *
     * @param array       $settings Index settings used when creating/updating the index.
     * @param SchemaQName $qname    QName used to derive the unfiltered index name.
     * @param array       $context  Data that helps the NCR decide where to read/write data from.
     *
     * @return array
     */
    protected function filterIndexSettings(array $settings, SchemaQName $qname, array $context): array
    {
        return $settings;
    }

    /**
     * Filter the index name before it's returned to the consumer.  Typically used to add
     * prefixes or suffixes for multi-tenant applications using the hints array.
     *
     * @param string      $indexName The resolved index name, from converting qname to the config value.
     * @param SchemaQName $qname     QName used to derive the unfiltered index name.
     * @param array       $context   Data that helps the NCR decide where to read/write data from.
     *
     * @return string
     */
    protected function filterIndexName(string $indexName, ?SchemaQName $qname, array $context): string
    {
        return $indexName;
    }
}
