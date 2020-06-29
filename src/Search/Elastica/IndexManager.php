<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Client;
use Elastica\Index;
use Elastica\Mapping;
use Gdbots\Ncr\Exception\SearchOperationFailed;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\ClassUtil;
use Gdbots\Pbj\Util\NumberUtil;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Pbjx\Enum\Code;

class IndexManager
{
    /**
     * Our "created_at" field is a 16 digit integer (seconds + 6 digits microtime)
     * In order to use elasticsearch time range queries we'll store a derived value
     * of the ISO (in UTC/ZULU) into another field.
     *
     * Generally we use "__" to indicate a derived field but kibana won't recognize it
     * and it's already been debated with no fix yet.
     *
     * @link  https://github.com/elastic/kibana/issues/2551
     * @link  https://github.com/elastic/kibana/issues/4762
     *
     * So for now, we'll use "d__" to indicate a derived field for ES.
     *
     * @const string
     */
    const CREATED_AT_ISO_FIELD_NAME = 'd__created_at_iso';

    private string $prefix;

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
    private array $indexes = [];

    /**
     * An array keyed by a qname with a value that references the index
     * that should be used and the mapper class to use.
     * [
     *      'acme:article' => [
     *          'mapper_class' => 'Acme\Ncr\Search\Elastica\ArticleMapper',
     *          'index_name'   => 'article',
     *      ]
     * ]
     *
     * @var array
     */
    private array $types = [];

    /**
     * Array of NodeMapper instances keyed by its class name.
     *
     * @var NodeMapper[]
     */
    private array $mappers = [];

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
            'mapper_class' => NodeMapper::class,
            'index_name'   => 'default',
        ];

        foreach ($this->indexes as $indexName => $settings) {
            $this->indexes[$indexName]['fq_index_name'] = 'default' === $indexName ? $this->prefix : "{$this->prefix}-{$indexName}";
            $this->indexes[$indexName]['number_of_shards'] = NumberUtil::bound($settings['number_of_shards'] ?? 5, 1, 100);
            $this->indexes[$indexName]['number_of_replicas'] = NumberUtil::bound($settings['number_of_replicas'] ?? 1, 1, 100);
        }
    }

    /**
     * Creates the index in ElasticSearch if it doesn't already exist. It will NOT
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
        $settings = $this->filterIndexSettings($this->indexes[$type['index_name'] ?? 'default'], $qname, $context);
        unset($settings['fq_index_name']);

        if (true === ($context['destroy'] ?? false)) {
            try {
                $index->delete();
            } catch (\Throwable $e) {
                throw new SearchOperationFailed(
                    sprintf(
                        '%s while deleting index [%s] for qname [%s].',
                        ClassUtil::getShortName($e),
                        $index->getName(),
                        $qname
                    ),
                    Code::INTERNAL,
                    $e
                );
            }
        }

        try {
            $settings['analysis'] = [
                'analyzer'   => $this->getCustomAnalyzers(),
                'normalizer' => $this->getCustomNormalizers(),
            ];
            $index->create([
                'settings' => $settings,
                'mappings' => $this->createMapping()->toArray(),
            ]);
        } catch (\Throwable $e) {
            if (strpos($e->getMessage(), 'resource_already_exists_exception')) {
                try {
                    $client->connect();
                    $index->setMapping($this->createMapping());
                    return $index;
                } catch (\Throwable $e2) {
                    $e = $e2;
                }
            }

            throw new SearchOperationFailed(
                sprintf(
                    '%s while creating index [%s] for qname [%s].',
                    ClassUtil::getShortName($e),
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
     * Filter the index settings before it's returned to the consumer. Typically used to adjust
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
     * Filter the index name before it's returned to the consumer. Typically used to add
     * prefixes or suffixes for multi-tenant applications using the context.
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

    protected function createMapping(): Mapping
    {
        $builder = $this->getMappingBuilder();
        foreach (MessageResolver::findAllUsingMixin(NodeV1Mixin::SCHEMA_CURIE_MAJOR) as $curie) {
            $builder->addSchema(MessageResolver::resolveCurie($curie)::schema());
        }

        $mapping = $builder->build();
        $properties = $mapping->getProperties();
        unset($properties[NodeV1Mixin::_ID_FIELD]);
        $properties[self::CREATED_AT_ISO_FIELD_NAME] = MappingBuilder::TYPES['date'];
        $mapping->setProperties($properties);

        return $mapping;
    }

    /**
     * @link http://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-custom-analyzer.html
     *
     * @return array
     */
    protected function getCustomAnalyzers(): array
    {
        return MappingBuilder::getCustomAnalyzers();
    }

    /**
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-normalizers.html
     *
     * @return array
     */
    protected function getCustomNormalizers(): array
    {
        return MappingBuilder::getCustomNormalizers();
    }

    protected function getMappingBuilder(): MappingBuilder
    {
        return new MappingBuilder();
    }
}
