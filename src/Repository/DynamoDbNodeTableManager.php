<?php

namespace Gdbots\Ncr\Repository;

use Gdbots\Pbj\SchemaQName;

/**
 * DynamoDbNodeTableManager
 *
 */
class DynamoDbNodeTableManager
{
    /**
     * An array of tables keyed by a qname. For example:
     * [
     *     'acme:article' => [
     *         'schema_class' => 'Gdbots\Ncr\Repository\DynamoDbNodeTableSchema',
     *         'table_name' => '%prefix%-article'
     *     ]
     * ]
     *
     * @var array
     */
    private $tables = [];

    /** @var DynamoDbNodeTableSchema[] */
    private $instances = [];

    /**
     * @param array $tables
     * @param string $tableNamePrefix
     */
    public function __construct(array $tables, $tableNamePrefix)
    {
        $this->tables = $tables;

        if (!isset($this->tables['default'])) {
            $this->tables['default'] = [
                'schema_class' => 'Gdbots\Ncr\Repository\DynamoDbNodeTableSchema'
            ];
        }
    }

    /**
     * @param SchemaQName $qname
     *
     * @return DynamoDbNodeTableSchema
     */
    public function getTableSchema(SchemaQName $qname)
    {
        $key = $qname->toString();

        if (isset($this->instances[$key])) {
            return $this->instances[$key];
        }

        if (isset($this->tables[$key], $this->tables[$key]['schema_class'])) {
            $class = $this->tables[$key]['schema_class'];
        } else {
            $class = $this->tables['default']['schema_class'];
        }

        if (isset($this->instances[$class])) {
            return $this->instances[$key] = $this->instances[$class];
        }

        return $this->instances[$key] = $this->instances[$class] = new $class;
    }

    /**
     * @param SchemaQName $qname
     * @param array $hints
     *
     * @return string
     */
    public function getTableName(SchemaQName $qname, array $hints = [])
    {
        return 'test';
    }
}
