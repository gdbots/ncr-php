<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Pbj\SchemaQName;

class TableManager
{
    private string $tableNamePrefix;

    /**
     * An array of tables keyed by a qname. For example:
     * [
     *     'acme:article' => [
     *         'class' => 'Acme\Ncr\Repository\DynamoDb\ArticleNodeTable',
     *         'table_name' => 'post' // optional, use if the "message" portion of qname is not desired.
     *     ]
     * ]
     *
     * @var array
     */
    private array $nodeTables = [];

    /** @var NodeTable[] */
    private array $instances = [];
    private array $resolvedTableNames = [];

    /**
     * @param string $tableNamePrefix The prefix to use for all table names.  Typically "app-environment-ncr", e.g. "acme-prod-ncr"
     * @param array  $nodeTables      Node tables that define the class and optionally the table name to use.
     */
    public function __construct(string $tableNamePrefix, array $nodeTables = [])
    {
        $this->tableNamePrefix = sprintf('%s-%s-', $tableNamePrefix, NodeTable::SCHEMA_VERSION);
        $this->nodeTables = $nodeTables;

        if (!isset($this->nodeTables['default'])) {
            $this->nodeTables['default'] = [
                'class'      => NodeTable::class,
                'table_name' => 'multi',
            ];
        }

        if (!isset($this->nodeTables['default']['class'])) {
            $this->nodeTables['default']['class'] = NodeTable::class;
        }
    }

    final public function getNodeTable(SchemaQName $qname): NodeTable
    {
        $key = $qname->toString();

        if (isset($this->instances[$key])) {
            return $this->instances[$key];
        }

        if (isset($this->nodeTables[$key], $this->nodeTables[$key]['class'])) {
            $class = $this->nodeTables[$key]['class'];
        } else {
            $class = $this->nodeTables['default']['class'];
        }

        if (isset($this->instances[$class])) {
            return $this->instances[$key] = $this->instances[$class];
        }

        return $this->instances[$key] = $this->instances[$class] = new $class;
    }

    /**
     * Returns the table name that should be used to read/write from for the given SchemaQName.
     *
     * @param SchemaQName $qname   QName used to derive the unfiltered table name.
     * @param array       $context Data that helps the NCR decide where to read/write data from.
     *
     * @return string
     */
    final public function getNodeTableName(SchemaQName $qname, array $context = []): string
    {
        $key = $qname->toString();

        if (isset($this->resolvedTableNames[$key])) {
            return $this->filterNodeTableName($this->resolvedTableNames[$key], $qname, $context);
        }

        if (isset($this->nodeTables[$key], $this->nodeTables[$key]['table_name'])) {
            $tableName = $this->nodeTables[$key]['table_name'];
        } elseif (isset($this->nodeTables['default'], $this->nodeTables['default']['table_name'])) {
            $tableName = $this->nodeTables['default']['table_name'];
        } else {
            $tableName = $qname->getMessage();
        }

        $this->resolvedTableNames[$key] = $this->tableNamePrefix . $tableName;
        return $this->filterNodeTableName($this->resolvedTableNames[$key], $qname, $context);
    }

    /**
     * Filter the table name before it's returned to the consumer.  Typically used to add
     * prefixes or suffixes for multi-tenant applications using the context array.
     *
     * @param string      $tableName The resolved table name, from converting qname to the config value.
     * @param SchemaQName $qname     QName used to derive the unfiltered table name.
     * @param array       $context   Data that helps the NCR decide where to read/write data from.
     *
     * @return string
     */
    protected function filterNodeTableName(string $tableName, SchemaQName $qname, array $context): string
    {
        return $tableName;
    }
}
