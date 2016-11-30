<?php

namespace Gdbots\Ncr\Repository\DynamoDb;

use Gdbots\Pbj\SchemaQName;

class TableManager
{
    /**
     * An array of tables keyed by a qname. For example:
     * [
     *     'acme:article' => [
     *         'class' => 'Gdbots\Ncr\Repository\DynamoDb\NodeTable',
     *         'table_name' => 'article'
     *     ]
     * ]
     *
     * @var array
     */
    private $nodeTables = [];

    /** @var NodeTable[] */
    private $instances = [];

    /**
     * @param string $tableNamePrefix
     * @param array  $nodeTables
     */
    public function __construct($tableNamePrefix, array $nodeTables)
    {
        $this->nodeTables = $nodeTables;

        if (!isset($this->nodeTables['default'])) {
            $this->nodeTables['default'] = [
                'class' => 'Gdbots\Ncr\Repository\DynamoDb\NodeTable',
                'table_name' => 'multi'
            ];
        }
    }

    /**
     * @param SchemaQName $qname
     *
     * @return NodeTable
     */
    public function getNodeTable(SchemaQName $qname)
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
     * @param SchemaQName $qname
     * @param array $hints
     *
     * @return string
     */
    public function getNodeTableName(SchemaQName $qname, array $hints = [])
    {
        return 'test';
    }
}
