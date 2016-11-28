<?php

namespace Gdbots\Ncr\Repository;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Gdbots\Common\Util\ClassUtils;
use Gdbots\Ncr\Exception\RepositoryOperationFailed;
use Gdbots\Schemas\Pbjx\Enum\Code;

/**
 * Creates the DynamoDb table schema for a NodeRepository.
 *
 * You can customize the Global Secondary Indexes by extending this
 * class and overriding the "getAttributeDefinitions" and
 * "getGlobalSecondaryIndexes" methods.
 *
 * For details on the format of the parameters:
 * @link http://docs.aws.amazon.com/aws-sdk-php/v2/api/class-Aws.DynamoDb.DynamoDbClient.html#_createTable
 *
 * NOTE: This will not update a table, it only handles creation.
 *
 */
class DynamoDbNodeTableSchema
{
    const SCHEMA_VERSION = 'v1';
    const HASH_KEY_NAME = '__node_ref';

    /**
     * The tables are constructed with "new $class" in the
     * repository so the constructor must be consistent.
     */
    final public function __construct()
    {
    }

    /**
     * Creates a DynamoDb table with the node schema.
     *
     * @param DynamoDbClient $client
     * @param string $tableName
     *
     * @throws RepositoryOperationFailed
     */
    final public function create(DynamoDbClient $client, $tableName)
    {
        try {
            $client->describeTable(['TableName' => $tableName]);
            return;
        } catch (DynamoDbException $e)  {
            // table doesn't exist, create it below
        }

        $attributeDefinitions = $this->getAttributeDefinitions() ?: [];
        $attributeDefinitions[] = ['AttributeName' => self::HASH_KEY_NAME, 'AttributeType' => 'S'];

        try {
            /*$client->createTable(*/echo json_encode([
                'TableName' => $tableName,
                'AttributeDefinitions' => $attributeDefinitions,
                'KeySchema' => [
                    ['AttributeName' => self::HASH_KEY_NAME, 'KeyType' => 'HASH'],
                ],
                'GlobalSecondaryIndexes' => $this->getGlobalSecondaryIndexes(),
                'StreamSpecification' => [
                    'StreamEnabled' => true,
                    'StreamViewType' => 'NEW_AND_OLD_IMAGES',
                ],
                'ProvisionedThroughput' => [
                    'ReadCapacityUnits'  => 2,
                    'WriteCapacityUnits' => 2
                ]
            ], JSON_PRETTY_PRINT);

            //$client->waitUntil('TableExists', ['TableName' => $tableName]);

        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to create table [%s] in region [%s].',
                    ClassUtils::getShortName($tableName),
                    $tableName,
                    $client->getRegion()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    /**
     * Describes a DynamoDb table.
     *
     * @param DynamoDbClient $client
     * @param string $tableName
     *
     * @return string
     *
     * @throws RepositoryOperationFailed
     */
    final public function describe(DynamoDbClient $client, $tableName)
    {
        try {
            $result = $client->describeTable(['TableName' => $tableName]);
            return json_encode($result->toArray(), JSON_PRETTY_PRINT);
        } catch (\Exception $e) {
            throw new RepositoryOperationFailed(
                sprintf(
                    '%s::Unable to describe table [%s] in region [%s].',
                    ClassUtils::getShortName($tableName),
                    $tableName,
                    $client->getRegion()
                ),
                Code::INTERNAL,
                $e
            );
        }
    }

    /**
     * @return array
     */
    protected function getAttributeDefinitions()
    {
        return [];
    }

    /**
     * @return array
     */
    protected function getGlobalSecondaryIndexes()
    {
        return [];
    }
}
