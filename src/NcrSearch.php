<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\QueryParser\ParsedQuery;

interface NcrSearch
{
    /**
     * Creates the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $context Data that helps the implementation decide where to create the storage.
     */
    public function createStorage(SchemaQName $qname, array $context = []): void;

    /**
     * Returns debugging information about the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $context Data that helps the implementation decide what storage to describe.
     *
     * @return string
     */
    public function describeStorage(SchemaQName $qname, array $context = []): string;

    /**
     * @param Message[] $nodes   An array of Nodes to add to the search index.
     * @param array     $context Data that helps the NCR Search decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function indexNodes(array $nodes, array $context = []): void;

    /**
     * @param NodeRef[] $nodeRefs An array of NodeRefs to delete from the search index.
     * @param array     $context  Data that helps the NCR Search decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function deleteNodes(array $nodeRefs, array $context = []): void;

    /**
     * Executes a search request and populates the provided response object with
     * the nodes found, total, time_taken, etc.
     *
     * @param Message       $request     Search request containing pagination, date filters, etc.
     * @param ParsedQuery   $parsedQuery Parsed version of the search query (the "q" field of the request).
     * @param Message       $response    Results from search will be added to this object.
     * @param SchemaQName[] $qnames      An array of qnames that the search should limit its search to.
     *                                   If empty, it will search all nodes in all indexes.
     * @param array         $context     Data that helps the NCR Search decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function searchNodes(Message $request, ParsedQuery $parsedQuery, Message $response, array $qnames = [], array $context = []): void;
}
