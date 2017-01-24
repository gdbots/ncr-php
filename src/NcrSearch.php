<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Pbj\SchemaQName;
use Gdbots\QueryParser\ParsedQuery;
use Gdbots\Schemas\Ncr\Mixin\Indexed\Indexed;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesRequest\SearchNodesRequest;
use Gdbots\Schemas\Ncr\Mixin\SearchNodesResponse\SearchNodesResponse;

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
     * @param Indexed[] $nodes   An array of Nodes to add to the search index.
     * @param array     $context Data that helps the NCR Search decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function indexNodes(array $nodes, array $context = []): void;

    /**
     * Executes a search request and populates the provided response object with
     * the nodes found, total, time_taken, etc.
     *
     * @param SearchNodesRequest  $request      Search request containing pagination, date filters, etc.
     * @param ParsedQuery         $parsedQuery  Parsed version of the search query (the "q" field of the request).
     * @param SearchNodesResponse $response     Results from search will be added to this object.
     * @param SchemaQName[]       $qnames       An array of curies that the search should limit its search to.
     *                                          If empty, it will search all nodes in all indexes.
     * @param array               $context      Data that helps the NCR Search decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function searchNodes(SearchNodesRequest $request, ParsedQuery $parsedQuery, SearchNodesResponse $response, array $qnames = [], array $context = []): void;
}
