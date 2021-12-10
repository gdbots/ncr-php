<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\Exception\RepositoryIndexNotFound;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;

interface Ncr
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
     * @param NodeRef $nodeRef    The NodeRef to check for in the NCR.
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $context    Data that helps the NCR decide where to read/write data from.
     *
     * @return bool
     *
     * @throws GdbotsNcrException
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): bool;

    /**
     * @param NodeRef $nodeRef    The NodeRef to get from the NCR.
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $context    Data that helps the NCR decide where to read/write data from.
     *
     * @return Message
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Message;

    /**
     * @param NodeRef[] $nodeRefs   An array of NodeRefs to get from the NCR.
     * @param bool      $consistent An eventually consistent read is used by default unless this is true.
     * @param array     $context    Data that helps the NCR decide where to read/write data from.
     *
     * @return Message[]
     *
     * @throws GdbotsNcrException
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array;

    /**
     * @param Message     $node         The Node to put into the NCR.
     * @param string|null $expectedEtag Used to perform optimistic concurrency check.
     * @param array       $context      Data that helps the NCR decide where to read/write data from.
     *
     * @throws OptimisticCheckFailed
     * @throws GdbotsNcrException
     */
    public function putNode(Message $node, ?string $expectedEtag = null, array $context = []): void;

    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NCR.
     * @param array   $context Data that helps the NCR decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function deleteNode(NodeRef $nodeRef, array $context = []): void;

    /**
     * @param IndexQuery $query   The IndexQuery to use to find NodeRefs.
     * @param array      $context Data that helps the NCR decide where to read/write data from.
     *
     * @return IndexQueryResult
     *
     * @throws RepositoryIndexNotFound
     * @throws GdbotsNcrException
     */
    public function findNodeRefs(IndexQuery $query, array $context = []): IndexQueryResult;

    /**
     * Reads nodes from the NCR (unordered).
     *
     * @param SchemaQName $qname
     * @param array       $context Data that helps the NCR decide where to read/write data from.
     *
     * @return \Generator
     *
     * @throws GdbotsNcrException
     */
    public function pipeNodes(SchemaQName $qname, array $context = []): \Generator;

    /**
     * Reads nodeRefs from the NCR (unordered).
     *
     * @param SchemaQName $qname
     * @param array       $context Data that helps the NCR decide where to read/write data from.
     *
     * @return \Generator
     *
     * @throws GdbotsNcrException
     */
    public function pipeNodeRefs(SchemaQName $qname, array $context = []): \Generator;
}
