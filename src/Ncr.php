<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Ncr\Exception\RepositoryIndexNotFound;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

interface Ncr
{
    /**
     * Creates the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $hints Data that helps the implementation decide where to create the storage.
     */
    public function createStorage(SchemaQName $qname, array $hints = []);

    /**
     * Returns debugging information about the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $hints Data that helps the implementation decide what storage to describe.
     *
     * @return string
     */
    public function describeStorage(SchemaQName $qname, array $hints = []): string;

    /**
     * @param NodeRef $nodeRef    The NodeRef to check for in the NCR.
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $hints      Data that helps the NCR decide where to read/write data from.
     *
     * @return bool
     *
     * @throws GdbotsNcrException
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): bool;

    /**
     * @param NodeRef $nodeRef    The NodeRef to get from the NCR.
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $hints      Data that helps the NCR decide where to read/write data from.
     *
     * @return Node
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): Node;
    //public function getNodeBatch(array $nodeRefs, $consistent = false, array $hints = []);

    /**
     * @param Node   $node         The Node to put into the NCR.
     * @param string $expectedEtag Used to perform optimistic concurrency check.
     * @param array  $hints        Data that helps the NCR decide where to read/write data from.
     *
     * @throws OptimisticCheckFailed
     * @throws GdbotsNcrException
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void;

    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NCR.
     * @param array   $hints   Data that helps the NCR decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function deleteNode(NodeRef $nodeRef, array $hints = []): void;

    /**
     * @param IndexQuery $query The IndexQuery to use to find NodeRefs.
     * @param array      $hints Data that helps the NCR decide where to read/write data from.
     *
     * @return IndexQueryResult
     *
     * @throws RepositoryIndexNotFound
     * @throws GdbotsNcrException
     */
    public function findNodeRefs(IndexQuery $query, array $hints = []): IndexQueryResult;
}
