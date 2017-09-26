<?php
declare(strict_types=1);

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
     * @return Node
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $context = []): Node;

    /**
     * @param NodeRef[] $nodeRefs   An array of NodeRefs to get from the NCR.
     * @param bool      $consistent An eventually consistent read is used by default unless this is true.
     * @param array     $context    Data that helps the NCR decide where to read/write data from.
     *
     * @return Node[]
     *
     * @throws GdbotsNcrException
     */
    public function getNodes(array $nodeRefs, bool $consistent = false, array $context = []): array;

    /**
     * @param Node   $node         The Node to put into the NCR.
     * @param string $expectedEtag Used to perform optimistic concurrency check.
     * @param array  $context      Data that helps the NCR decide where to read/write data from.
     *
     * @throws OptimisticCheckFailed
     * @throws GdbotsNcrException
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $context = []): void;

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
     * Reads nodes from the NCR (unordered) and executes the $receiver for every
     * node returned, i.e. "$receiver($node);".
     *
     * @param SchemaQName $qname
     * @param callable    $receiver The callable that will receive the node. "function f(Node $node)".
     * @param array       $context  Data that helps the NCR decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function pipeNodes(SchemaQName $qname, callable $receiver, array $context = []): void;

    /**
     * Reads nodeRefs from the NCR (unordered) and executes the $receiver for every
     * NodeRef returned, i.e. "$receiver($nodeRef);".
     *
     * @param SchemaQName $qname
     * @param callable    $receiver The callable that will receive the NodeRef. "function f(NodeRef $nodeRef)".
     * @param array       $context  Data that helps the NCR decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    public function pipeNodeRefs(SchemaQName $qname, callable $receiver, array $context = []): void;
}
