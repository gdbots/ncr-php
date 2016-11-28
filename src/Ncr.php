<?php

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
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
     * @param array       $hints Data that helps the implementation decide where to create the storage.
     *
     * @return string
     */
    public function describeStorage(SchemaQName $qname, array $hints = []);

    /**
     * @param NodeRef $nodeRef
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $hints      Data that helps the repository decide where to read/write data from.
     *
     * @return Node
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getByNodeRef(NodeRef $nodeRef, $consistent = false, array $hints = []);

    /**
     * @param Node   $node
     * @param string $expectedEtag Used to perform optimistic concurrency check.
     * @param array  $hints        Data that helps the repository decide where to read/write data from.
     *
     * @throws OptimisticCheckFailed
     * @throws GdbotsNcrException
     */
    //public function putNode(Node $node, $expectedEtag = null, array $hints = []);

    /**
     * @param NodeRef $nodeRef
     * @param array   $hints   Data that helps the repository decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    //public function deleteNode(NodeRef $nodeRef, array $hints = []);
}
