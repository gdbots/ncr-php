<?php

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\Exception\OptimisticCheckFailed;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

interface Ncr
{
    /**
     * @param NodeRef $nodeRef
     * @param bool    $consistent An eventually consistent read is used by default unless this is true.
     * @param array   $hints      Data that helps the NCR decide where to read/write data from.
     *
     * @return Node
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getNode(NodeRef $nodeRef, $consistent = false, array $hints = []);
    //public function getNodeBatch(array $nodeRefs, $consistent = false, array $hints = []);
    //public function getNodeByIndex(SchemaQName $qname, $index, $value, array $hints = []);
    //public function getNodeBatchByIndex(SchemaQName $qname, $index, array $values, array $hints = []);
    //$this->ncr->getNodeByIndex($qname, 'email', 'test@test.com', [];

    /**
     * @param Node   $node
     * @param string $expectedEtag Used to perform optimistic concurrency check.
     * @param array  $hints        Data that helps the NCR decide where to read/write data from.
     *
     * @throws OptimisticCheckFailed
     * @throws GdbotsNcrException
     */
    public function putNode(Node $node, $expectedEtag = null, array $hints = []);

    /**
     * @param NodeRef $nodeRef
     * @param array   $hints   Data that helps the NCR decide where to read/write data from.
     *
     * @throws GdbotsNcrException
     */
    //public function deleteNode(NodeRef $nodeRef, array $hints = []);
}
