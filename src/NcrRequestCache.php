<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\GdbotsNcrException;
use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

interface NcrRequestCache
{
    /**
     * @param NodeRef $nodeRef The NodeRef to check for in the NCR Cache.
     *
     * @return bool
     *
     * @throws GdbotsNcrException
     */
    public function hasNode(NodeRef $nodeRef): bool;

    /**
     * @param NodeRef $nodeRef The NodeRef to get from the NCR.
     *
     * @return Node
     *
     * @throws NodeNotFound
     * @throws GdbotsNcrException
     */
    public function getNode(NodeRef $nodeRef): Node;

    /**
     * @param Node $node The Node to put into the NCR.
     *
     * @throws GdbotsNcrException
     */
    public function putNode(Node $node): void;
//node = ncr_cache_has(nodeRef), ncr_cache_get(nodeRef);
    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NCR.
     *
     * @throws GdbotsNcrException
     */
    public function deleteNode(NodeRef $nodeRef): void;
}
