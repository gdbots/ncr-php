<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Schemas\Ncr\Enum\NodeStatus;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

/**
 * NcrPreloader provides a way to inform the current request
 * that you want certain nodes to be available at some point
 * before the request is finished. This is mostly for ensuring
 * an envelope has derefs or initial state in a javascript client
 * application is populated.
 *
 * It uses the NcrLazyLoader and NcrCache to accomplish the
 * task but will not flush its own record of preloaded nodes
 * once the lazy loading has finished. This is the key
 * difference between the preloader and the lazy loader.
 *
 */
final class NcrPreloader
{
    /** @var NcrLazyLoader */
    private $lazyLoader;

    /** @var NcrCache */
    private $ncrCache;

    /** @var NodeRef[] */
    private $nodeRefs = [];

    /**
     * @param NcrLazyLoader $lazyLoader
     * @param NcrCache      $ncrCache
     */
    public function __construct(NcrLazyLoader $lazyLoader, NcrCache $ncrCache)
    {
        $this->lazyLoader = $lazyLoader;
        $this->ncrCache = $ncrCache;
    }

    /**
     * @param callable $filter - A function with signature "func(Node $node, NodeRef $nodeRef): bool"
     *
     * @return Node[]
     */
    public function getNodes(?callable $filter = null): array
    {
        $nodes = [];

        foreach ($this->nodeRefs as $key => $nodeRef) {
            try {
                $node = $this->ncrCache->getNode($nodeRef);
                if (null === $filter || $filter($node, $nodeRef)) {
                    $nodes[$key] = $node;
                }
            } catch (\Throwable $e) {
                // if you don't node me by now, you will never never never node me.
            }
        }

        return $nodes;
    }

    /**
     * @return Node[]
     */
    public function getPublishedNodes(): array
    {
        $published = NodeStatus::PUBLISHED();
        return $this->getNodes(function (Node $node) use ($published) {
            return $published->equals($node->get('status'));
        });
    }

    /**
     * @param NodeRef $nodeRef
     *
     * @return bool
     */
    public function hasNodeRef(NodeRef $nodeRef): bool
    {
        return isset($this->nodeRefs[$nodeRef->toString()]);
    }

    /**
     * @param NodeRef $nodeRef
     */
    public function addNodeRef(NodeRef $nodeRef): void
    {
        if (!$this->ncrCache->hasNode($nodeRef)) {
            $this->lazyLoader->addNodeRefs([$nodeRef]);
        }

        $this->nodeRefs[$nodeRef->toString()] = $nodeRef;
    }

    /**
     * @param NodeRef[] $nodeRefs
     */
    public function addNodeRefs(array $nodeRefs): void
    {
        foreach ($nodeRefs as $nodeRef) {
            $this->addNodeRef($nodeRef);
        }
    }

    /**
     * @param NodeRef $nodeRef
     */
    public function removeNodeRef(NodeRef $nodeRef): void
    {
        unset($this->nodeRefs[$nodeRef->toString()]);
    }

    /**
     * Clears the preloaded node refs.
     */
    public function clear(): void
    {
        $this->nodeRefs = [];
    }
}
