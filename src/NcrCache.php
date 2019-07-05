<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Pbj\MessageRef;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

/**
 * NcrCache is a first level cache which is ONLY seen and used by
 * the current request.  It is used to cache all nodes returned
 * from get node request(s).  This cache is used during Pbjx
 * request processing or if the NCR is running in the current
 * process and is using the MemoizingNcr.
 *
 * This cache should not be used when asking for a consistent result.
 *
 * NcrCache is NOT an identity map and the NCR is NOT an ORM. In some
 * cases you may get the same exact object but it's not a guarantee so
 * don't do something like this:
 *  $nodeRef = NodeRef::fromString('acme:article:123');
 *  $cache->getNode($nodeRef) !== $cache->getNode($nodeRef);
 *
 * If you need to check equality, use the message interface:
 *
 * $node1 = $cache->getNode($nodeRef);
 * $node2 = $cache->getNode($nodeRef);
 * $node->equals($node2); // returns true if their data is the same
 *
 */
final class NcrCache
{
    /** @var NcrLazyLoader */
    private $lazyLoader;

    /**
     * Array of nodes keyed by their NodeRef.
     *
     * @var Node[]
     */
    private $nodes = [];

    /**
     * The maximum number of items to keep in cache.
     *  0 means unlimited
     *
     * @var int
     */
    private $maxItems = 1000;

    /**
     * @param NcrLazyLoader $lazyLoader
     * @param int           $maxItems
     */
    public function __construct(NcrLazyLoader $lazyLoader, int $maxItems = 1000)
    {
        $this->lazyLoader = $lazyLoader;
        $this->maxItems = $maxItems;
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to check for in the NcrCache.
     *
     * @return bool
     */
    public function hasNode(NodeRef $nodeRef): bool
    {
        return isset($this->nodes[$nodeRef->toString()]);
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to get from the NcrCache.
     *
     * @return Node
     *
     * @throws NodeNotFound
     */
    public function getNode(NodeRef $nodeRef): Node
    {
        if (!$this->hasNode($nodeRef)) {
            if ($this->lazyLoader->hasNodeRef($nodeRef)) {
                $this->lazyLoader->flush();
                if (!$this->hasNode($nodeRef)) {
                    throw NodeNotFound::forNodeRef($nodeRef);
                }
            } else {
                throw NodeNotFound::forNodeRef($nodeRef);
            }
        }

        $node = $this->nodes[$nodeRef->toString()];
        if ($node->isFrozen()) {
            $node = $this->nodes[$nodeRef->toString()] = clone $node;
        }

        return $node;
    }

    /**
     * @param Node $node The Node to put into the NcrCache.
     */
    public function addNode(Node $node): void
    {
        $this->pruneNodeCache();
        $nodeRef = NodeRef::fromNode($node);
        $this->nodes[$nodeRef->toString()] = $node;
        $this->lazyLoader->removeNodeRefs([$nodeRef]);
    }

    /**
     * @param Node[] $nodes The Nodes to put into the NcrCache.
     */
    public function addNodes(array $nodes): void
    {
        $this->pruneNodeCache();
        $nodeRefs = [];

        foreach ($nodes as $node) {
            $nodeRef = NodeRef::fromNode($node);
            $nodeRefs[] = $nodeRef;
            $this->nodes[$nodeRef->toString()] = $node;
        }

        $this->lazyLoader->removeNodeRefs($nodeRefs);
    }

    /**
     * @param NodeRef $nodeRef The NodeRef to delete from the NcrCache.
     */
    public function removeNode(NodeRef $nodeRef): void
    {
        unset($this->nodes[$nodeRef->toString()]);
        $this->lazyLoader->removeNodeRefs([$nodeRef]);
    }

    /**
     * Dereferences any nodes in the given node that are referenced
     * within the provided fields. e.g. derefNodes(article, ['category_refs', 'image_ref'])
     * would return the category and image nodes associated with the article.
     *
     * @param Node   $node   The node containing the references.
     * @param array  $fields Array of field names containing references you want to dereference to nodes.
     * @param string $return The field name to return from those nodes, or null to get the entire node.
     *
     * @return array
     */
    public function derefNodes(Node $node, array $fields, ?string $return = null): array
    {
        $nodeRefs = [];
        foreach ($fields as $field) {
            if (!$node->has($field)) {
                continue;
            }

            $values = $node->get($field);
            if (empty($values)) {
                continue;
            }

            if (!is_array($values)) {
                $values = [$values];
            }

            foreach ($values as $value) {
                if ($value instanceof NodeRef) {
                    $nodeRefs[$value->toString()] = $value;
                } elseif ($value instanceof MessageRef) {
                    $nodeRef = NodeRef::fromMessageRef($value);
                    $nodeRefs[$nodeRef->toString()] = $nodeRef;
                } elseif ($value instanceof Node) {
                    $nodeRef = NodeRef::fromNode($value);
                    $nodeRefs[$nodeRef->toString()] = $nodeRef;
                    $this->addNode($value);
                } else {
                    $nodeRef = NodeRef::fromString((string)$value);
                    $nodeRefs[$nodeRef->toString()] = $nodeRef;
                }
            }
        }

        $derefs = [];

        if (null === $return) {
            foreach ($nodeRefs as $nodeRef) {
                try {
                    $derefs[] = $this->getNode($nodeRef);
                } catch (\Throwable $e) {
                    // ignore
                }
            }
        } else {
            foreach ($nodeRefs as $nodeRef) {
                try {
                    $node = $this->getNode($nodeRef);
                    if ($node->has($return)) {
                        $derefs[] = $node->get($return);
                    }
                } catch (\Throwable $e) {
                    // ignore
                }
            }
        }

        return $derefs;
    }

    /**
     * Clears the NcrCache.
     */
    public function clear(): void
    {
        $this->nodes = [];
    }

    /**
     * Prunes node cache by removing 20% of the cache if it is full.
     */
    private function pruneNodeCache(): void
    {
        if ($this->maxItems > 0 && count($this->nodes) > $this->maxItems) {
            $this->nodes = array_slice($this->nodes, (int)($this->maxItems * 0.2), null, true);
        }
    }
}
