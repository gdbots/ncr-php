<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Repository;

use Gdbots\Ncr\Exception\NodeNotFound;
use Gdbots\Ncr\IndexQuery;
use Gdbots\Ncr\IndexQueryResult;
use Gdbots\Ncr\Ncr;
use Gdbots\Pbj\Serializer\PhpArraySerializer;
use Gdbots\Schemas\Ncr\Mixin\Node\Node;
use Gdbots\Schemas\Ncr\NodeRef;

/**
 * NCR which runs entirely in memory, typically used for unit tests.
 */
class InMemoryNcr implements Ncr
{
    use LocalNodeCacheTrait;

    /** @var PhpArraySerializer */
    private static $serializer;

    /**
     * @param Node[]|array $nodes
     */
    public function __construct(array $nodes = [])
    {
        foreach ($nodes as $node) {
            try {
                if (!$node instanceof Node) {
                    $node = $this->createNodeFromArray($node);
                }

                $nodeRef = NodeRef::fromNode($node);
                $this->addToNodeCache($nodeRef, $node);
            } catch (\Exception $e) {
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function hasNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): bool
    {
        return $this->isInNodeCache($nodeRef);
    }

    /**
     * {@inheritdoc}
     */
    public function getNode(NodeRef $nodeRef, bool $consistent = false, array $hints = []): Node
    {
        if (!$this->isInNodeCache($nodeRef)) {
            throw NodeNotFound::forNodeRef($nodeRef);
        }

        return $this->getFromNodeCache($nodeRef);
    }

    /**
     * {@inheritdoc}
     */
    public function putNode(Node $node, ?string $expectedEtag = null, array $hints = []): void
    {
        $node->freeze();
        $nodeRef = NodeRef::fromNode($node);
        $this->addToNodeCache($nodeRef, $node);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteNode(NodeRef $nodeRef, array $hints = []): void
    {
        $this->removeFromNodeCache($nodeRef);
    }

    /**
     * {@inheritdoc}
     */
    public function findNodeRefs(IndexQuery $query, array $hints = []): IndexQueryResult
    {
        // fixme: handle findNodeRefs in memory
        return new IndexQueryResult($query);
    }

    /**
     * @param array $data
     *
     * @return Node
     */
    private function createNodeFromArray(array $data = []): Node
    {
        if (null === self::$serializer) {
            self::$serializer = new PhpArraySerializer();
        }

        /** @var Node $node */
        $node = self::$serializer->deserialize($data);
        return $node;
    }
}
