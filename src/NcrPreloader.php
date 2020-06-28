<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\Util\ArrayUtil;
use Gdbots\Pbj\WellKnown\MessageRef;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Schemas\Ncr\Enum\NodeStatus;

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
    const DEFAULT_NAMESPACE = 'default';
    private NcrLazyLoader $lazyLoader;
    private NcrCache $ncrCache;

    /**
     * Array of node refs keyed within a namespace.
     * e.g. ['default' => ['acme:article:123' => NodeRef]]
     *
     * @var array
     */
    private array $nodeRefs = [];

    public function __construct(NcrLazyLoader $lazyLoader, NcrCache $ncrCache)
    {
        $this->lazyLoader = $lazyLoader;
        $this->ncrCache = $ncrCache;
    }

    /**
     * @param callable $filter - A function with signature "func(Message $node, NodeRef $nodeRef): bool"
     * @param string   $namespace
     *
     * @return Message[]
     */
    public function getNodes(?callable $filter = null, string $namespace = self::DEFAULT_NAMESPACE): array
    {
        $nodes = [];
        $nodeRefs = $this->nodeRefs[$namespace] ?? [];

        foreach ($nodeRefs as $key => $nodeRef) {
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
     * @param string $namespace
     *
     * @return Message[]
     */
    public function getPublishedNodes(string $namespace = self::DEFAULT_NAMESPACE): array
    {
        $published = NodeStatus::PUBLISHED();
        return $this->getNodes(function (Message $node) use ($published) {
            return $published->equals($node->get($node::STATUS_FIELD));
        }, $namespace);
    }

    public function hasNodeRef(NodeRef $nodeRef, string $namespace = self::DEFAULT_NAMESPACE): bool
    {
        if (!isset($this->nodeRefs[$namespace])) {
            return false;
        }

        return isset($nodeRefs[$namespace][$nodeRef->toString()]);
    }

    /**
     * @param string $namespace
     *
     * @return NodeRef[]
     */
    public function getNodeRefs(string $namespace = self::DEFAULT_NAMESPACE): array
    {
        return array_values($this->nodeRefs[$namespace] ?? []);
    }

    public function addNodeRef(NodeRef $nodeRef, string $namespace = self::DEFAULT_NAMESPACE): void
    {
        if (!$this->ncrCache->hasNode($nodeRef)) {
            $this->lazyLoader->addNodeRefs([$nodeRef]);
        }

        if (!isset($this->nodeRefs[$namespace])) {
            $this->nodeRefs[$namespace] = [];
        }

        $this->nodeRefs[$namespace][$nodeRef->toString()] = $nodeRef;
    }

    /**
     * @param NodeRef[] $nodeRefs
     * @param string    $namespace
     */
    public function addNodeRefs(array $nodeRefs, string $namespace = self::DEFAULT_NAMESPACE): void
    {
        foreach ($nodeRefs as $nodeRef) {
            $this->addNodeRef($nodeRef, $namespace);
        }
    }

    /**
     * Finds NodeRefs in the provided messages based on the paths provided.  The paths
     * is an array of ['field_name' => 'qname'] or ['field1', 'field2'] which will be used
     * to create the NodeRefs if the field is populated on any of the messages.
     *
     * @param Message[] $messages Array of messages to extract NodeRefs message.
     * @param array     $paths    An associative array of ['field_name' => 'qname'], i.e. ['user_id', 'acme:user']
     *                            or an array of field names ['user_ref', 'category_ref']
     * @param string    $namespace
     */
    public function addEmbeddedNodeRefs(array $messages, array $paths, string $namespace = self::DEFAULT_NAMESPACE): void
    {
        $nodeRefs = [];

        if (!ArrayUtil::isAssoc($paths)) {
            $paths = array_flip($paths);
        }

        foreach ($messages as $message) {
            foreach ($paths as $fieldName => $qname) {
                if (!$message->has($fieldName)) {
                    continue;
                }

                $values = $message->get($fieldName);
                if (!is_array($values)) {
                    $values = [$values];
                }

                foreach ($values as $value) {
                    if ($value instanceof NodeRef) {
                        $nodeRefs[] = $value;
                    } elseif ($value instanceof MessageRef) {
                        $nodeRefs[] = NodeRef::fromMessageRef($value);
                    } elseif ($value instanceof Message) {
                        $nodeRefs[] = $value->generateNodeRef();
                    } else {
                        $nodeRefs[] = NodeRef::fromString("{$qname}:{$value}");
                    }
                }
            }
        }

        $this->addNodeRefs($nodeRefs, $namespace);
    }

    public function removeNodeRef(NodeRef $nodeRef, string $namespace = self::DEFAULT_NAMESPACE): void
    {
        if (!isset($this->nodeRefs[$namespace])) {
            return;
        }

        unset($this->nodeRefs[$namespace][$nodeRef->toString()]);
    }

    /**
     * Clears the preloaded node refs.
     *
     * @param string $namespace
     */
    public function clear(?string $namespace = self::DEFAULT_NAMESPACE): void
    {
        if (null === $namespace) {
            $this->nodeRefs = [];
            return;
        }

        unset($this->nodeRefs[$namespace]);
    }
}
