<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Common\Util\ClassUtils;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Ncr\NodeRef;
use Gdbots\Schemas\Ncr\Request\GetNodeBatchRequestV1;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class NcrLazyLoader
{
    /** @var Pbjx */
    private $pbjx;

    /** @var LoggerInterface */
    private $logger;

    /** @var GetNodeBatchRequestV1 */
    private $getNodeBatchRequest;

    /**
     * @param Pbjx            $pbjx
     * @param LoggerInterface $logger
     */
    public function __construct(Pbjx $pbjx, ?LoggerInterface $logger = null)
    {
        $this->pbjx = $pbjx;
        $this->logger = $logger ?: new NullLogger();
    }

    /**
     * Returns true if the given NodeRef is in the pending lazy load request.
     *
     * @param NodeRef $nodeRef
     *
     * @return bool
     */
    public function hasNodeRef(NodeRef $nodeRef): bool
    {
        if (null === $this->getNodeBatchRequest) {
            return false;
        }

        return $this->getNodeBatchRequest->isInSet('node_refs', $nodeRef);
    }

    /**
     * Returns the NodeRefs that the lazy loader has queued up.
     *
     * @return NodeRef[]
     */
    public function getNodeRefs(): array
    {
        if (null === $this->getNodeBatchRequest) {
            return [];
        }

        return $this->getNodeBatchRequest->get('node_refs', []);
    }

    /**
     * Finds NodeRefs in the provided messages based on the paths provided.  The paths
     * is an array of ['field_name' => 'qname'] which will be used to create the
     * NodeRefs if the field is populated on any of the messages.
     *
     * @param Message[] $messages Array of messages to extract NodeRefs message.
     * @param array     $paths    An associative array of ['field_name' => 'qname'], i.e. ['user_id', 'acme:user']
     */
    public function addEmbeddedNodeRefs(array $messages, array $paths): void
    {
        $nodeRefs = [];

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
                    } else {
                        $nodeRefs[] = NodeRef::fromString("{$qname}:{$value}");
                    }
                }
            }
        }

        $this->addNodeRefs($nodeRefs);
    }

    /**
     * Adds an array of NodeRefs that should be loaded at some point later
     * ONLY if there is a request for a NodeRef that is not already
     * available in first level cache (aka NcrCache).
     *
     * @param NodeRef[] $nodeRefs An array of NodeRefs to lazy load.
     */
    public function addNodeRefs(array $nodeRefs): void
    {
        if (empty($nodeRefs)) {
            return;
        }

        if (null === $this->getNodeBatchRequest) {
            $this->getNodeBatchRequest = GetNodeBatchRequestV1::create();
        }

        $this->getNodeBatchRequest->addToSet('node_refs', $nodeRefs);
    }

    /**
     * Removes an array of NodeRefs from the deferrered request.
     *
     * @param NodeRef[] $nodeRefs
     */
    public function removeNodeRefs(array $nodeRefs): void
    {
        if (null === $this->getNodeBatchRequest) {
            return;
        }

        $this->getNodeBatchRequest->removeFromSet('node_refs', $nodeRefs);
    }

    /**
     * Clears the deferrered requests.
     */
    public function clear(): void
    {
        $this->getNodeBatchRequest = null;
    }

    /**
     * Processes the deferrered request which should populate the
     * NcrCache once complete.  At least for any nodes that exist.
     */
    public function flush(): void
    {
        if (null === $this->getNodeBatchRequest) {
            return;
        }

        if (!$this->getNodeBatchRequest->has('node_refs')) {
            return;
        }

        try {
            $request = $this->getNodeBatchRequest;
            $this->getNodeBatchRequest = null;
            $this->pbjx->request($request);
        } catch (\Exception $e) {
            $this->logger->error(
                sprintf('%s::NcrLazyLoader::flush() could not complete.', ClassUtils::getShortName($e)),
                ['exception' => $e, 'pbj' => $request->toArray()]
            );
        }
    }
}
