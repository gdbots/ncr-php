<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Event\GetResponseEvent;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\Event\ResponseCreatedEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\Mixin\GetNodeBatchRequest\GetNodeBatchRequestV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\GetNodeBatchResponse\GetNodeBatchResponseV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\GetNodeRequest\GetNodeRequestV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\GetNodeResponse\GetNodeResponseV1Mixin;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

final class NcrRequestInterceptor implements EventSubscriber
{
    private const SLUG_TTL = 300;
    private CacheItemPoolInterface $cache;
    private NcrCache $ncrCache;

    /**
     * If a request for nodes contains items already available in NcrCache
     * then it will store the NodeRefs here and pick them up from cache
     * on the response created handler.
     *
     * @var array
     */
    private array $pickup = [];

    /**
     * When get node requests occur with slugs we will attempt
     * to cache the slug -> node_ref, these are there stories...
     * boom boom.
     *
     * @var CacheItemInterface[]
     */
    private array $cacheItems = [];

    public static function getSubscribedEvents()
    {
        return [
            GetNodeRequestV1Mixin::SCHEMA_CURIE . '.enrich'             => 'enrichGetNodeRequest',
            GetNodeBatchRequestV1Mixin::SCHEMA_CURIE . '.before_handle' => 'onGetNodeBatchRequestBeforeHandle',
            GetNodeResponseV1Mixin::SCHEMA_CURIE . '.created'           => 'onGetNodeResponseCreated',
            GetNodeBatchResponseV1Mixin::SCHEMA_CURIE . '.created'      => 'onGetNodeBatchResponseCreated',
        ];
    }

    public function __construct(CacheItemPoolInterface $cache, NcrCache $ncrCache)
    {
        $this->cache = $cache;
        $this->ncrCache = $ncrCache;
    }

    /**
     * Enrich the request with a node_ref if possible to minimize
     * the number of lookups against the slug secondary index.
     *
     * @param PbjxEvent $pbjxEvent
     */
    public function enrichGetNodeRequest(PbjxEvent $pbjxEvent): void
    {
        $request = $pbjxEvent->getMessage();
        if ($request->has(GetNodeRequestV1Mixin::NODE_REF_FIELD)
            || $request->get(GetNodeRequestV1Mixin::CONSISTENT_READ_FIELD)
            || !$request->has(GetNodeRequestV1Mixin::QNAME_FIELD)
            || !$request->has(GetNodeRequestV1Mixin::SLUG_FIELD)
        ) {
            return;
        }

        $qname = SchemaQName::fromString($request->get(GetNodeRequestV1Mixin::QNAME_FIELD));
        $cacheKey = $this->getSlugCacheKey($qname, $request->get(GetNodeRequestV1Mixin::SLUG_FIELD));

        $cacheItem = $this->cache->getItem($cacheKey);
        if (!$cacheItem->isHit()) {
            $this->cacheItems[$cacheKey] = $cacheItem;
            return;
        }

        try {
            $nodeRef = NodeRef::fromString($cacheItem->get());
            if ($nodeRef->getQName() === $qname) {
                $request->set(GetNodeRequestV1Mixin::NODE_REF_FIELD, $nodeRef);
            }
        } catch (\Throwable $e) {
        }
    }

    public function onGetNodeBatchRequestBeforeHandle(GetResponseEvent $pbjxEvent): void
    {
        $request = $pbjxEvent->getRequest();
        if ($request->get(GetNodeBatchRequestV1Mixin::CONSISTENT_READ_FIELD)
            || !$request->has(GetNodeBatchRequestV1Mixin::NODE_REFS_FIELD)
        ) {
            return;
        }

        $ncrCachedNodeRefs = [];

        /** @var NodeRef $nodeRef */
        foreach ($request->get(GetNodeBatchRequestV1Mixin::NODE_REFS_FIELD) as $nodeRef) {
            if ($this->ncrCache->hasNode($nodeRef)) {
                $ncrCachedNodeRefs[] = $nodeRef;
            }
        }

        if (empty($ncrCachedNodeRefs)) {
            return;
        }

        $request->removeFromSet(GetNodeBatchRequestV1Mixin::NODE_REFS_FIELD, $ncrCachedNodeRefs);
        $this->pickup[(string)$request->get('request_id')] = $ncrCachedNodeRefs;
    }

    public function onGetNodeResponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if (!$response->has(GetNodeResponseV1Mixin::NODE_FIELD)) {
            return;
        }

        $node = $response->get(GetNodeResponseV1Mixin::NODE_FIELD);
        $this->ncrCache->addNode($node);

        $request = $pbjxEvent->getRequest();
        if (!$request->has(GetNodeRequestV1Mixin::QNAME_FIELD)
            || !$request->has(GetNodeRequestV1Mixin::SLUG_FIELD)
        ) {
            // was not a slug lookup
            return;
        }

        if ($node->get(GetNodeRequestV1Mixin::SLUG_FIELD) !== $request->get(GetNodeRequestV1Mixin::SLUG_FIELD)) {
            // for some reason, the node we got ain't the one we want
            return;
        }

        $nodeRef = NodeRef::fromNode($node);
        $cacheKey = $this->getSlugCacheKey($nodeRef->getQName(), $request->get(GetNodeRequestV1Mixin::SLUG_FIELD));
        if (!isset($this->cacheItems[$cacheKey])) {
            // lookup never happend
            return;
        }

        $cacheItem = $this->cacheItems[$cacheKey];
        $cacheItem->set($nodeRef->toString())->expiresAfter(self::SLUG_TTL);
        unset($this->cacheItems[$cacheKey]);
        $this->cache->saveDeferred($cacheItem);
    }

    public function onGetNodeBatchResponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if ($response->has(GetNodeBatchResponseV1Mixin::NODES_FIELD)) {
            $this->ncrCache->addNodes($response->get(GetNodeBatchResponseV1Mixin::NODES_FIELD));
        }

        $requestId = $response->get('ctx_request_ref')->getId();
        if (!isset($this->pickup[$requestId])) {
            return;
        }

        /** @var NodeRef $nodeRef */
        foreach ($this->pickup[$requestId] as $nodeRef) {
            try {
                $response->addToMap(
                    GetNodeBatchResponseV1Mixin::NODES_FIELD,
                    $nodeRef->toString(),
                    $this->ncrCache->getNode($nodeRef)
                );
            } catch (\Throwable $e) {
                $response->addToSet(GetNodeBatchResponseV1Mixin::MISSING_NODE_REFS_FIELD, [$nodeRef]);
            }
        }

        unset($this->pickup[$requestId]);
    }

    private function getSlugCacheKey(SchemaQName $qname, string $slug): string
    {
        return str_replace('-', '_', sprintf(
            'stnr.%s.%s.%s',
            $qname->getVendor(),
            $qname->getMessage(),
            md5($slug)
        ));
    }
}
