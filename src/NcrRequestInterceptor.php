<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Event\GetResponseEvent;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\Event\ResponseCreatedEvent;
use Gdbots\Pbjx\EventSubscriber;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

class NcrRequestInterceptor implements EventSubscriber
{
    protected CacheItemPoolInterface $cache;
    protected NcrCache $ncrCache;

    /**
     * If a request for nodes contains items already available in NcrCache
     * then it will store the NodeRefs here and pick them up from cache
     * on the response created handler.
     *
     * @var array
     */
    protected array $pickup = [];

    /**
     * When get node requests occur with slugs we will attempt
     * to cache the slug -> node_ref, these are there stories...
     * boom boom.
     *
     * @var CacheItemInterface[]
     */
    protected array $cacheItems = [];

    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:get-node-request.enrich'              => 'enrichGetNodeRequest',
            'gdbots:ncr:mixin:get-node-batch-request.before_handle' => 'onGetNodeBatchRequestBeforeHandle',
            'gdbots:ncr:mixin:get-node-response.created'            => 'onGetNodeResponseCreated',
            'gdbots:ncr:mixin:get-node-batch-response.created'      => 'onGetNodeBatchResponseCreated',
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
        if ($request->has('node_ref')
            || $request->get('consistent_read')
            || !$request->has('qname')
            || !$request->has('slug')
        ) {
            return;
        }

        $qname = SchemaQName::fromString($request->get('qname'));
        $cacheKey = $this->getSlugCacheKey($qname, $request->get('slug'));

        $cacheItem = $this->cache->getItem($cacheKey);
        if (!$cacheItem->isHit()) {
            $this->cacheItems[$cacheKey] = $cacheItem;
            return;
        }

        try {
            $nodeRef = NodeRef::fromString($cacheItem->get());
            if ($nodeRef->getQName() === $qname) {
                $request->set('node_ref', $nodeRef);
            }
        } catch (\Throwable $e) {
        }
    }

    public function onGetNodeBatchRequestBeforeHandle(GetResponseEvent $pbjxEvent): void
    {
        $request = $pbjxEvent->getRequest();
        if ($request->get('consistent_read') || !$request->has('node_refs')) {
            return;
        }

        $ncrCachedNodeRefs = [];

        /** @var NodeRef $nodeRef */
        foreach ($request->get('node_refs') as $nodeRef) {
            if ($this->ncrCache->hasNode($nodeRef)) {
                $ncrCachedNodeRefs[] = $nodeRef;
            }
        }

        if (empty($ncrCachedNodeRefs)) {
            return;
        }

        $request->removeFromSet('node_refs', $ncrCachedNodeRefs);
        $this->pickup[(string)$request->get('request_id')] = $ncrCachedNodeRefs;
    }

    public function onGetNodeResponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if (!$response->has('node')) {
            return;
        }

        $node = $response->get('node');
        $this->ncrCache->addNode($node);

        $request = $pbjxEvent->getRequest();
        if (!$request->has('qname') || !$request->has('slug')) {
            // was not a slug lookup
            return;
        }

        if ($node->get('slug') !== $request->get('slug')) {
            // for some reason, the node we got ain't the one we want
            return;
        }

        $nodeRef = NodeRef::fromNode($node);
        $cacheKey = $this->getSlugCacheKey($nodeRef->getQName(), $request->get('slug'));
        if (!isset($this->cacheItems[$cacheKey])) {
            // lookup never happend
            return;
        }

        $cacheItem = $this->cacheItems[$cacheKey];
        $cacheItem->set($nodeRef->toString())->expiresAfter($this->getSlugCacheTtl($nodeRef->getQName(), $request));
        unset($this->cacheItems[$cacheKey]);
        $this->cache->saveDeferred($cacheItem);
    }

    public function onGetNodeBatchResponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if ($response->has('nodes')) {
            $this->ncrCache->addNodes($response->get('nodes'));
        }

        $requestId = $response->get('ctx_request_ref')->getId();
        if (!isset($this->pickup[$requestId])) {
            return;
        }

        /** @var NodeRef $nodeRef */
        foreach ($this->pickup[$requestId] as $nodeRef) {
            try {
                $response->addToMap('nodes', $nodeRef->toString(), $this->ncrCache->getNode($nodeRef));
            } catch (\Throwable $e) {
                $response->addToSet('missing_node_refs', [$nodeRef]);
            }
        }

        unset($this->pickup[$requestId]);
    }

    protected function getSlugCacheKey(SchemaQName $qname, string $slug): string
    {
        return str_replace('-', '_', sprintf(
            'stnr.%s.%s.%s',
            $qname->getVendor(),
            $qname->getMessage(),
            md5($slug)
        ));
    }

    protected function getSlugCacheTtl(SchemaQName $qname, Message $request): int
    {
        return 300;
    }
}
