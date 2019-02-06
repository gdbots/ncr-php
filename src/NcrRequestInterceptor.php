<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbjx\Event\GetResponseEvent;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\Event\ResponseCreatedEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\NodeRef;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;

final class NcrRequestInterceptor implements EventSubscriber
{
    private const SLUG_TTL = 300;

    /** @var CacheItemPoolInterface */
    private $cache;

    /** @var NcrCache */
    private $ncrCache;

    /**
     * If a request for nodes contains items already available in NcrCache
     * then it will store the NodeRefs here and pick them up from cache
     * on the response created handler.
     *
     * @var array
     */
    private $pickup = [];

    /**
     * When get node requests occur with slugs we will attempt
     * to cache the slug -> node_ref, these are there stories...
     * boom boom.
     *
     * @var CacheItemInterface[]
     */
    private $cacheItems = [];

    /**
     * @param CacheItemPoolInterface $cache
     * @param NcrCache               $ncrCache
     */
    public function __construct(CacheItemPoolInterface $cache, NcrCache $ncrCache)
    {
        $this->cache = $cache;
        $this->ncrCache = $ncrCache;
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:get-node-request.enrich'              => 'enrichGetNodeRequest',
            'gdbots:ncr:mixin:get-node-batch-request.before_handle' => 'onGetNodeBatchRequestBeforeHandle',
            'gdbots:ncr:mixin:get-node-response.created'            => 'onGetNodeReponseCreated',
            'gdbots:ncr:mixin:get-node-batch-response.created'      => 'onGetNodeBatchReponseCreated',
        ];
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

    /**
     * @param GetResponseEvent $pbjxEvent
     */
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

    /**
     * @param ResponseCreatedEvent $pbjxEvent
     */
    public function onGetNodeReponseCreated(ResponseCreatedEvent $pbjxEvent): void
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
        $cacheItem->set($nodeRef->toString())->expiresAfter(self::SLUG_TTL);
        unset($this->cacheItems[$cacheKey]);
        $this->cache->saveDeferred($cacheItem);
    }

    /**
     * @param ResponseCreatedEvent $pbjxEvent
     */
    public function onGetNodeBatchReponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if ($response->has('nodes')) {
            $this->ncrCache->addNodes($response->get('nodes'));
        }

        $requestId = $response->get('ctx_request_ref')->getId();
        if (isset($this->pickup[$requestId])) {
            /** @var NodeRef $nodeRef */
            foreach ($this->pickup[$requestId] as $nodeRef) {
                $response->addToMap('nodes', $nodeRef->toString(), $this->ncrCache->getNode($nodeRef));
            }

            unset($this->pickup[$requestId]);
        }
    }

    /**
     * @param SchemaQName $qname
     * @param string      $slug
     *
     * @return string
     */
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
