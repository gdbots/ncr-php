<?php
declare(strict_types = 1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Event\GetResponseEvent;
use Gdbots\Pbjx\Event\ResponseCreatedEvent;
use Gdbots\Pbjx\EventSubscriber;
use Gdbots\Schemas\Ncr\NodeRef;

final class NcrRequestInterceptor implements EventSubscriber
{
    /** @var NcrCache */
    private $cache;

    /**
     * If a request for nodes contains items already available in NcrCache
     * then it will store the NodeRefs here and pick them up from cache
     * on the response created handler.
     *
     * @var array
     */
    private $pickup = [];

    /**
     * @param NcrCache $cache
     */
    public function __construct(NcrCache $cache)
    {
        $this->cache = $cache;
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

        $cachedNodeRefs = [];

        /** @var NodeRef $nodeRef */
        foreach ($request->get('node_refs') as $nodeRef) {
            if ($this->cache->hasNode($nodeRef)) {
                $cachedNodeRefs[] = $nodeRef;
            }
        }

        if (empty($cachedNodeRefs)) {
            return;
        }

        $request->removeFromSet('node_refs', $cachedNodeRefs);
        $this->pickup[(string)$request->get('request_id')] = $cachedNodeRefs;
    }

    /**
     * @param ResponseCreatedEvent $pbjxEvent
     */
    public function onGetNodeReponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if ($response->has('node')) {
            $this->cache->addNode($response->get('node'));
        }
    }

    /**
     * @param ResponseCreatedEvent $pbjxEvent
     */
    public function onGetNodeBatchReponseCreated(ResponseCreatedEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getResponse();
        if ($response->has('nodes')) {
            $this->cache->addNodes($response->get('nodes'));
        }

        $requestId = $response->get('ctx_request_ref')->getId();
        if (isset($this->pickup[$requestId])) {
            /** @var NodeRef $nodeRef */
            foreach ($this->pickup[$requestId] as $nodeRef) {
                $response->addToMap('nodes', $nodeRef->toString(), $this->cache->getNode($nodeRef));
            }

            unset($this->pickup[$requestId]);
        }
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:get-node-batch-request.before_handle' => 'onGetNodeBatchRequestBeforeHandle',
            'gdbots:ncr:mixin:get-node-response.created'            => 'onGetNodeReponseCreated',
            'gdbots:ncr:mixin:get-node-batch-response.created'      => 'onGetNodeBatchReponseCreated',
        ];
    }
}
