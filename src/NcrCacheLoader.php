<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;

/*
 * todo: handle modification of GetNodeRequest and GetNodeBatchRequest
 * to remove node_refs (when consistent=false) that can be filled after
 * the response is created with NcrCache or potentially eliminate the
 * request altogether and return early if all nodes are in cache.
 * ref Gdbots\Pbjx\Event\GetResponseEvent.
 *
 */
class NcrCacheLoader implements EventSubscriber
{
    /** @var NcrCache */
    private $cache;

    /**
     * @param NcrCache $cache
     */
    public function __construct(NcrCache $cache)
    {
        $this->cache = $cache;
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function onGetNodeReponseCreated(PbjxEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getMessage();
        if ($response->has('node')) {
            $this->cache->putNode($response->get('node'));
        }
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function onGetNodeBatchReponseCreated(PbjxEvent $pbjxEvent): void
    {
        $response = $pbjxEvent->getMessage();
        if ($response->has('nodes')) {
            $this->cache->putNodes($response->get('nodes'));
        }
    }

    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:get-node-response.created'       => 'onGetNodeReponseCreated',
            'gdbots:ncr:mixin:get-node-batch-response.created' => 'onGetNodeBatchReponseCreated',
        ];
    }
}
