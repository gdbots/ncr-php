<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\EventStore\StreamSlice;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Schemas\Pbjx\Mixin\GetEventsRequest\GetEventsRequest;
use Gdbots\Schemas\Pbjx\Mixin\GetEventsResponse\GetEventsResponse;
use Gdbots\Schemas\Pbjx\StreamId;

abstract class AbstractGetNodeHistoryRequestHandler extends AbstractRequestHandler
{
    /**
     * @param GetEventsRequest $request
     * @param Pbjx             $pbjx
     *
     * @return GetEventsResponse
     */
    protected function handle(GetEventsRequest $request, Pbjx $pbjx): GetEventsResponse
    {
        /** @var StreamId $streamId */
        $streamId = $request->get('stream_id');
        $context = $this->createEventStoreContext($request, $streamId);

        // if someone is getting "creative" and trying to pull a different stream
        // then we'll just return an empty slice.  no soup for you.
        if ($this->canReadStream($request, $pbjx)) {
            $slice = $pbjx->getEventStore()->getStreamSlice(
                $streamId,
                $request->get('since'),
                $request->get('count'),
                $request->get('forward'),
                true,
                $context
            );
        } else {
            $slice = new StreamSlice([], $streamId, $request->get('forward'));
        }

        return $this->createGetEventsResponse($request, $pbjx)
            ->set('has_more', $slice->hasMore())
            ->set('last_occurred_at', $slice->getLastOccurredAt())
            ->addToList('events', $slice->toArray()['events']);
    }

    /**
     * @param GetEventsRequest $request
     * @param Pbjx             $pbjx
     *
     * @return bool
     */
    protected function canReadStream(GetEventsRequest $request, Pbjx $pbjx): bool
    {
        /** @var StreamId $streamId */
        $streamId = $request->get('stream_id');

        /*
         * a simplistic but mostly correct assertion that requests tend
         * to be named "acme:news:request:get-[node-label]-history-request".
         * If the incoming request is asking for that topic it is allowed.
         *
         * This only exists to prevent someone with permission to get
         * history on one thing but pass in a different stream id
         * (message permission vs message content permission).
         *
         * Override if more complex check is desired or if the convention
         * doesn't match.
         */
        $allowedTopic = str_replace(
            ['get-', '-request', '-history'],
            ['', '', '.history'],
            $request::schema()->getCurie()->getMessage()
        );

        return $allowedTopic === $streamId->getTopic();
    }

    /**
     * @param GetEventsRequest $request
     * @param Pbjx             $pbjx
     *
     * @return GetEventsResponse
     */
    protected function createGetEventsResponse(GetEventsRequest $request, Pbjx $pbjx): GetEventsResponse
    {
        /** @var GetEventsResponse $response */
        $response = $this->createResponseFromRequest($request, $pbjx);
        return $response;
    }
}
