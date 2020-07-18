<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeHistoryResponseV1;
use Gdbots\Schemas\Pbjx\StreamId;

class GetNodeHistoryRequestHandler implements RequestHandler
{
    public static function handlesCuries(): array
    {
        return [
            'gdbots:ncr:request:get-node-history-request',
        ];
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $request->get('node_ref');
        $response = GetNodeHistoryResponseV1::create();
        $context = ['causator' => $request];

        $streamId = StreamId::fromNodeRef($nodeRef);
        $slice = $pbjx->getEventStore()->getStreamSlice(
            $streamId,
            $request->get('since'),
            $request->get('count'),
            $request->get('forward'),
            true,
            $context
        );

        return $response->set('has_more', $slice->hasMore())
            ->set('last_occurred_at', $slice->getLastOccurredAt())
            ->addToList('events', $slice->toArray()['events']);
    }
}
