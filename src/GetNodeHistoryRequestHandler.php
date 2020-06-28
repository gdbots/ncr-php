<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\Message;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Schemas\Ncr\Request\GetNodeHistoryRequestV1;
use Gdbots\Schemas\Ncr\Request\GetNodeHistoryResponseV1;
use Gdbots\Schemas\Pbjx\StreamId;

class GetNodeHistoryRequestHandler implements RequestHandler
{
    public static function handlesCuries(): array
    {
        return [
            GetNodeHistoryRequestV1::SCHEMA_CURIE,
        ];
    }

    public function handleRequest(Message $request, Pbjx $pbjx): Message
    {
        /** @var NodeRef $nodeRef */
        $nodeRef = $request->get(GetNodeHistoryRequestV1::NODE_REF_FIELD);
        $response = GetNodeHistoryResponseV1::create();
        $context = ['causator' => $request];

        $streamId = StreamId::fromNodeRef($nodeRef);
        $slice = $pbjx->getEventStore()->getStreamSlice(
            $streamId,
            $request->get(GetNodeHistoryRequestV1::SINCE_FIELD),
            $request->get(GetNodeHistoryRequestV1::COUNT_FIELD),
            $request->get(GetNodeHistoryRequestV1::FORWARD_FIELD),
            true,
            $context
        );

        return $response->set($response::HAS_MORE_FIELD, $slice->hasMore())
            ->set($response::LAST_OCCURRED_AT_FIELD, $slice->getLastOccurredAt())
            ->addToList($response::EVENTS_FIELD, $slice->toArray()['events']);
    }
}
