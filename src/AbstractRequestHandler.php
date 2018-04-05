<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\SchemaCurie;
use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\RequestHandlerTrait;
use Gdbots\Schemas\Pbjx\Mixin\Request\Request;
use Gdbots\Schemas\Pbjx\Mixin\Response\Response;

abstract class AbstractRequestHandler implements RequestHandler
{
    use RequestHandlerTrait;
    use PbjxHelperTrait;

    /**
     * Conventionally the response messages are named the same as
     * the request but with a "-response" suffix.
     *
     * @param Request $request
     *
     * @return Response
     */
    protected function createResponseFromRequest(Request $request): Response
    {
        $curie = str_replace('-request', '-response', $request::schema()->getCurie()->toString());
        /** @var Response $class */
        $class = MessageResolver::resolveCurie(SchemaCurie::fromString($curie));
        return $class::create();
    }
}
