<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbjx\RequestHandler;
use Gdbots\Pbjx\RequestHandlerTrait;

abstract class AbstractRequestHandler implements RequestHandler
{
    use RequestHandlerTrait;
    use PbjxHelperTrait;
}
