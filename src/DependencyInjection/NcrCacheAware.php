<?php
declare(strict_types=1);

namespace Gdbots\Ncr\DependencyInjection;

use Gdbots\Ncr\NcrCache;

/**
 * Marker Interface used for dependency injection
 */
interface NcrCacheAware
{
    /**
     * @param NcrCache $ncrCache
     */
    public function setNcrCache(NcrCache $ncrCache);
}
