<?php
declare(strict_types=1);

namespace Gdbots\Ncr\DependencyInjection;

use Gdbots\Ncr\NcrCache;

/**
 * Basic implementation of NcrCacheAware interface.
 */
trait NcrCacheAwareTrait
{
    /** @var NcrCache */
    protected $ncrCache;

    /**
     * @param NcrCache $ncrCache
     */
    public function setNcrCache(NcrCache $ncrCache)
    {
        $this->ncrCache = $ncrCache;
    }
}
