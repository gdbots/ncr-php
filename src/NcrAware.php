<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

/**
 * Marker Interface used for dependency injection
 */
interface NcrAware
{
    /**
     * @param Ncr $ncr
     */
    public function setNcr(Ncr $ncr);
}
