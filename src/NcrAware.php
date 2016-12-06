<?php

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
