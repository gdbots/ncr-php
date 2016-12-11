<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

/**
 * Marker Interface used for dependency injection
 */
interface NcrSearchAware
{
    /**
     * @param NcrSearch $ncrSearch
     */
    public function setNcrSearch(NcrSearch $ncrSearch);
}
