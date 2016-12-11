<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

/**
 * Basic implementation of NcrSearchAware interface.
 */
trait NcrSearchAwareTrait
{
    /** @var NcrSearch */
    protected $ncrSearch;

    /**
     * @param NcrSearch $ncrSearch
     */
    public function setNcrSearch(NcrSearch $ncrSearch)
    {
        $this->ncrSearch = $ncrSearch;
    }
}
