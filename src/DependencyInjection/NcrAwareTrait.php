<?php
declare(strict_types=1);

namespace Gdbots\Ncr\DependencyInjection;

use Gdbots\Ncr\Ncr;

/**
 * Basic implementation of NcrAware interface.
 */
trait NcrAwareTrait
{
    /** @var Ncr */
    protected $ncr;

    /**
     * @param Ncr $ncr
     */
    public function setNcr(Ncr $ncr)
    {
        $this->ncr = $ncr;
    }
}
