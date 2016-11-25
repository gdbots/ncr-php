<?php

namespace Gdbots\Ncr\Search;

interface CanCreateSearchStorage
{
    /**
     * Creates the storage for the search index.
     *
     * @param array $hints  Data that helps the implementation decide where to create the storage.
     */
    public function createSearchStorage(array $hints = []);

    /**
     * Returns debugging information about the storage for this search index.
     *
     * @param array $hints  Data that helps the implementation decide what storage to describe.
     *
     * @return string
     */
    public function describeSearchStorage(array $hints = []);
}
