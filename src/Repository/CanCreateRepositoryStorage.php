<?php

namespace Gdbots\Ncr\Repository;

interface CanCreateRepositoryStorage
{
    /**
     * Creates the storage for the repository.
     *
     * @param array $hints  Data that helps the implementation decide where to create the storage.
     */
    public function createRepositoryStorage(array $hints = []);

    /**
     * Returns debugging information about the storage for this repository.
     *
     * @param array $hints  Data that helps the implementation decide what storage to describe.
     *
     * @return string
     */
    public function describeRepositoryStorage(array $hints = []);
}
