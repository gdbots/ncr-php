<?php

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;

interface NcrAdmin
{
    /**
     * Creates the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $hints Data that helps the implementation decide where to create the storage.
     */
    public function createStorage(SchemaQName $qname, array $hints = []);

    /**
     * Returns debugging information about the storage for a given SchemaQName.
     *
     * @param SchemaQName $qname
     * @param array       $hints Data that helps the implementation decide where to create the storage.
     *
     * @return string
     */
    public function describeStorage(SchemaQName $qname, array $hints = []): string;
}
