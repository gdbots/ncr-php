<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Gdbots\Pbj\Field;
use Gdbots\Pbj\Marshaler\Elastica\MappingBuilder as BaseMappingBuilder;
use Gdbots\Pbj\Schema;

class MappingBuilder extends BaseMappingBuilder
{
    protected function filterProperties(Schema $schema, Field $field, string $path, array $properties): array
    {
        if ($path === 'title') {
            $properties['fields'] = [
                'completion' => ['type' => 'completion', 'analyzer' => 'pbj_keyword'],
                'raw'        => ['type' => 'keyword', 'normalizer' => 'pbj_keyword'],
                'standard'   => ['type' => 'text', 'analyzer' => 'standard'],
            ];
        }

        return $properties;
    }
}
