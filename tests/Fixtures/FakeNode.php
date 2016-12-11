<?php

namespace Gdbots\Tests\Ncr\Fixtures;

use Gdbots\Pbj\AbstractMessage;
use Gdbots\Pbj\Enum\Format;
use Gdbots\Pbj\FieldBuilder as Fb;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\Schema;
use Gdbots\Pbj\Type as T;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Trait;

final class FakeNode extends AbstractMessage implements
    NodeV1
{
    use NodeV1Trait;

    /**
     * @return Schema
     */
    protected static function defineSchema()
    {
        $schema = new Schema('pbj:gdbots:tests.ncr:fixtures:fake-node:1-0-0', __CLASS__,
            [],
            [
                NodeV1Mixin::create(),
            ]
        );

        MessageResolver::registerSchema($schema);
        return $schema;
    }

    /**
     * @return array
     */
    public function getUriTemplateVars()
    {
        return ['fake_id' => (string)$this->get('_id')];
    }
}
