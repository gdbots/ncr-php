<?php
declare(strict_types=1);

namespace Gdbots\Ncr;

use Gdbots\Pbj\SchemaQName;
use Gdbots\Pbj\Util\StringUtil;

final class AggregateResolver
{
    /**
     * An array of all the available aggregates keyed by a qname.
     *
     * @var Aggregate[]
     */
    private static array $aggregates = [];

    /**
     * An array of class names keyed by a qname.
     * [
     *     'acme:article' => 'Acme\Ncr\ArticleAggregate'
     * ],
     *
     * @param Aggregate[] $aggregates
     */
    public static function register(array $aggregates): void
    {
        if (empty(self::$aggregates)) {
            self::$aggregates = $aggregates;
            return;
        }

        self::$aggregates = array_merge(self::$aggregates, $aggregates);
    }

    public static function resolve(SchemaQName $qname): Aggregate|string
    {
        $key = $qname->toString();
        if (isset(self::$aggregates[$key])) {
            return self::$aggregates[$key];
        }

        $vendor = StringUtil::toCamelFromSlug($qname->getVendor());
        $message = StringUtil::toCamelFromSlug($qname->getMessage());
        $class = "{$vendor}\\Ncr\\{$message}Aggregate";
        if (class_exists($class)) {
            return self::$aggregates[$key] = $class;
        }

        return self::$aggregates[$key] = Aggregate::class;
    }
}
