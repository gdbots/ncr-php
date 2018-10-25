<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Validator;

use Gdbots\Common\Util\SlugUtils;
use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Pbj\Assertion;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\DependencyInjection\PbjxValidator;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Pbjx\EventSubscriber;
use Psr\Cache\CacheItemPoolInterface;

class NodeIdempotencyValidator implements EventSubscriber, PbjxValidator
{
    /** @var CacheItemPoolInterface */
    protected $cache;


    /**
     * @param CacheItemPoolInterface $cache
     */
    public function __construct(CacheItemPoolInterface $cache)
    {
      $this->cache = $cache;
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function validateCreateNode(PbjxEvent $pbjxEvent): void
    {
        $command = $pbjxEvent->getMessage();

        Assertion::true($command->has('node'), 'Field "node" is required.', 'node');

        /** @var CacheItemInterface[] $cacheItems */
        $cacheItems = [];

        /** @var Message $node */
        $node = $command->get('node');
        $qname = $node::schema()->getQName();

        $title = SlugUtils::create($node->get('title'));
        $keys = [
            // acme:article:some-title
            "{$qname}:{$title}" => true,
        ];
        $propertyNames = [
            // acme:article:some-title
            "{$qname}:{$title}" => 'title',
        ];

        if ($node->has('slug')) {
            $keys["{$qname}:{$node->get('slug')}"] = true;
            $propertyNames["{$qname}:{$node->get('slug')}"] = 'slug';
        }

        $cacheItems = $this->cache->getItems(array_keys($keys));
        if ($cacheItems instanceof \Traversable) {
            $cacheItems = iterator_to_array($cacheItems);
        }

        // now, check for these keys in cache
        foreach ($keys as $cacheKey => $value) {
            if (!isset($cacheItems[$cacheKey])) {
                continue;
            }

            $cacheItem = $cacheItems[$cacheKey];
            if (!$cacheItem->isHit()) {
                // save item on cache storage
                // $this->cache->save($cacheItem->set(true));
                continue;
            }

            $propertyName = $propertyNames[$cacheKey];
            throw new NodeAlreadyExists(
                sprintf(
                    'The [%s] with [%s] [%s] already exists so [%s] cannot continue.',
                    $node::schema()->getCurie()->getMessage(),
                    $propertyName,
                    $node->get($propertyName),
                    $command->generateMessageRef()
                )
            );
        }
    }




    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:create-node.validate' => 'validateCreateNode',
        ];
    }
}
