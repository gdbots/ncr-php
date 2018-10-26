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

        $keys = [
            // acme:article:some-title
            $this->getCacheKey($node, 'title') => 'title',
        ];

        if ($node->has('slug')) {
            $keys[$this->getCacheKey($node, 'slug')] = 'slug';
        }

        $cacheItems = $this->cache->getItems(array_keys($keys));
        if ($cacheItems instanceof \Traversable) {
            $cacheItems = iterator_to_array($cacheItems);
        }

        // now, check for these keys in cache
        foreach ($keys as $cacheKey => $value) {
            $cacheItem = $cacheItems[$cacheKey];
            if (!$cacheItem->isHit()) {
                continue;
            }

            throw new NodeAlreadyExists(
                sprintf(
                    'The [%s] with [%s] [%s] already exists so [%s] cannot continue.',
                    $node::schema()->getCurie()->getMessage(),
                    $value,
                    $node->get($value),
                    $command->generateMessageRef()
                )
            );
        }
    }

    /**
     * @param PbjxEvent $pbjxEvent
     */
    public function onCreateNodeAfterHandler(PbjxEvent $pbjxEvent): void
    {

    }

    /**
     * @param {Message} $node the node to get the key from
     * @param {String} $propertyName the name of the node's property to get the value from and will be used as part of the key
     * @param array   $context
     * @return string
     */
    protected function getCacheKey(Message $node, string $propertyName, array $context = []): string
    {
        $value = $node->get($propertyName);
        return str_replace('-', '_', sprintf(
            '%s.%s.php',
            str_replace(':', '_', $node::schema()->getQName()),
            (!SlugUtils::isValid($value) ? SlugUtils::create($value) : $value)
        ));
    }


    /**
     * @return array
     */
    public static function getSubscribedEvents()
    {
        return [
            'gdbots:ncr:mixin:create-node.validate' => 'validateCreateNode',
            'gdbots:ncr:mixin:create-node.after_handle' => 'onCreateNodeAfterHandler',
        ];
    }
}
