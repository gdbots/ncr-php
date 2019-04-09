# CHANGELOG for 0.x
This changelog references the relevant changes done in 0.x versions.


## v0.3.16
* When a node is updated in `AbstractUpdateNodeHandler` and its new status is deleted, restore the default status.


## v0.3.15
* When `SearchNodesRequest` in `ElasticaNcrSearch` has `cursor` then always use page 1.


## v0.3.14
* Add `NcrCache::derefNodes` which can deference fields a node has which are refernces to other nodes.
* Modify `NcrLazyLoader::addEmbeddedNodeRefs` and `NcrPreloader::addEmbeddedNodeRefs` so they accept an associative array of paths or just a simple array.


## v0.3.13
* Add optional namespace argument to all `NcrPreloader` methods.  Now preloading can be isolated to namespaces which makes dealing with partial page caching and rendering of preloaded nodes less duplicative in the final output.


## v0.3.12
* Fix bug in `NcrRequestInterceptor` that was missing the last `%s` in a sprintf.  d'oh!


## v0.3.11
* Add caching to `Psr6Ncr` for when `NodeNotFound` exceptions occur. This ensures the underlying Ncr isn't hammered when nodes don't exist.
* Add caching for secondary lookups on `slug` to `NcrRequestInterceptor`.
* Add `bool $indexOnReplay = false` argument to `AbstractNodeProjector` to make it possible to enable indexing real-time in backup environments.


## v0.3.10
* Add `NcrPreloader::getNodeRefs` and `NcrPreloader::addEmbeddedNodeRefs` to simplify preloading many paths at once.


## v0.3.9
* Add `NcrPreloader` for preloading nodes. Typically used to enrich envelopes on HTTP endpoints or populated initial state in a client side javascript application.


## v0.3.8
* In `AbstractNodeProjector` run `$pbjx->send($command);` immediately if the `expires_at` is in the past.
* Add `NodeIdempotencyValidator` that ensures nodes are not duplicated even if events are delayed in processing. This is done using a `Psr\Cache\CacheItemPoolInterface` provider.


## v0.3.7
* Add `NodeMapper::getCustomNormalizers` and use the `MappingFactory::getCustomNormalizers` if available.


## v0.3.6
* Add `$config['aws_session_token'] = $this->credentials->getSecurityToken();` in `Gdbots\Ncr\Search\Elastica\AwsAuthV4ClientManager` so signatures work in AWS ECS.


## v0.3.5
* Fix bug in `AbstractLockNodeHandler` which attempts to set slug field which doesn't exist.


## v0.3.4
* Add qname filter on `pipeNodes` and `pipeNodeRefs` in `InMemoryNcr`.


## v0.3.3
* Change visibility on `ElasticaNcrSearch` private services to protected to allow extended classes to use them.


## v0.3.2
* Adjust inflection in `AbstractSearchNodesRequestHandler` for people.


## v0.3.1
* Adjust inflection in `AbstractSearchNodesRequestHandler` for _ies_ scenarios.


## v0.3.0
* Add conventional resolution for messages in abstract handlers (only override in concrete handler if needed).
* Add `UniqueNodeValidator` which ensures node ids and slugs are not duplicated.
* Ignore already deleted nodes in `AbstractNodeProjector`.
* Rename `PbjxHandlerTrait` to `PbjxHelperTrait` as it's now used in binders and validators.
* Modify `NodeCommandBinder` to use `PbjxHelperTrait` and change `getNodeForCommand` to `getNode(PbjxEvent $pbjxEvent): Node`.


## v0.2.4
* Add optional abstract services for common node handlers and projectors.


## v0.2.3
* Add `NodeEtagEnricher` that will automatically update the node's etag field during updates.
* Replace all `} catch (\Exception $e) {` with `} catch (\Throwable $e) {`.


## v0.2.2
* Add normalizer settings in `Gdbots\Ncr\Search\Elastica\IndexManager` and use that for the _raw_ fields in `Gdbots\Ncr\Search\Elastica\NodeMapper` so ElasticSearch sorting works.  The keyword type must be used for these unless you enable fielddata which is generally not recommended.
 

## v0.2.1
* Add handling for `statuses` field from `gdbots:ncr:mixin:search-nodes-request` in `Gdbots\Ncr\Search\Elastica\QueryFactory`.


## v0.2.0
__BREAKING CHANGES__

* Require `"gdbots/pbjx": "^2.1.1"`.
* Remove all `Gdbots\Ncr\DependencyInjection\*` classes since Symfony 4 autowiring wipes out the need for it.


## v0.1.2
* Update `gdbots/pbjx` composer constraint to require `^2.1`.
* Add Elasticsearch 5.x support in `Gdbots\Ncr\Search\Elastica\NodeMapper` and also use 
  multi-field index for `title.raw` on all nodes. 


## v0.1.1
* issue #4: BUG :: `NcrCache::pruneNodeCache` fails due to float vs int (php7 strict_types).


## v0.1.0
* Initial version.
