# CHANGELOG for 0.x
This changelog references the relevant changes done in 0.x versions.


## v0.3.3
* Add qname filter on `pipeNodes` and `pipeNodeRefs` in `inMemoryNcr`.


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
