# CHANGELOG for 0.x
This changelog references the relevant changes done in 0.x versions.


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
