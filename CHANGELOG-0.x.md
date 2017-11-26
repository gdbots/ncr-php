# CHANGELOG for 0.x
This changelog references the relevant changes done in 0.x versions.


## v0.1.2
* Update `gdbots/pbjx` composer constraint to require `^2.1`.
* Add Elasticsearch 5.x support in `Gdbots\Ncr\Search\Elastica\NodeMapper` and also use 
  multi-field index for `title.raw` on all nodes. 


## v0.1.1
* issue #4: BUG :: `NcrCache::pruneNodeCache` fails due to float vs int (php7 strict_types).


## v0.1.0
* Initial version.
