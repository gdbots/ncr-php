# CHANGELOG for 1.x
This changelog references the relevant changes done in 1.x versions.


## v1.0.7
* Cancel any publish jobs when a node is published in `AbstractNodeProjector`.


## v1.0.6
* Don't throw exceptions in `NcrRequestInterceptor` when nodes can't be fetched from `NcrCache`.
* Disable cache pruning in `NcrCache` when lazy loader is flushing.


## v1.0.5
* Don't throw exceptions in `MemoizingNcr` when nodes can't be fetched from `NcrCache` since they can be pruned which is normal.
* Increase default max items in `NcrCache` from 500 to 1000.
* In `AbstractPublishNodeHandler` create a new, current dated slug if local time is supplied and the item already has a dated slug.
* In `AbstractNodeProjector` when nodes are scheduled or published and the slug exists on the event, set that value on the node.


## v1.0.4
* Use a consistent read in `AbstractGetNodeHistoryRequestHandler`.


## v1.0.3
* Ensure `NcrLazyLoader::flush` can bypass permission check.
 

## v1.0.2
* Remove pool delay from `DynamoDbNcr::doPipeNodes` altogether since concurrency and batching with symfony commands does the trick.


## v1.0.1
* Add `$context['concurrency']` check in `DynamoDbNcr::doPipeNodes` so that can be configured. Defaults to 25.


## v1.0.0
* Initial stable version.
