# CHANGELOG for 2.x
This changelog references the relevant changes done in 2.x versions.


## v2.1.5
* Do not set event slug unless schema has that field in `Aggregate::[un]lockNode`.


## v2.1.4
* Ensure only unique index names are used in `ElasticaNcrSearch::searchNodes`.


## v2.1.3
* When requesting a consistent read with `gdbots:ncr:request:get-node-request` and the node isn't in the Ncr yet, then use the aggregate.


## v2.1.2
* Fix bug in `Aggregate` where `updateNodeTags` fails if `add_tags` is missing on the command.


## v2.1.1
* Add implementations for `gdbots:ncr:command:update-node-labels` and `gdbots:ncr:command:update-node-tags`.
* Fix bug in `GetNodeRequestHandler` where `$nodeRef` wasn't set when doing a slug lookup.
* Remove use of mixin/message constants for fields and schema refs as it's too noisy and isn't enough of a help to warrant it.


## v2.1.0
* Uses `"gdbots/pbj": "^3.1"` with context arg for `Pbjx::sendAt` and `Pbjx::cancelJobs`.


## v2.0.4
* Fix issue, yes again, with Elastica/Guzzle throwing exception on `Index::exists` instead of returning a boolean.


## v2.0.3
* Add `Aggregate::copyContext` to reduce duplicated code for common context fields.


## v2.0.2
* Use `Message::SCHEMA_CURIE_MAJOR` when resolving with `MessageResolver::findAllUsingMixin`.


## v2.0.1
* Fix issue with Elastica/Guzzle throwing exception on `Index::exists` instead of returning a boolean.


## v2.0.0
__BREAKING CHANGES__

* Uses `"gdbots/pbj": "^3.0"`
* Uses `"gdbots/pbjx": "^3.0"`
* Uses `"gdbots/schemas": "^2.0"`
* Adds `Aggregate` and `AggregateResolver` with all `gdbots:ncr:command:*` implementations and hooks to allow for customizations.
* Removes all abstract node command handlers as they are not implemented in the aggregate.
* Adds `NcrProjector` with concrete ncr and ncr search projections.
* Adds `ExpirableWatcher` and `PublishableWatcher` which handles the job scheduling. This was previously done in the `AbstractNodeProjector`.
* Removes `NodeEtagEnricher` since this is now done in the `Aggregate`.
* Removes `BindFromNodeEvent` and `BeforePutNodeEvent` as these features should now be handled in the `Aggregate`.
* Adds `NodeProjectedEvent` which is dispatched after the `NcrProjector` puts the new node into the Ncr and NcrSearch.
