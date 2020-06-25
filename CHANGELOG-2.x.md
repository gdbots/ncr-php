# CHANGELOG for 2.x
This changelog references the relevant changes done in 2.x versions.


## v2.0.0
__BREAKING CHANGES__

* Uses `"gdbots/pbj": "^3.0"`
* Uses `"gdbots/pbjx": "^3.0"`
* Uses `"gdbots/schemas": "^2.0"`
* Adds `Aggregate` and `AggregateResolver` with all `gdbots:ncr:command:*` implementations and hooks to allow for customizations.
* Adds `NcrProjector` with concrete ncr and ncr search projections.
* Adds `ExpirableWatcher` and `PublishableWatcher` which handles the job scheduling. This was previously done in the `AbstractNodeProjector`.
* Removes `NodeEtagEnricher` since this is now done in the `Aggregate`.
