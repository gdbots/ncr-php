# CHANGELOG for 3.x
This changelog references the relevant changes done in 3.x versions.


## v3.1.0
* Require symfony 6.2.x
* Fix deprecation notice from elastica for addIndices.


## v3.0.2
* Only apply track_total_hits if it's true in QueryFactory, otherwise total is always 0.


## v3.0.1
* Fix bug with status enum not being castable to string in `QueryFactory::applyStatus`.


## v3.0.0
__BREAKING CHANGES__

* Requires `"gdbots/pbj": "^4.0"`
* Requires `"gdbots/pbjx": "^4.0"`
* Requires `"gdbots/schemas": "^3.0"`
