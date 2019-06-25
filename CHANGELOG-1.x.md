# CHANGELOG for 1.x
This changelog references the relevant changes done in 1.x versions.


## v1.0.3
* Ensure `NcrLazyLoader::flush` can bypass permission check.
 

## v1.0.2
* Remove pool delay from `DynamoDbNcr::doPipeNodes` altogether since concurrency and batching with symfony commands does the trick.


## v1.0.1
* Add `$context['concurrency']` check in `DynamoDbNcr::doPipeNodes` so that can be configured. Defaults to 25.


## v1.0.0
* Initial stable version.
