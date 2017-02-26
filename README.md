ncr-php
=============

[![Build Status](https://api.travis-ci.org/gdbots/ncr-php.svg)](https://travis-ci.org/gdbots/ncr-php)
[![Code Climate](https://codeclimate.com/github/gdbots/ncr-php/badges/gpa.svg)](https://codeclimate.com/github/gdbots/ncr-php)
[![Test Coverage](https://codeclimate.com/github/gdbots/ncr-php/badges/coverage.svg)](https://codeclimate.com/github/gdbots/ncr-php/coverage)

Node Content Repository for php.  Using this library assumes that you've already created and compiled your own pbj classes using the [Pbjc](https://github.com/gdbots/pbjc-php) and are making use of the __"gdbots:ncr:mixin:*"__ mixins from [gdbots/schemas](https://github.com/gdbots/schemas).

> If your project is using Symfony3 use the [gdbots/ncr-bundle-php](https://github.com/gdbots/ncr-bundle-php) to simplify the integration.


# Nodes and Edges
A [node or vertex](https://en.wikipedia.org/wiki/Graph_theory) is a _noun/entity_ in your system.  An article, tweet, video, person, place, order, product, etc.  The edges are the _relationships_ between those things like "friends", "tagged to", "married to", "published by", etc.

This library doesn't provide you with a graph database implementation.  It's concerned with persisting/retrieving nodes and edges.  Graph traversal would still need to be provided by another library.  It is recommended that data be replicated or projected out of the Ncr (or layered on top like [GraphQL](http://graphql.org/)) into something suited for that purpose (e.g. Neo4j, Titan, ElasticSearch).


# NodeRef
A NodeRef is a qualified identifier to a node/vertex.  It is less verbose than a `MessageRef` as it is implied that node labels must be unique within a given vendor namespace and therefore can be represented in a more compact manner.

> __NodeRef Format:__ vendor:label:id  
> The __"vendor:label"__ portion is a `SchemaQName`

__Examples:__

> acme:article:41e4532f-2f58-4b9d-afc8-e9c2cbcb4aba  
> twitter:tweet:789234931599835136  
> youtube:video:EG0wQRsXLi4

Nodes do not actually have a `node_ref` field, they have an `_id` field.  The `NodeRef` is derived by taking the `SchemaQName` of the node's schema along with its `_id`.  The `NodeRef` is an immutable value object which is used in various places without needing to actually have the node instance.


# Ncr
The Ncr is the service responsible for node persistence.  It is intentionally limited to basic key/value storage operations (e.g. put/get/find by index) to ensure the underlying implementation can be swapped out with little effort or decorated easily (caching layers for example).

__Available repository implementations:__

+ DynamoDb
+ Psr6 _(gives you Redis, File, Memcached, Doctrine, etc.)_
+ In Memory

Review the `Gdbots\Ncr\Ncr` interface for reference on the available methods.

## Ncr::findNodeRefs
The Ncr is a simple key/value store which means querying is limited to the id of the item or a secondary index.  An example of a secondary index would be the email or username of a user, the slug of an article or the isbn of a book.

An `IndexQuery` is used to `findNodeRefs` that match a query against a secondary index.

__An example of using a IndexQuery:__

```php
$query = IndexQueryBuilder::create(SchemaQName::fromString('acme:user'), 'email', 'homer@simpson.com')
    ->setCount(1)
    ->build();
$result = $this->ncr->findNodeRefs($query);
if (!$result->count()) {
    throw new NodeNotFound('Unable to find homer.');
}

$node = $this->ncr->getNode($result->getNodeRefs()[0]);
```
Not all storage engines can enforce uniqueness on a secondary index so the interface also cannot make that assumption.  Because of this the `findNodeRefs` may return more than one value.  It is up to your application logic to deal with that.


## Ncr::pipeNodes
Getting data out of the Ncr should be dead simple, it's just json after all.  Use the `pipeNodes` or `pipeNodeRefs` methods to export data.  The __gdbots/ncr-bundle-php__ provides console commands that make use of this to export and reindex nodes.

__Exporting nodes using pipeNodes:__

```php
$receiver = function (Node $node) {
    echo json_encode($node) . PHP_EOL;
};

$ncr->pipeNodes(SchemaQName::fromString('acme:article'), $receiver);
```


# NcrCache
NcrCache is a first level cache which is ONLY seen and used by the current request.  It is used to cache all nodes returned from get node request(s).  This cache is used during [Pbjx](https://github.com/gdbots/pbjx-php) request processing or if the Ncr is running in the current process and is using the MemoizingNcr.

> This cache should not be used when asking for a consistent result.

__NcrCache is NOT an identity map__ and the __Ncr is NOT an ORM__. In some cases you may get the same exact object but it's not a guarantee so don't do something like this:

```php
$nodeRef = NodeRef::fromString('acme:article:123');
$cache->getNode($nodeRef) !== $cache->getNode($nodeRef);
```

If you need to check equality, use the message interface:

```php
$node1 = $cache->getNode($nodeRef);
$node2 = $cache->getNode($nodeRef);
$node->equals($node2); // returns true if their data is the same
```


# NcrLazyLoader
NcrCache and other request interceptors make use of this service to batch load nodes only if they are requested.  An example of this is when loading an article you may want to fetch the author or related items, but not always.  Rather than force the logic to exist in the loading of an article, something else can manage that.

> Lazy loading is generally application specific so this library provides some tools to make is easier.

__Example lazy loading:__

```php
/**
 * @param ResponseCreatedEvent $pbjxEvent
 */
public function onSearchNodesResponse(ResponseCreatedEvent $pbjxEvent): void
{
    $response = $pbjxEvent->getResponse();
    if (!$response->has('nodes')) {
        return;
    }

    // for all nodes in this search response, mark the creator
    // and updater for lazy load.  if they get requested at some point
    // in the current request, it will be batched for optimal performance
    $this->lazyLoader->addEmbeddedNodeRefs($response->get('nodes'), [
        'creator_ref' => 'acme:user',
        'updater_ref' => 'acme:user',
    ]);
}
```


# NcrSearch
The Ncr provides the reliable storage and retrieval of Nodes.  NcrSearch is in most cases a separate storage provider.  For example, DynamoDb for Ncr and ElasticSearch for NcrSearch.  In fact, the only implementation we have right now is ElasticSearch.

To use this feature, the nodes you want to index must be using the __"gdbots:ncr:mixin:indexed"__ mixin.  When using the __gdbots/ncr-bundle-php__ you can enable the indexing with a simple configuration option.  The bundle also provides a reindex console command.

Searching nodes is generally done in a request handler.  Here is an example of searching nodes:

```php
/**
 * @param SearchUsersRequest $request
 * @param Pbjx               $pbjx
 *
 * @return SearchUsersResponse
 */
protected function handle(SearchUsersRequest $request, Pbjx $pbjx): SearchUsersResponse
{
    $parsedQuery = ParsedQuery::fromArray(json_decode($request->get('parsed_query_json', '{}'), true));

    $response = SearchUsersResponseV1::create();
    $this->ncrSearch->searchNodes(
        $request,
        $parsedQuery,
        $response,
        [SchemaQName::fromString('acme:user')]
    );

    return $response;
}
```
