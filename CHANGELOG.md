# Labone Python API Changelog

## Version 1.1.0 

* Introduce `labone.server` which allows spawning capnp servers based on the 
  reflection schema.
* Adapt `labone` to latest capnp schema improvements.
* Fix bug in `labone.nodetree` that caused the node tree never to be destroyed.
    The result was that when creating and deleting sessions frequently pycapnp
    would crash because to many sessions where active at the same time.

## Version 1.0.0

* Initial release of the `labone` API.
* Full LabOne Session functionality.
    * get Value(s)
    * set Values(s)
    * list nodes
    * subscribe
* Async node tree implementation.