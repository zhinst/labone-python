# Labone Python API Changelog

## Version 2.3.0
* Pump version of `zhinst.comms` to 2.0.0
* Deprecate `labone.server` since the logic has been moved to `zhinst.comms.server`

## Version 2.2.0
* Adapt the server module to support closing a running server and run a server
  forever.

## Version 2.1.0
* Pump version of `zhinst.comms` to 1.1.0

## Version 2.0.0 

* Switch to the custom capnp backend `zhinst.comms`. This fixes the stability issues.

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