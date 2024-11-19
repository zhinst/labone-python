# Labone Python API Changelog

## Version 3.2.0
* `subscribe` accepts keyword arguments, which are forwarded to the data-server.
  This allows to configure the subscription to the data-server.
  Note that as of LabOne 24.10, no node supports yet subscription configuration.
* Fix error message in data server log if a subscription is cancelled gracefully.
* Adapt mock data server to hand unsubscribe events correctly.

## Version 3.1.2
* Fix bug which caused streaming errors to cancel the subscriptions
* Raise severity of errors during subscriptions to `FAILED` to cause a data server
  log entry.

## Version 3.1.1
* Add support for Python 3.13

## Version 3.1.0
* Expose timeout from underlying package when creating a new connection. 
  This allows specifying custom timeouts, e.g. when dealing with slow networks

## Version 3.0.0

* Enable server side parsing of the shf vectors. This is a breaking change since
the structure of the shf vector structs are now unified with the capnp schema.
* Replace the following `labone.core.Value` types with their capnp equivalent:
  `ShfDemodulatorVectorData`,`ShfResultLoggerVectorData`,`ShfScopeVectorData`,
  `ShfPidVectorData`,`CntSample`,`TriggerSample`
* Move the `extra_header` from the annotated value into the value field. This only affects 
shf vectors
* Adapt the `session.set` and `session.set_with_expression` functions to take either
an `AnnotatedValue` or `Value` and a path. This prevents unnecessary copies.
* Add support for `zhinst.comms` 3.0
* Update the `hpk_schema` to the latest version. This included stubs for all structs
defined in the capnp schema.

## Version 2.3.2
* Pump version of `zhinst.comms` to 2.1

## Version 2.3.1
* Add missing dependency on setuptools

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
