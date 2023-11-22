---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.15.2
  kernelspec:
    display_name: python
    language: python
    name: python3
---

# Quick Start Guide

This short script guides you trough the new python api.

Warning:
    Keep in mind that the API changes frequently and is not set into stone yet.

The first part focuses on the LabOne part. The second part on the reflection 
server.

## Connecting to a Device

To connect to a device simply import the `Instrument` class and create a new
instance (create is required because the constructor does not allow async code). 

```python
from labone import Instrument

device = await Instrument.create("dev8294", host="127.0.0.1", port=8004)
```

In addition to the Instrument we also expose a DataServer class. Manily to 
have access to the zi nodes.

```python
from labone import DataServer

dataserver = await DataServer.create(host="127.0.0.1", port=8004)
```

## Unerlying Kernel Session 

The device and dataserver objects are based on the same underlying kernel session
class. It can be accessed by the `kernel_session` attribute

```python
device.kernel_session
```

## Object Based Nodetree
The device/dataserver object automatically exposes the nodetree of the device/kernel.
Nodes can be accessed like attributes. The async nodetree behaves exactly the same 
than the one from zhinst-toolkit, with the following exceptions: 

* The nodetree automatically checks if a node excists or not when accessing it
* getting wildcards or partial nodes return a result object instead of a dict

```python
dataserver.debug.level
```

To get or set a node simply use the call operator.
There is only one way of setting a node and both set and get return the acknowledged
value and timestamp from the device.

```python
print(await dataserver.debug.level(3))
print(await dataserver.debug.level())
```

Getting a partial node returns a result object. 
The result object can be used similar to the nodetree. When a leaf node is accessed 
it returns its value.


```python
result = await dataserver()
print(result.debug.level)
```

```python
for value in result.results():
    print(value)
```

## Subscriptions

The most fundamental different thing compared to zhinst.core is the subscription 
handling. Instead of a session based poll function a subscriptions returns a 
python fifo async queue to which the kernel automatically pushes all update events
that happen to the subscribed node. 

Note:
    Creating a subscription is very cheap and does not have a lot overhead. So
    creating and destroying subscriptions is nothing bad. However subscribing to
    nodes with frequent updates causes some not negletible network overhead. To
    avoid subscribing to the same node multiple times one should rather use the 
    `fork` feature described below.

```python
queue = await dataserver.debug.level.subscribe()
print(queue.qsize())
await dataserver.debug.level(1)
await dataserver.debug.level(2)
await dataserver.debug.level(3)
print(queue.qsize())
```

The queue derives from the building AsyncQueue and can therefore be used is the same way.

```python
while queue.qsize() > 0:
    print(queue.get_nowait())
```

Since every subscription will cause the server to send the update requests, n subscriptions
produce n messages for a single update. This can easily be avoided by the `fork` mechanism.

An existing, active, queue can be forked. The forked queue is independet of the
original queue and will receive updates through the same subscription, even if
the original queue is deleted.

Note that the forked queue will not contain updates previous to the fork event.

```python
print(queue.qsize())
await dataserver.debug.level(1)
queue2 = queue.fork()
await dataserver.debug.level(2)
await dataserver.debug.level(3)
print(queue.qsize())
print(queue2.qsize())
```

If a subscription is no longer neede one can simply delete the queue. Alternatively
a queue can be disconnected.

```python
queue.disconnect()
del queue2
```

## Reflection Server

The python client is based on the reflection schema defined in LabOne. 
When connecting to a server the first thing is to ask the server for all its
capabilities. Instead of using the above instrument class or the node tree one
can also access the capabilities on its own

```python
from labone.core.reflection import ReflectionServer

server = await ReflectionServer.create(host="127.0.0.1", port=9999)
```
