# Python API for Zurich Instruments LabOne software

Python API for LabOne.

> **Warning**
The name of the project and package are still work in progress.

## Contributing

See [Contributing](CONTRIBUTING.md)

# Demo Usage
```python
connection = await Session.create(kernel_info= ZIKernelInfo(), server_info =ServerInfo(host='localhost', port=8004))

await connection._session.listNodesJson("/zi/*").a_wait()
```