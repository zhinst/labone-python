# Development

This project uses [Hatch](https://hatch.pypa.io/latest/) for development management.

## Installing requirements

```bash
pip install -r requirements.txt
```

## Running tests

### Package tests

```bash
hatch run test:pytest
hatch run test:unit
```

### Code style tests

```bash
hatch run style:lint
hatch run style:typing
```

### Building the package

```bash
hatch build
```
