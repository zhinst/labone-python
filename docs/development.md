# Development

This project uses [Hatch](https://hatch.pypa.io/latest/) for development management.

## Install initial requirements

```bash
pip install -r requirements.txt
```

## Running tests

### Package tests

```bash
hatch run test
```

### Code style tests

```bash
hatch run lint:style
hatch run lint:typing
```

### Building the package

```bash
hatch build
```
