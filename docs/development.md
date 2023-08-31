# Development

This project uses [Hatch](https://hatch.pypa.io/latest/) for development management.

## Create hatch environments

```bash
hatch env create
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
