"""Custom build script for hatch backend."""
import ast
import enum
import re
import shutil
import subprocess
from pathlib import Path

import black
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

SOURCE_DIR = Path(__file__).parent / "src/labone"

IGNORED = [
    SOURCE_DIR / "resources",
    SOURCE_DIR / "sync_helper.py",
    SOURCE_DIR / "__init__.py",
    SOURCE_DIR / "_version.py",
    SOURCE_DIR / "sync",
    SOURCE_DIR / "semi_sync",
]

IGNORED_RUFF_RULES = [
    "ARG002",
    "E501",
    "ARG003",
    "ARG004",
    "E731",
    "PLR0913",
    "A003",
    "ARG001",
    "N802",
    "BLE001",
    "TRY200",
    "SLF001",
    "B904",
    "PLR2004",
    "ANN401",
    "D102",
]
RUFF_IGNORE_COMMENT = "# ruff: noqa: " + ", ".join(IGNORED_RUFF_RULES) + "\n"


TIMEOUT_DEFAULT = 10.0
TIMEOUT_DOCSTRING = f"Timeout in seconds (default = {TIMEOUT_DEFAULT})"

IGNORED_ASYNC_FUNCTIONS = {
    SOURCE_DIR
    / "core/subscription.py": [
        "sendValues",  # This functions is used in the
    ],
    SOURCE_DIR
    / "nodetree/helper.py": [
        "list_nodes",
        "list_nodes_info",
        "set",
        "set_with_expression",
        "get",
        "get_with_expression",
        "subscribe",
    ],
}


class SyncMode(enum.Enum):
    """Modes for the sync backend."""

    SYNC = "sync"
    SEMI_SYNC = "semi_sync"


class AsyncTransformer(ast.NodeTransformer):
    """AST transformer to unsync all async functions.

    This transformer adds the right unsync decorator to all async functions.
    It also adds the import statement that imports the right decorator in
    the cleanup function.

    Args:
        file: The file that is being transformed.
        tree: The AST tree of the file.
    """

    def __init__(self, file: Path, tree: ast.Module, mode: SyncMode):
        self._file = file.resolve()
        self._tree = tree
        self._mode = mode
        self._contains_async = False
        self._ignore_function = IGNORED_ASYNC_FUNCTIONS.get(self._file, [])

    def _adapt_docstring(self, node: ast.AsyncFunctionDef) -> None:
        old_docstring = ast.get_docstring(node)
        if old_docstring is None:
            return
        # Find end of Args section
        # This is a bit tricky since there might not be an Args section
        # We therefor split the adaption into two parts, depending on
        # weather the Args section is present or not.
        if "Args" in old_docstring:
            new_docstring = re.sub(
                r"(( *?)Args:\n((\2    .*\n)|\n)*)(\n?\2[A-Z]?)",
                rf"\1\2    timeout: {TIMEOUT_DOCSTRING}\n\5",
                old_docstring,
            )
        else:
            new_docstring = re.sub(
                r"((\s*))$",
                rf"\n\2Args:\n\2    timeout: {TIMEOUT_DOCSTRING}\n\1",
                old_docstring,
            )
        # Unfortunately there is no easy way to do this so we hack it a bit.
        node.body[0].value = ast.Constant(new_docstring)

    def visit_AsyncFunctionDef(  # noqa: N802
        self,
        node: ast.AsyncFunctionDef,
    ) -> ast.AsyncFunctionDef:
        """AST build in method called for all async functions during visit.

        Args:
            node: ast node of the async function

        Returns:
            Modified node of the async function
        """
        if node.name in self._ignore_function or (
            node.name.startswith("_") and not node.name.startswith("__")
        ):
            return node
        self._contains_async = True
        # Add a new keyword argument 'timeout' to the function
        timeout_arg = ast.arg(
            arg="timeout",
            annotation=ast.Name(id="int", ctx=ast.Load()),
        )
        node.args.kwonlyargs.append(timeout_arg)
        timeout_default = ast.keyword(
            arg=timeout_arg.arg,
            value=ast.Num(n=TIMEOUT_DEFAULT),
        )
        node.args.kw_defaults.append(timeout_default.value)
        # Add unsync decorator
        node.decorator_list.append(
            ast.Name(id=f"make_{self._mode.value}", ctx=ast.Load()),
        )
        # Add timeout to docstring
        self._adapt_docstring(node)
        return node

    def _add_import_statement(self) -> None:
        """Add the unsync import statement to the ast of the module."""
        import_statement = ast.ImportFrom(
            module="labone.sync_helper",
            names=[ast.alias(name=f"make_{self._mode.value}", asname=None)],
            level=0,
        )
        # It would be nice if we can add the import at the correct place
        # (together with the other package internal includes)
        # Therefore we first check if there are any imports from labone
        # If there are we add the import statement after teh last import
        for i, node in enumerate(self._tree.body):
            if isinstance(node, ast.ImportFrom) and "labone" in node.module:
                self._tree.body.insert(i, import_statement)
                return
        for i, node in enumerate(self._tree.body):
            if not isinstance(node, (ast.ImportFrom, ast.Import)) and not (
                isinstance(node, ast.Expr) and isinstance(node.value, ast.Str)
            ):
                self._tree.body.insert(i, import_statement)
                return

    def post_processing(self) -> None:
        """Do some post processing after visiting all nodes."""
        if self._contains_async:
            self._add_import_statement()


class ImportTransformer(ast.NodeTransformer):
    """AST transformer to change all imports.

    When copying the files to the sync backend we need to change all imports.
    This transformer does that.

    Args:
        mode: The mode of the sync backend.
    """

    def __init__(self, mode: SyncMode):
        self._mode = mode
        root_dir = Path(__file__).parent / "src/"
        self._ignored_imports = [
            str(path.relative_to(root_dir)).replace("/", ".")
            for path in IGNORED
            if path.is_dir()
        ]
        self._import_re = re.compile(rf"labone\.(?!{mode.value}|resources|sync_helper)")

    def _redirect_import(self, module: str) -> str:
        """Redirect the import to the right backend.

        Args:
            module: The module to redirect.

        Returns:
            The redirected module.
        """
        return self._import_re.sub(rf"labone.{self._mode.value}.", module)

    def visit_Import(self, node: ast.Import) -> ast.Import:  # noqa: N802
        """AST build in method called for all imports during visit.

        Args:
            node: ast node of the import

        Returns:
            Modified node of the import
        """
        node.names[0].name = self._redirect_import(node.names[0].name)
        return node

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.ImportFrom:  # noqa: N802
        """AST build in method called for all imports during visit.

        Args:
            node: ast node of the import

        Returns:
            Modified node of the import
        """
        node.module = self._redirect_import(node.module)
        return node


class UnsyncGenerator:
    """Generator for the unsync backend.

    This class is responsible for generating the unsync backend.
    Depending on the mode it will generate the right backend by duplicating
    all files into a subdirectory, adding the right unsync decorator and
    adapting all the import statements.
    This hard copy approach guarantees optimal performance and also does work
    with static tye checkers.

    Args:
        mode: The mode of the sync backend.
        src: The source directory of the labone package.
    """

    def __init__(self, mode: SyncMode, src: Path = SOURCE_DIR):
        self._mode = mode
        self._src = src
        self._dest = self._src / mode.value

    @staticmethod
    def _is_ignored_module(file_path: Path) -> bool:
        """Check if the module should be ignored.

        Args:
            file_path: The path to the module.

        Returns:
            True if the module should be ignored.
        """
        file_path = file_path.resolve()
        for ignored in IGNORED:
            if str(file_path).startswith(str(ignored.resolve())):
                return True
        return False

    def _unsync_module(self, module: Path) -> None:
        """Unsync a single module.

        Read the original module, unsync it and write it to the new location.

        Args:
            module: The module to unsync.
        """
        # read original file
        with module.open("r") as f:
            source = f.read()

        tree = ast.parse(source)

        # Add unsync decorator
        async_transformer = AsyncTransformer(file=module, tree=tree, mode=self._mode)
        async_transformer.visit(tree)
        async_transformer.post_processing()

        # Cleanup imports
        ImportTransformer(self._mode).visit(tree)

        source = ast.unparse(tree)

        # Add ruff ignore (ast discards the comments)
        source = RUFF_IGNORE_COMMENT + source

        # Format with black to make it look nice
        source = black.format_str(source, mode=black.Mode())

        # Write to new location
        target_module = self._dest / module.relative_to(SOURCE_DIR)
        target_module.parent.mkdir(parents=True, exist_ok=True)
        with target_module.open("w") as f:
            f.write(source)

    def _post_process(self) -> None:
        """Do some post processing after copying all files.

        Currently this is just running ruff
        """
        with (self._dest / "__init__.py").open("w") as f:
            f.write('"""Sync backend for the Zurich Instruments LabOne software."""')
        subprocess.run(
            [  # noqa: S603, S607
                "ruff",
                "check",
                self._dest,
                "--fix",
                "--no-respect-gitignore",
            ],
            stdout=subprocess.DEVNULL,
            check=True,
        )

    def generate(self) -> None:
        """Generate the unsync backend."""
        if self._dest.exists():
            shutil.rmtree(self._dest)

        [
            self._unsync_module(module)
            for module in SOURCE_DIR.glob("**/*.py")
            if not self._is_ignored_module(module)
        ]

        self._post_process()


class CustomHook(BuildHookInterface):
    """A custom build hook for nbconvert."""

    def initialize(self, **_) -> None:
        """Initialize the hook."""
        if self.target_name not in ["wheel", "sdist"]:
            return
        UnsyncGenerator(SyncMode.SYNC).generate()
        # UnsyncGenerator(SyncMode.SEMI_SYNC).generate()


if __name__ == "__main__":
    UnsyncGenerator(SyncMode.SYNC).generate()
    # UnsyncGenerator(SyncMode.SEMI_SYNC).generate()
