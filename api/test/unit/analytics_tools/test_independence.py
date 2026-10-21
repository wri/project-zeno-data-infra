import ast
from pathlib import Path

import analytics_tools


def imported_modules(source_file):
    tree = ast.parse(source_file.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module


def test_package_does_not_depend_on_the_api_application():
    package_directory = Path(analytics_tools.__file__).parent

    for source_file in package_directory.rglob("*.py"):
        for module in imported_modules(source_file):
            assert not module.startswith("app"), f"{source_file} imports {module}"
