"""Workflow sources must parse with the base image's Python interpreter."""
import ast
import os
from pathlib import Path


def test_python_sources_parse_with_base_interpreter():
    root = Path(os.getenv('WORKFLOWS_SOURCE', '/workflows'))
    sources = sorted(root.rglob('*.py'))
    assert sources, f'No Python sources mounted at {root}'
    for path in sources:
        ast.parse(path.read_text(), filename=str(path))
