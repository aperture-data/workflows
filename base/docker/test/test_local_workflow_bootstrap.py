"""Check shell bootstrap and cleanup examples without replacing full model tests."""
import ast
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import ModuleType

import pytest


ROOT = Path(os.getenv('WORKFLOWS_SOURCE', '/workflows'))


@pytest.mark.parametrize('app', ['ocr-extraction', 'embeddings-extraction'])
@pytest.mark.parametrize('ci_run', [0, 1])
def test_unit_precheck_has_a_base_on_fresh_local_and_preloaded_ci(tmp_path, app, ci_run):
    # Simulate the SDK registry and Compose. A unit launch still requires its base.
    app_dir = tmp_path / 'apps' / app
    app_dir.mkdir(parents=True)
    shutil.copyfile(ROOT / '.commonrc', tmp_path / '.commonrc')
    shutil.copyfile(ROOT / 'apps' / app / 'test.sh', app_dir / 'test.sh')
    ready = tmp_path / 'base-ready'
    events = tmp_path / 'events.jsonl'
    if ci_run:
        ready.touch()
    compose = tmp_path / 'compose.sh'
    compose.write_text('''#!/usr/bin/env python
import json, os, pathlib, sys
args = sys.argv[1:]
operation = next(value for value in args if value in {'build', 'run', 'up', 'logs', 'down'})
with open(os.environ['BOOTSTRAP_EVENTS'], 'a') as stream:
    stream.write(json.dumps({'operation': operation, 'args': args}) + '\\n')
ready = pathlib.Path(os.environ['BOOTSTRAP_BASE'])
if operation == 'build' and args[-1] == 'base':
    assert list(pathlib.Path(os.environ['SDK_WHEEL_DIR']).glob('*.whl'))
    ready.touch()
if operation == 'run' and 'unit-tests' in args and not ready.exists():
    print('The unit precheck has no locally built or preloaded base.', file=sys.stderr)
    sys.exit(73)
''')
    compose.chmod(0o755)
    tools = tmp_path / 'tools'
    tools.mkdir()
    oras = tools / 'oras'
    oras.write_text('#!/bin/sh\n'
                    'test "$1 $2 $3" = "pull registry-1.docker.io/aperturedata/aperturedb-python:latest --output" || exit 1\n'
                    'touch "$4/aperturedb-1.0.3-py3-none-any.whl"\n')
    oras.chmod(0o755)
    pause = tools / 'sleep'
    pause.write_text('#!/bin/sh\nexit 0\n')
    pause.chmod(0o755)
    environment = dict(os.environ, CI_RUN=str(ci_run), CLEANUP_ON_EXIT='1',
                       PATH=str(tools) + os.pathsep + os.environ['PATH'],
                       BOOTSTRAP_BASE=str(ready), BOOTSTRAP_EVENTS=str(events))
    result = subprocess.run(['bash', 'test.sh'], cwd=app_dir, env=environment,
                            text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    commands = [json.loads(line) for line in events.read_text().splitlines()]
    unit_index = next(index for index, command in enumerate(commands)
                      if command['operation'] == 'run' and 'unit-tests' in command['args'])
    preceding_builds = [command for command in commands[:unit_index]
                        if command['operation'] == 'build' and command['args'][-1] == 'base']
    assert bool(preceding_builds) == (ci_run == 0)
    assert any(command['operation'] == 'up' for command in commands)


@pytest.mark.parametrize('app,source,names', [
    ('face-detection', 'face_detection.py', ['PROCESSED_LABEL_IMAGES']),
    ('ocr-extraction', 'app.py', ['IMAGE_EXTRACTION_DESCRIPTOR_SET',
                                'PDF_EXTRACTION_DESCRIPTOR_SET']),
])
def test_cleanup_example_removes_exactly_the_app_owned_descriptor_classes(monkeypatch, app, source, names):
    tree = ast.parse((ROOT / 'apps' / app / 'app' / source).read_text())
    owned = {target.id: ast.literal_eval(node.value)
             for node in tree.body if isinstance(node, ast.Assign)
             for target in node.targets if isinstance(target, ast.Name) and target.id in names}
    assert set(owned) == set(names)
    readme = (ROOT / 'apps' / app / 'README.md').read_text()
    block = readme.split('```python\n', 1)[1].split('```', 1)[0]
    calls = []

    class RecordingUtils:
        def __init__(self, client):
            pass

        def remove_descriptorset(self, descriptor_class):
            calls.append(descriptor_class)
            return True

    module = ModuleType('aperturedb.Utils')
    module.Utils = RecordingUtils
    monkeypatch.setitem(sys.modules, 'aperturedb.Utils', module)
    exec(compile(block, app + '/README.md', 'exec'), {'client': object()})
    assert set(calls) == set(owned.values())
