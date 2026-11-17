"""The diagnostic artifact is limited to state and whitelisted exception names."""
import json
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(os.getenv('WORKFLOWS_SOURCE', '/workflows'))


@pytest.mark.parametrize('exit_code', [0, 1])
def test_phase_capture_keeps_health_timing_and_excludes_sensitive_payloads(tmp_path, exit_code):
    tools = tmp_path / 'tools'
    tools.mkdir()
    docker = tools / 'docker'
    docker.write_text('''#!/usr/bin/env python
import json, sys
if sys.argv[1] == 'ps':
    print('fixture-container')
else:
    print(json.dumps({'service':'embeddings-extraction','state':'exited',
        'exit_code':1,'oom_killed':False,'image_id':'sha256:fixture',
        'started_at':'2026-01-01T00:00:00Z','finished_at':'2026-01-01T00:03:00Z',
        'health':'unhealthy','health_checks':[{'started_at':'start','finished_at':'end',
        'exit_code':1,'Output':'DO_NOT_EXPORT_PAYLOAD'}],
        'Env':['PASSWORD=DO_NOT_EXPORT_PAYLOAD'],
        'Config':{'secret':'DO_NOT_EXPORT_PAYLOAD'}}))
''')
    docker.chmod(0o755)
    log = tmp_path / 'test.log'
    log.write_text('ConnectionError: DO_NOT_EXPORT_PAYLOAD\n'
                   'source/config/environment DO_NOT_EXPORT_PAYLOAD\n')
    output = tmp_path / 'test-state.json'
    environment = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'])
    subprocess.run(['bash', str(ROOT / 'tests/capture_test_state.sh'),
                    'fixture-project', 'integration', str(exit_code), str(log), str(output)],
                   env=environment, check=True, capture_output=True, text=True, timeout=10)
    text = output.read_text()
    result = json.loads(text)
    assert result['phase'] == 'integration'
    assert result['exit_code'] == exit_code
    assert result['exception_types'] == ['ConnectionError']
    assert result['containers'][0]['oom_killed'] is False
    assert result['containers'][0]['health_checks'] == [
        {'started_at': 'start', 'finished_at': 'end', 'exit_code': 1}]
    assert 'DO_NOT_EXPORT_PAYLOAD' not in text
    assert set(result['containers'][0]) == {
        'service', 'state', 'exit_code', 'oom_killed', 'image_id', 'started_at',
        'finished_at', 'health', 'health_checks'}
