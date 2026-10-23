"""Exercise source predicates and index definitions without ML downloads."""
import ast
from pathlib import Path
import time
from aperturedb.Connector import Connector


def query(command):
    response, _ = db.query([command])
    assert db.last_query_ok(), response
    return response[0][next(iter(command))]


def deprecations():
    values = query({'GetMetrics': {'metrics': [
        {'family': 'deprecated_usage_total'}]}})['values']
    return {m['labels']['feature']: m['value']
            for family in values.get('families', []) or []
            for m in family.get('metrics', []) or []}


class BindValue(ast.NodeTransformer):
    def __init__(self, value):
        self.value = value

    def visit_Name(self, node):
        return ast.parse(repr(self.value), mode='eval').body

    visit_Attribute = visit_Name


for attempt in range(60):
    try:
        db = Connector(host='db', port=55555, user='admin', password='admin',
                       use_ssl=False, retry_max_attempts=1)
        query({'GetStatus': {}})
        break
    except Exception:
        time.sleep(1)
else:
    raise RuntimeError('Database did not become ready')

try:
    for value in ('$literal-id', 'other'):
        query({'AddEntity': {'class': 'PredicateCheck', 'properties': {
            'id': value, 'spec_id': value, 'run_id': value, 'url': value}}})
    entities = query({'FindEntity': {'with_class': 'PredicateCheck',
                      'results': {'list': ['id', '_uniqueid']}}})['entities']
    before = deprecations()
    checked = 0
    indexes_checked = 0
    for stage in ('crawl-website', 'text-extraction', 'text-embeddings', 'rag'):
        for path in (Path('/apps') / stage / 'app').glob('*.py'):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Dict):
                    continue
                for key, predicate in zip(node.keys, node.values):
                    if isinstance(key, ast.Constant) and key.value == 'CreateIndex':
                        # Use the actual source selectors, isolating each index's class.
                        definition = {}
                        for field, value in zip(predicate.keys, predicate.values):
                            name = ast.literal_eval(field)
                            definition[name] = (f'IndexCheck{indexes_checked}' if name == 'class'
                                                else ast.literal_eval(value))
                        query({'CreateIndex': definition})
                        assert definition['kind'] == 'ordered', (path, definition)
                        query({'RemoveIndex': definition})
                        indexes_checked += 1
                    if not isinstance(key, ast.Constant) or key.value != 'constraints':
                        continue
                    assert isinstance(predicate, ast.List), (path, predicate.lineno)
                    property_name = ast.literal_eval(predicate.elts[0])
                    operator = ast.literal_eval(predicate.elts[1])
                    for entity in entities:
                        value = entity['_uniqueid'] if property_name == '$_uniqueid' else entity['id']
                        value = [value] if operator == 'in' else value
                        # Bind only the runtime IDs; keep the source expression intact.
                        expr = BindValue(value).visit(ast.parse(ast.unparse(predicate), mode='eval'))
                        constraints = eval(compile(ast.fix_missing_locations(expr), str(path), 'eval'))
                        found = query({'FindEntity': {'with_class': 'PredicateCheck',
                            'constraints': constraints, 'results': {'list': ['id']}}})
                        assert found.get('entities') == [{'id': entity['id']}], (path, constraints, entity, found)
                    checked += 1
    assert checked > 0
    assert indexes_checked == 16, indexes_checked
    assert deprecations() == before, 'Queries emitted deprecated syntax metrics'
    print(f'PASS: {checked} source predicates and {indexes_checked} index definitions without deprecation increments')
finally:
    query({'DeleteEntity': {'with_class': 'PredicateCheck'}})
    db.close()
