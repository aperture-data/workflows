"""Wire-format regression tests; run in the SQL workflow test container."""
import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest


def load_fdw_module(name):
    # The SQL test image does not load Multicorn inside PostgreSQL. Import just
    # these dependency-free helpers from the FDW source mounted by the runner.
    root = Path(os.environ.get("FDW_SOURCE", "/fdw/fdw"))
    spec = importlib.util.spec_from_file_location(name, root / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def expressions():
    return load_fdw_module("expressions")


def test_wire_conversion_preserves_planner_and_property_data(expressions):
    import copy
    payload = {"constraints": {"name": ["==", "$literal"]}}
    query = [{"FindEntity": {
        "constraints": {"age": [">=", 20, "<", 90]},
        "properties": {"payload": {"_json": payload}},
        "is_connected_to": {"any": [
            {"ref": 1, "constraints": {"tag": ["in", ["$a", "b"]]}},
            {"ref": 2, "constraints": {}},
        ]},
    }}]
    original = copy.deepcopy(query)
    result = expressions.query_expressions(query)[0]["FindEntity"]
    assert query == original
    assert result["constraints"] == ["all", ["$age", ">=", 20], ["$age", "<", 90]]
    assert result["properties"] == original[0]["FindEntity"]["properties"]
    assert result["is_connected_to"]["any"] == [
        {"ref": 1, "constraints": ["$tag", "in", {"array": ["$$a", "b"]}]},
        {"ref": 2},
    ]


def test_dates_ranges_and_json_literals(expressions):
    assert expressions.to_expression({"any": {
        "age": [">=", 20, "<", 90], "name": ["==", "$name"]}}) == [
        "any", ["all", ["$age", ">=", 20], ["$age", "<", 90]],
        ["$name", "==", "$$name"]]
    assert expressions.to_expression({"created": [">", {"_date": "2026-01-01"}]}) == [
        "$created", ">", {"to_date": "2026-01-01"}]
    assert expressions.literal({"lower": "$name"}) == {"json": {"lower": "$name"}}
    assert expressions.to_expression({"age": ["not in", [20, 30]]}) == [
        "$age", "not_in", {"array": [20, 30]}]
    expression = ["$count", ">", 0]
    assert expressions.to_expression(expression) is expression


def test_empty_and_conditional_filters(expressions):
    assert expressions.query_expressions([{"AddEntity": {
        "if_not_found": {"id": ["==", "$id"]}, "constraints": {}}}]) == [
        {"AddEntity": {"if_not_found": ["$id", "==", "$$id"]}}]


@pytest.mark.parametrize("info", [
    [123, False, "String"],
    {"count": 123, "type": "String"},
])
def test_schema_property_formats(info):
    schema = load_fdw_module("schema")
    assert schema.property_info(info) == (123, False, "String")


def test_legacy_index_metadata_is_preserved():
    schema = load_fdw_module("schema")
    assert schema.property_info([7, True, "Number"]) == (7, True, "Number")


def test_object_schema_indexes_come_from_grouped_definitions():
    schema = load_fdw_module("schema")
    data = {"matched": 10, "properties": {
        "name": {"count": 10, "type": "String"},
        "age": {"count": 8, "type": "Number"}}}
    classes = {"Person": data}
    indexes = {"entity": {"Person": {"name": [{"kind": "ordered"}]}}}
    assert schema.needs_index_metadata(classes)
    result = schema.with_index_metadata(classes, indexes, "entity")
    assert result["Person"]["properties"]["name"]["indexed"] is True
    assert result["Person"]["properties"]["age"]["indexed"] is False
    assert "indexed" not in data["properties"]["name"]
    assert not schema.needs_index_metadata(result)
    assert not schema.needs_index_metadata({"Person": {
        "matched": 1, "properties": {"name": [1, True, "String"]}}})
    connections = schema.with_index_metadata({"Knows": [data]}, {
        "connection": {"Knows": {"age": [{"kind": "type"}]}}}, "connection")
    assert connections["Knows"][0]["properties"]["age"]["indexed"] is True


def test_schema_reader_fetches_only_new_index_response(monkeypatch):
    from unittest.mock import MagicMock
    package = types.ModuleType("fdw_api_test")
    package.__path__ = []
    common = types.ModuleType("fdw_api_test.common")
    common.PROXY_SOCKET_PATH = "/unused"
    common.UnixHTTPConnection = MagicMock()
    monkeypatch.setitem(sys.modules, "fdw_api_test", package)
    monkeypatch.setitem(sys.modules, "fdw_api_test.common", common)
    for name in ("expressions", "schema"):
        monkeypatch.setitem(sys.modules, "fdw_api_test." + name, load_fdw_module(name))
    root = Path(os.environ.get("FDW_SOURCE", "/fdw/fdw"))
    spec = importlib.util.spec_from_file_location("fdw_api_test.aperturedb", root / "aperturedb.py")
    api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api)
    execute = MagicMock(return_value=(0, [{"GetIndexes": {
        "status": 0, "indexes": {"entity": {"Person": {"name": [{"kind": "ordered"}]}}}
    }}], []))
    api.execute_query = execute
    classes = {"Person": {"matched": 1, "properties": {"name": {"count": 1, "type": "String"}}}}
    result = api.get_classes("entities", {"entities": {"classes": classes}})
    execute.assert_called_once_with([{"GetIndexes": {}}])
    assert result["Person"]["properties"]["name"]["indexed"] is True
    api.get_classes("entities", {"entities": {"classes": classes}})
    assert execute.call_count == 1


def test_index_enrichment_preserves_explicit_flags():
    schema = load_fdw_module("schema")
    classes = {"Person": {"matched": 1, "properties": {
        "missing": {"count": 1, "type": "String"},
        "indexed": {"count": 1, "type": "String", "indexed": True},
        "unindexed": {"count": 1, "type": "String", "indexed": False},
    }}}
    indexes = {"entity": {"Person": {
        "missing": [{"kind": "ordered"}],
        "unindexed": [{"kind": "ordered"}],
    }}}
    assert schema.needs_index_metadata(classes)
    result = schema.with_index_metadata(classes, indexes, "entity")
    properties = result["Person"]["properties"]
    assert properties["missing"]["indexed"] is True
    assert properties["indexed"]["indexed"] is True
    assert properties["unindexed"]["indexed"] is False
