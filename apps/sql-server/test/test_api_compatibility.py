"""Native FDW planning/schema regressions, without PostgreSQL or ML downloads."""
import importlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import types
from uuid import uuid4

import pytest

ROOT = Path(os.environ.get("FDW_SOURCE", "/fdw/fdw"))


def load_fdw_module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fdw(monkeypatch):
    class Definition:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Wrapper:
        def __init__(self, *args):
            pass

    class Qual:
        def __init__(self, field_name, operator, value):
            self.field_name, self.operator, self.value = field_name, operator, value
    multicorn = types.ModuleType("multicorn")
    multicorn.ColumnDefinition = multicorn.TableDefinition = Definition
    multicorn.ForeignDataWrapper, multicorn.Qual = Wrapper, Qual
    monkeypatch.setitem(sys.modules, "multicorn", multicorn)
    package_name = "fdw_native_" + uuid4().hex
    package = types.ModuleType(package_name)
    package.__path__ = [str(ROOT)]
    monkeypatch.setitem(sys.modules, package_name, package)
    common = importlib.import_module(package_name + ".common")
    monkeypatch.setattr(common, "get_log_level", lambda: logging.WARNING)
    spec = importlib.util.spec_from_file_location(package_name, ROOT / "__init__.py",
                                                  submodule_search_locations=[str(ROOT)])
    spec.loader.exec_module(package)
    yield package, multicorn
    for name in list(sys.modules):
        if name.startswith(package_name + "."):
            del sys.modules[name]


def test_literals_are_native_and_do_not_treat_data_as_expressions():
    expr = load_fdw_module("expressions")
    assert expr.predicate("tag", "in", ["$a", "b"]) == ["$tag", "in", {"array": ["$$a", "b"]}]
    assert expr.predicate("created", ">", {"_date": "2026-01-01"}
                          ) == ["$created", ">", {"to_date": "2026-01-01"}]
    assert expr.literal({"lower": "$name"}) == {"json": {"lower": "$name"}}
    assert expr.predicate("age", "not in", [20, 30]) == ["$age", "not_in", {"array": [20, 30]}]
    assert not hasattr(expr, "query_expressions")


def test_typed_schema_index_flags_are_scoped_to_exact_endpoint_categories():
    schema = load_fdw_module("schema")
    source = {"nodes": [
        {"type": "entity", "class": "Same", "matched": 2,
            "properties": {"id": {"count": 2, "type": "String"}}},
        {"type": "image", "class": "Same", "matched": 1, "properties": {"id": {"count": 1, "type": "String"}}}],
        "edges": [dict(type="connection", **{"class": "Links"}, src_type="entity", src_class="Same",
                       dst_type=target, dst_class="Same", matched=1,
                       properties={"rank": {"count": 1, "type": "Number"}}) for target in ("image", "entity")]}
    indexes = [{"target": "image", "class": "Same", "property": "id"},
               {"target": {"src_type": "entity", "dst_type": "image"}, "class": "Links",
                "src_class": "Same", "dst_class": "Same", "property": "rank"}]
    nodes = schema.native_classes("entities", source, indexes)
    assert set(nodes) == {"entity:Same", "image:Same"}
    assert not nodes["entity:Same"]["properties"]["id"]["indexed"]
    assert nodes["image:Same"]["properties"]["id"]["indexed"]
    edges = schema.native_classes("connections", source, indexes)["Links"]
    assert [edge["properties"]["rank"]["indexed"] for edge in edges] == [True, False]
    assert "indexed" not in source["nodes"][0]["properties"]["id"]


def setup_snapshot(package):
    api = importlib.import_module(package.__name__ + ".aperturedb")
    api._SCHEMA = {"nodes": [
        {"type": "entity", "class": "Shared", "matched": 3,
            "properties": {"name": {"count": 3, "type": "String"}}},
        {"type": "image", "class": "Shared", "matched": 2, "properties": {"name": {"count": 2, "type": "String"}}}],
        "edges": [{"class": "Owns", "src_type": "entity", "src_class": "Shared",
                   "dst_type": "image", "dst_class": "Shared", "matched": 2, "properties": {}}]}
    api._INDEXES = []
    return api


def make_wrapper(package, table):
    return package.FDW(table.options, {column.column_name: column for column in table.columns})


def test_typed_media_tables_and_entity_tables_do_not_collide(fdw):
    package, _ = fdw
    setup_snapshot(package)
    entities = package.FDW.import_schema("entity", {}, {}, None, [])
    media = package.FDW.import_schema("system", {}, {}, None, [])
    assert [table.table_name for table in entities] == ["Shared"]
    table = next(table for table in media if table.table_name == "image:Shared")
    wrapper = make_wrapper(package, table)
    query, rewrite = wrapper._get_query([], {"name", "_uniqueid"})
    body = query[0]["FindImage"]
    assert body["with_class"] == "Shared" and "batch" not in body
    assert body["limit"] > 0 and body["offset"] == 0 and body["uniqueids"]
    assert rewrite is None


@pytest.mark.parametrize('qualifications, expected_rows', [
    ([], 10000),
    ([('name', '=', '$literal')], 1000),
    ([('_uniqueid', '=', '1.2.3')], 10),
    ([('_uniqueid', 'IN', ['1.2.3', '2.3.4'])], 20),
    ([('_uniqueid', ('=', True), ['1.2.3', '2.3.4'])], 20),
    ([('name', '=', 'example'), ('name', '>', 'a')], 500),
])
def test_relation_size_estimation_accepts_native_predicates(fdw, qualifications, expected_rows):
    package, multicorn = fdw
    setup_snapshot(package)
    table = package.FDW.import_schema('entity', {}, {}, None, [])[0]
    wrapper = make_wrapper(package, table)
    wrapper._options.count = 1000
    quals = [multicorn.Qual(*qual) for qual in qualifications]
    assert wrapper.get_rel_size(quals, {'name', '_uniqueid'}) == (expected_rows, 56)


def test_endpoint_quals_use_typed_object_references_and_keep_other_filters(fdw):
    package, multicorn = fdw
    setup_snapshot(package)
    table = package.FDW.import_schema("connection", {}, {}, None, [])[0]
    wrapper = make_wrapper(package, table)
    query, rewrite = wrapper._get_query([
        multicorn.Qual("_src", "=", "1.2.3"), multicorn.Qual("_dst", "IN", ["2.3.4"])], {"_src", "_dst"})
    assert list(query[0]) == ["FindEntity"] and list(query[1]) == ["FindImage"]
    assert query[0]["FindEntity"]["with_class"] == "Shared"
    assert query[1]["FindImage"]["constraints"] == ["$_uniqueid", "in", {"array": ["2.3.4"]}]
    body = query[-1]["FindConnection"]
    assert body["with_class"] == "Owns" and body["src"] == 1 and body["dst"] == 2
    assert "constraints" not in body and "batch" not in body and rewrite is None


def test_mixed_endpoint_connection_tables_are_retained_without_guessing_types(fdw):
    package, multicorn = fdw
    api = setup_snapshot(package)
    api._SCHEMA["edges"].append({**api._SCHEMA["edges"][0], "dst_type": "entity"})
    table = package.FDW.import_schema("connection", {}, {}, None, [])[0]
    wrapper = make_wrapper(package, table)
    query, _ = wrapper._get_query([multicorn.Qual("_src", "=", "1.2.3")], {"_src", "_dst"})
    assert len(query) == 1 and query[0]["FindConnection"]["with_class"] == "Owns"
    assert "constraints" not in query[0]["FindConnection"]


def test_paging_does_not_mutate_the_previous_query_and_stops_on_a_short_page(fdw):
    package, _ = fdw
    wrapper = object.__new__(package.FDW)
    original = [{"FindImage": {"with_class": "Photos", "offset": 0, "limit": 10}}]
    following = wrapper._get_next_query(original, [{"FindImage": {"status": 0, "returned": 10}}])
    assert following[0]["FindImage"]["offset"] == 10 and original[0]["FindImage"]["offset"] == 0
    assert wrapper._get_next_query(
        following, [{"FindImage": {"status": 0, "returned": 3}}]) is None


@pytest.mark.skipif(not os.getenv("WORKFLOWS_TEST_PORT"), reason="Set a private server-1.0 endpoint")
def test_real_fdw_import_query_paging_vector_metadata_and_typed_endpoint_filters(fdw, monkeypatch):
    import struct
    from aperturedb.Connector import Connector
    from aperturedb.CommonLibrary import execute_query
    from aperturedb.Utils import Utils
    package, multicorn = fdw
    api = importlib.import_module(package.__name__ + ".aperturedb")
    prefix = "SQLNative" + uuid4().hex
    people, vectors = prefix + "People", prefix + "Vectors"
    db = Connector(host="127.0.0.1", port=int(
        os.environ["WORKFLOWS_TEST_PORT"]), user="admin", password="admin", use_ssl=False)

    def query(commands, blobs=None): return execute_query(db, commands, blobs or [])
    monkeypatch.setattr(api, "execute_query", query)
    monkeypatch.setattr(package, "execute_query", query)
    try:
        assert Utils(db).create_vector_index(vectors, 2, metric="L2", engines={
            "flat": {}}, metadata={"fingerprint": "preserved"})
        payload = struct.pack("2f", 1., 0.)
        status, response, _ = query([
            {"AddEntity": {"class": people, "_ref": 1, "properties": {"name": "$literal"}}},
            {"AddDescriptor": {"class": vectors, "connect": {"ref": 1, "class": prefix + "HasVector", "direction": "in"}, "properties": {"name": "near"}}}], [payload])
        assert status == 0, response
        api._SCHEMA = query([{"GetSchema": {}}])[1][0]["GetSchema"]
        api._INDEXES = query([{"GetIndexes": {}}])[1][0]["GetIndexes"]["indexes"]
        table = next(table for table in package.FDW.import_schema(
            "entity", {}, {}, None, []) if table.table_name == people)
        wrapper = make_wrapper(package, table)
        assert list(wrapper.execute([multicorn.Qual("name", "=", "$literal")], {"name"})) == [
            {"name": "$literal"}]
        status, response, _ = query(
            [{"AddEntity": {"class": people, "properties": {"name": f"row{i}"}}} for i in range(102)])
        assert status == 0, response
        rows = list(wrapper.execute([], {"name"}))
        assert len(rows) == 103 and len({row["name"] for row in rows}) == 103
        from io import BytesIO
        from PIL import Image
        buffer = BytesIO()
        Image.new("RGB", (2, 2), "blue").save(buffer, format="PNG")
        status, response, _ = query(
            [{"AddImage": {"class": people, "properties": {"name": "image"}}}], [buffer.getvalue()])
        assert status == 0, response
        api._SCHEMA = query([{"GetSchema": {}}])[1][0]["GetSchema"]
        media = next(table for table in package.FDW.import_schema(
            "system", {}, {}, None, []) if table.table_name == "image:" + people)
        assert list(make_wrapper(package, media).execute([], {"name"})) == [{"name": "image"}]

        descriptor = importlib.import_module(package.__name__ + ".descriptor")
        assert descriptor.get_descriptor_sets()[vectors]["fingerprint"] == "preserved"
        table = next(table for table in package.FDW.import_schema(
            "descriptor", {}, {}, None, []) if table.table_name == vectors)
        wrapper = make_wrapper(package, table)
        rows = list(wrapper.execute([multicorn.Qual("_find_similar", "=", json.dumps(
            {"vector": [1., 0.], "k_neighbors": 1}))], {"name", "_find_similar"}))
        assert len(rows) == 1 and rows[0]["name"] == "near"
        table = next(table for table in package.FDW.import_schema(
            "connection", {}, {}, None, []) if table.table_name == prefix + "HasVector")
        wrapper = make_wrapper(package, table)
        owner_id = db.query([{"FindEntity": {"with_class": people, "uniqueids": True, "constraints": [
                            "$name", "==", "$$literal"]}}])[0][0]["FindEntity"]["entities"][0]["_uniqueid"]
        rows = list(wrapper.execute([multicorn.Qual("_src", "=", owner_id)], {"_src", "_dst"}))
        assert len(rows) == 1 and rows[0]["_src"] == owner_id
    finally:
        Utils(db).remove_descriptorset(vectors)
        db.query([{"DeleteImage": {"with_class": people}},
                 {"DeleteEntity": {"with_class": people}}])
        db.close()
