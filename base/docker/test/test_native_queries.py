"""Server-1.0 workflow regressions without models, downloads or cloud services."""
import ast
import importlib.util
import logging
import math
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest
from uuid import uuid4

from aperturedb.Constraints import predicate

ROOT = Path(os.getenv("WORKFLOWS_SOURCE", "/workflows"))
GENERATORS = [
    ("embeddings-extraction", "images", "FindImageQueryGenerator", "FindImage"),
    ("embeddings-extraction", "pdfs", "FindPDFQueryGenerator", "FindBlob"),
    ("embeddings-extraction", "videos", "FindVideoQueryGenerator", "FindVideo"),
    ("caption-image", "images", "FindImageQueryGenerator", "FindImage"),
    ("face-detection", "face_detection", "FindImageQueryGenerator", "FindImage"),
    ("ocr-extraction", "image_ocr", "FindImageOCRQueryGenerator", "FindImage"),
    ("ocr-extraction", "pdf_ocr", "FindPDFOCRQueryGenerator", "FindBlob"),
]


def load_generator(app, module, class_name, pool):
    path = ROOT / "apps" / app / "app" / (module + ".py")
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    module_ast = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[
                    ast.alias(
                        name="annotations")],
                level=0),
            node],
        type_ignores=[])
    namespace = dict(
        QueryGenerator=SimpleNamespace(
            QueryGenerator=object),
        math=math,
        logger=logging.getLogger("workflow-test"),
        predicate=predicate,
        TextExtractor=lambda: object(),
        TextSegmenter=lambda **kw: object(),
        ConnectionPool=lambda **kw: pool,
        PROCESSED_LABEL_IMAGES="done",
        MAX_TOKENS=100,
        OVERLAP_TOKENS=10)
    exec(compile(ast.fix_missing_locations(module_ast), str(path), "exec"), namespace)
    return namespace[class_name]


class NativeQueryTests(unittest.TestCase):
    def test_clean_embedding_run_removes_old_spec_before_ensuring_vector_index(self):
        source = ROOT / "apps/text-embeddings/app/app.py"
        run = next(node for node in ast.parse(source.read_text()).body
                   if isinstance(node, ast.FunctionDef) and node.name == "run_text_embeddings")
        operations = Mock()
        io = operations.io
        io.get_segments.return_value = []
        from contextlib import nullcontext
        embedder = operations.Embedder
        embedder.parse_string.return_value = {}
        namespace = dict(logger=logging.getLogger("embedding-clean"), uuid4=uuid4,
                         AperturedbIO=lambda **kwargs: nullcontext(io), Embedder=embedder)
        exec(compile(ast.Module(body=[run], type_ignores=[]), str(source), "exec"), namespace)
        args = SimpleNamespace(input="source", output="selected", descriptorset="shared",
                               engine="HNSW", model="model", clean=True, delete=False, delete_all=False)
        namespace["run_text_embeddings"](args)
        calls = [step[0] for step in operations.mock_calls]
        self.assertLess(calls.index("io.delete_spec"), calls.index("Embedder.from_new_descriptor_set"))
        io.delete_spec.assert_called_once_with("selected")
        embedder.from_new_descriptor_set.assert_called_once_with(
            client=io.db, descriptor_set="shared", engine="HNSW")

    def test_crawl_run_preserves_scrapy_counters_and_reports_write_failure(self):
        import json
        import re
        from datetime import datetime, timezone

        source = ROOT / "apps/crawl-website/app/crawl_website.py"
        function = next(n for n in ast.parse(source.read_text()).body
                        if isinstance(n, ast.FunctionDef) and n.name == "create_run")
        execute_query = Mock(return_value=(0, [], []))
        namespace = dict(datetime=datetime, logging=logging, re=re, json=json,
                         execute_query=execute_query, error_urls=None)
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        finished = datetime(2026, 1, 2, tzinfo=timezone.utc)
        dynamic_stats = {"spider/domain/docs.aperturedata.dev": 2,
                         "spider/domain/" + "a" * 64: 1, "extra_stats": 7}
        stats = {"itempipeline/item_count": 3, "finish_time": finished, **dynamic_stats}
        namespace["create_run"](None, "spec", "run", stats)
        properties = execute_query.call_args.args[1][1]["AddEntity"]["properties"]
        self.assertEqual(properties["itempipeline/item_count"], 3)
        self.assertEqual(properties["finish_time"], {"_date": finished.isoformat()})
        self.assertEqual(json.loads(properties["extra_stats"]), dynamic_stats)
        self.assertEqual(properties["id"], "run")
        self.assertEqual(properties["spec_id"], "spec")

        execute_query.return_value = (1, {}, [])
        with self.assertRaisesRegex(RuntimeError, "Could not record CrawlRun"):
            namespace["create_run"](None, "spec", "run", {})

    def test_mutating_generators_keep_disjoint_id_batches_after_completion_changes(self):
        import inspect
        for app, module, name, command in GENERATORS:
            with self.subTest(app=app, module=module):
                selected = [f"id{i}" for i in range(65)]

                class Pool:
                    def execute_query(self, query, *args):
                        self.query = query
                        return 0, [{command: {"status": 0, "returned": len(selected),
                                              "entities": [{"_uniqueid": value} for value in selected]}}], []

                    def clone(self):
                        return self
                pool = Pool()
                cls = load_generator(app, module, name, pool)
                options = dict(
                    pool=pool,
                    db=pool,
                    embedder=SimpleNamespace(
                        context_length=100),
                    done_property="done",
                    caption_image_property="caption",
                    ocr=object(),
                    generate_embeddings=False)
                signature = inspect.signature(cls)
                instance = cls(**{key: value for key,
                                  value in options.items() if key in signature.parameters})
                first_query = pool.query[0][command]
                self.assertTrue(first_query["uniqueids"])
                self.assertNotIn("count", first_query["results"])
                collected = []
                for index in range(len(instance)):
                    # Response handlers remove objects from the unfinished
                    # selection. Later batches must retain the initial IDs.
                    if index == 1:
                        selected[:] = selected[instance.batch_size:]
                    query, _ = instance.getitem(index)
                    body = query[0][command]
                    self.assertNotIn("batch", body)
                    condition = body["constraints"]
                    self.assertEqual(condition[:2], ["$_uniqueid", "in"])
                    collected.extend(condition[2]["array"])
                self.assertEqual(collected, [f"id{i}" for i in range(65)])
                self.assertEqual(len(collected), len(set(collected)))

    def test_text_job_requires_one_vector_index_before_processing(self):
        path = ROOT / "apps/text-embeddings/app/aperturedb_io.py"
        cls = next(node for node in ast.parse(path.read_text()).body
                   if isinstance(node, ast.ClassDef) and node.name == "AperturedbIO")
        method = next(node for node in cls.body
                      if isinstance(node, ast.FunctionDef) and node.name == "connect_descriptor_set")
        namespace = {}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[method], type_ignores=[])),
                     str(path), "exec"), namespace)
        for count in (0, 2):
            with self.subTest(indexes=count):
                io = SimpleNamespace(descriptorset_name="MissingOrAmbiguous", engine="IVF",
                                     execute_query=lambda query: ([{"GetIndexes": {
                                         "status": 0, "indexes": [{}] * count}}], []))
                with self.assertRaisesRegex(ValueError, f"Expected one vector index.*found {count}"):
                    namespace["connect_descriptor_set"](io)
        io = SimpleNamespace(descriptorset_name="Ready", engine="HNSW",
                             execute_query=Mock(return_value=([{"GetIndexes": {
                                 "status": 0, "indexes": [{}]}}], [])))
        namespace["connect_descriptor_set"](io)
        self.assertEqual(io.execute_query.call_args.args[0][0]["GetIndexes"]["kind"], "hnsw")

    def test_embedding_metadata_updates_only_the_selected_kind(self):
        path = ROOT / "base/docker/scripts/embeddings/aperturedb_io.py"
        spec = importlib.util.spec_from_file_location("vector_io", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        client = Mock()
        indexes = [{"kind": "flat", "params": {"metric": ["IP"]}, "metadata": {"tag": "exact"}},
                   {"kind": "hnsw", "params": {"metric": ["L2"]}, "metadata": {"tag": "graph"}}]
        with patch.object(module, "Utils") as utils, patch.object(module, "_execute") as execute:
            utils.return_value.get_vector_indexes.return_value = indexes
            self.assertEqual(module.find_descriptor_set(client, "Images")["tag"], "exact")
            module.update_descriptor_set(client, "Images", {"model": "example"}, engine="HNSW")
            update = execute.call_args.args[1][0]["UpdateIndex"]
            self.assertEqual(update["kind"], "hnsw")
            self.assertEqual(update["metadata"], {"tag": "graph", "model": "example"})

    def test_generated_connection_csvs_name_media_and_vector_classes(self):
        import csv
        import tempfile
        path = ROOT / "apps/dataset-ingestion/app/native_csv.py"
        spec = importlib.util.spec_from_file_location("workflow_native_csv", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "images_model_metadata.adb.csv").write_text("set,id\nVectors,vector1\n")
            path = root / "images_model_connection.adb.csv"
            path.write_text("ConnectionClass,_Image@id,_Descriptor@id\nHasVector,image1,vector1\n")
            module.prepare_connections(root)
            with path.open() as source:
                rows = list(csv.reader(source))
            self.assertEqual(rows, [["ConnectionClass", "image:Images@id",
                             "descriptor:Vectors@id"], ["HasVector", "image1", "vector1"]])
            module.prepare_connections(root)
            with path.open() as source:
                self.assertEqual(list(csv.reader(source)), rows)

    @unittest.skipUnless(os.getenv("WORKFLOWS_TEST_PORT"), "Set WORKFLOWS_TEST_PORT for a private 1.0 DB")
    def test_video_workflow_reads_clips_through_the_native_owner_reference(self):
        import shutil
        import subprocess
        import tempfile
        from aperturedb.Connector import Connector
        executable = shutil.which("ffmpeg")
        if executable is None:
            self.skipTest("ffmpeg is required to create the synthetic video")
        path = ROOT / "apps/embeddings-extraction/test/test_video.py"
        fixture = next(node for node in ast.parse(path.read_text()).body
                       if isinstance(node, ast.FunctionDef) and node.name == "run_query")
        query = ast.literal_eval(next(node.value for node in fixture.body
                                      if isinstance(node, ast.Assign)
                                      and any(isinstance(target, ast.Name) and target.id == "query"
                                              for target in node.targets)))[:2]
        prefix = "WorkflowOwners" + uuid4().hex
        video_class, clip_class = prefix + "Videos", prefix + "Clips"
        query[0]["FindVideo"]["with_class"] = video_class
        query[1]["FindClip"]["with_class"] = clip_class
        db = Connector(host="127.0.0.1", port=int(os.environ["WORKFLOWS_TEST_PORT"]),
                       user="admin", password="admin", use_ssl=False)
        try:
            with tempfile.TemporaryDirectory() as directory:
                video = Path(directory) / "tiny.mp4"
                subprocess.run([executable, "-hide_banner", "-loglevel", "error",
                                "-f", "lavfi", "-i", "color=c=blue:s=16x16:r=2:d=2",
                                "-an", "-c:v", "mpeg4", "-y", str(video)], check=True)
                response, _ = db.query([
                    {"AddVideo": {"class": video_class, "_ref": 1}},
                    {"AddVideo": {"class": video_class, "_ref": 2}},
                    {"AddClip": {"class": clip_class, "video_ref": 1,
                                 "frame_number_range": {"start": 0, "stop": 1}}},
                    {"AddClip": {"class": clip_class, "video_ref": 2,
                                 "frame_number_range": {"start": 1, "stop": 2}}}],
                    [video.read_bytes()] * 2)
            self.assertTrue(all(next(iter(item.values()))["status"] == 0
                                for item in response), response)
            response, _ = db.query(query)
            self.assertEqual(response[0]["FindVideo"]["returned"], 2, response)
            self.assertEqual(response[1]["FindClip"]["count"], 2, response)
        finally:
            db.query([{"DeleteClip": {"with_class": clip_class}},
                      {"DeleteVideo": {"with_class": video_class}}])
            db.close()

    @unittest.skipUnless(os.getenv("WORKFLOWS_TEST_PORT"), "Set WORKFLOWS_TEST_PORT for a private 1.0 DB")
    def test_movie_cleanup_preserves_other_datasets_in_the_shared_vector_class(self):
        import struct
        from aperturedb.CommonLibrary import execute_query
        from aperturedb.Connector import Connector
        from aperturedb.Utils import Utils
        source = ROOT / "apps/dataset-ingestion-movies/app/ingest_movies.py"
        node = next(n for n in ast.parse(source.read_text()).body if isinstance(
            n, ast.FunctionDef) and n.name == "cleanup_movies")
        prefix = "MovieCleanup" + uuid4().hex
        namespace = dict(execute_query=execute_query,
                         predicate=predicate, DATASET_NAME=prefix + "A")
        exec(compile(ast.fix_missing_locations(ast.Module(
            body=[node], type_ignores=[])), str(source), "exec"), namespace)
        db = Connector(host="127.0.0.1", port=int(
            os.environ["WORKFLOWS_TEST_PORT"]), user="admin", password="admin", use_ssl=False)
        try:
            assert Utils(db).create_vector_index(prefix, 2, metric="L2", engines={
                "flat": {}}, metadata={"dataset_name": prefix + "A", "fingerprint": "model"})
            status, response, _ = execute_query(db, [
                {"AddDescriptor": {"class": prefix, "properties": {"dataset_name": prefix + "A"}}},
                {"AddDescriptor": {"class": prefix, "properties": {"dataset_name": prefix + "B"}}}], [struct.pack("2f", 1., 0.)] * 2)
            self.assertEqual(status, 0, response)
            namespace["cleanup_movies"](db)
            response, _ = db.query(
                [{"FindDescriptor": {"with_class": prefix, "results": {"list": ["dataset_name"]}}}])
            self.assertEqual([entity["dataset_name"] for entity in response[0]
                             ["FindDescriptor"]["entities"]], [prefix + "B"])
            index = Utils(db).get_indexes(target="descriptor", class_name=prefix, kind="flat")[0]
            self.assertEqual(index["metadata"]["fingerprint"], "model")
        finally:
            Utils(db).remove_descriptorset(prefix)
            db.close()

    @unittest.skipUnless(os.getenv("WORKFLOWS_TEST_PORT"), "Set WORKFLOWS_TEST_PORT for a private 1.0 DB")
    def test_text_embedding_job_uses_native_indexes_and_cleans_its_class(self):
        import json
        from datetime import datetime, timezone
        import numpy as np
        from aperturedb.CommonLibrary import execute_query
        from aperturedb.Connector import Connector
        from aperturedb.Utils import Utils
        source = ROOT / "apps/text-embeddings/app/aperturedb_io.py"
        node = next(n for n in ast.parse(source.read_text()).body if isinstance(
            n, ast.ClassDef) and n.name == "AperturedbIO")
        prefix = "TextNative" + uuid4().hex
        spec_class, input_class, run_class = prefix + "Spec", prefix + "Input", prefix + "Run"
        namespace = dict(logger=logging.getLogger("text-native"), logging=logging, json=json,
                         execute_query=execute_query, predicate=predicate, uuid4=uuid4, datetime=datetime,
                         timezone=timezone, Segment=lambda **kw: SimpleNamespace(**kw),
                         SPEC_CLASS=spec_class, INPUT_SPEC_CLASS=input_class, RUN_CLASS=run_class)
        module_ast = ast.Module(body=[ast.ImportFrom(module="__future__", names=[
                                ast.alias(name="annotations")], level=0), node], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module_ast), str(source), "exec"), namespace)
        batcher_path = ROOT / "base/docker/scripts/batcher.py"
        definition = importlib.util.spec_from_file_location(
            "native_symbolic_batcher", batcher_path)
        batcher = importlib.util.module_from_spec(definition)
        definition.loader.exec_module(batcher)
        db = Connector(host="127.0.0.1", port=int(
            os.environ["WORKFLOWS_TEST_PORT"]), user="admin", password="admin", use_ssl=False)
        io = object.__new__(namespace["AperturedbIO"])
        io.db, io.input_spec_id, io.spec_id, io.run_id = db, prefix + "input", prefix + "spec", prefix + "run"
        io.descriptorset_name, io.engine = prefix + "Vectors", "Flat"
        embedder = SimpleNamespace(model_spec="test/model", metric="L2",
                                      dimensions=2, fingerprint_hash=lambda: "test-fingerprint")
        io.start_time, io.batch_size, io.n_embeddings = datetime.now(timezone.utc), 2, 0
        io.batcher = batcher.SymbolicBatcher(
            io.execute_query, batch_size=20, prolog=io._batcher_prolog)
        ids = [prefix + "segment" + str(i) for i in range(3)]
        try:
            with self.assertRaisesRegex(ValueError, "Expected one vector index.*found 0"):
                io.connect_descriptor_set()
            assert Utils(db).create_vector_index(
                io.descriptorset_name, 2, metric="L2", engines={"flat": {}})
            commands = [{"AddEntity": {"class": input_class,
                                       "_ref": 1, "properties": {"id": io.input_spec_id}}}]
            commands.extend({"AddEntity": {"class": "Segment", "properties": {"id": value, "url": "test://text", "text": value},
                                           "connect": {"ref": 1, "class": "segmentationSpecHasSegment", "direction": "in"}}} for value in ids)
            status, response, _ = execute_query(db, commands)
            self.assertEqual(status, 0, response)
            io.create_indexes()
            io.create_spec(embedder)
            io.connect_descriptor_set()
            segments = list(io.get_segments())
            self.assertEqual({segment.id for segment in segments}, set(ids))
            for segment in segments:
                io.create_embedding(SimpleNamespace(segment_id=segment.id, text=segment.text, url=segment.url,
                                                    title=None, vector=np.array([1., 0.], dtype=np.float32)))
            io.batcher.flush()
            io.create_run()
            self.assertEqual(Utils(db).count_descriptors_in_set(io.descriptorset_name), 3)
            io.delete_spec(io.spec_id)
            self.assertNotIn(io.descriptorset_name, Utils(db).get_descriptorset_list())
            # A delete-only invocation must not create an orphan EmbeddingsRun
            # while leaving the context manager.
            namespace["create_connector"] = lambda: db
            namespace["SymbolicBatcher"] = batcher.SymbolicBatcher
            with namespace["AperturedbIO"](io.input_spec_id, io.spec_id, io.run_id,
                                           io.descriptorset_name, io.engine) as cleanup:
                cleanup.delete_spec(io.spec_id)
            self.assertEqual(Utils(db).count_entities(run_class), 0)
            self.assertEqual(Utils(db).count_entities(input_class), 1)
        finally:
            Utils(db).remove_descriptorset(io.descriptorset_name)
            db.query([{"DeleteEntity": {"with_class": name}} for name in (input_class, spec_class, run_class)] +
                     [{"DeleteEntity": {"with_class": "Segment", "constraints": predicate("id", "in", ids)}}])
            db.close()

    @unittest.skipUnless(os.getenv("WORKFLOWS_TEST_PORT"), "Set WORKFLOWS_TEST_PORT for a private 1.0 DB")
    def test_real_completion_updates_do_not_skip_later_image_pages(self):
        from io import BytesIO
        from PIL import Image
        from aperturedb.CommonLibrary import execute_query
        from aperturedb.Connector import Connector
        prefix = "WorkflowPages" + uuid4().hex
        done = "done_" + uuid4().hex
        db = Connector(host="127.0.0.1", port=int(os.environ["WORKFLOWS_TEST_PORT"]),
                       user="admin", password="admin", use_ssl=False)
        buffer = BytesIO()
        Image.new("RGB", (2, 2), "blue").save(buffer, format="PNG")
        payload = buffer.getvalue()

        class Pool:
            def execute_query(self, query, blobs=None):
                return execute_query(db, query, blobs or [])
        pool = Pool()
        try:
            commands = [{"AddImage": {"class": prefix, "properties": {"sequence": i}}}
                        for i in range(65)]
            status, response, _ = execute_query(db, commands, [payload] * 65)
            self.assertEqual(status, 0, response)
            cls = load_generator("embeddings-extraction", "images",
                                 "FindImageQueryGenerator", pool)
            generator = cls(pool, object(), done)
            selected_count = len(generator._selected_ids)
            self.assertGreaterEqual(selected_count, 65)
            self.assertEqual(len(generator), math.ceil(selected_count / generator.batch_size))
            seen = []
            for index in range(len(generator)):
                query, blobs = generator.getitem(index)
                status, response, data = pool.execute_query(query, blobs)
                self.assertEqual(status, 0, response)
                ids = [entity["_uniqueid"]
                       for entity in response[0]["FindImage"].get("entities", [])]
                self.assertEqual(len(ids), len(data))
                seen.extend(ids)
                response, _ = db.query([{"UpdateImage": {"with_class": prefix,
                                                         "constraints": predicate("_uniqueid", "in", ids), "properties": {done: True}}}])
                self.assertEqual(response[0]["UpdateImage"]["status"], 0, response)
            self.assertEqual(len(seen), selected_count)
            self.assertEqual(len(set(seen)), selected_count)
            response, _ = db.query([{"FindImage": {"with_class": prefix,
                                                   "constraints": predicate(done, "==", True), "results": {"count": True}}}])
            self.assertEqual(response[0]["FindImage"]["count"], 65)
        finally:
            db.query([{"DeleteImage": {"with_class": prefix}}])
            db.close()

    @unittest.skipUnless(os.getenv("WORKFLOWS_TEST_PORT"),
                         "Set WORKFLOWS_TEST_PORT for a private 1.0 DB")
    def test_vector_metadata_update_search_and_dependent_class_cleanup(self):
        import numpy as np
        from aperturedb.CommonLibrary import execute_query
        from aperturedb.Connector import Connector
        from aperturedb.Utils import Utils
        prefix = "WorkflowNative" + uuid4().hex
        path = ROOT / "base/docker/scripts/embeddings/aperturedb_io.py"
        spec = importlib.util.spec_from_file_location("workflow_vector_io", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        db = Connector(host="127.0.0.1", port=int(os.environ["WORKFLOWS_TEST_PORT"]),
                       user="admin", password="admin", use_ssl=False)
        owner, vectors = prefix + "Owner", prefix + "Vectors"
        try:
            self.assertEqual(db.hello_version, 2)
            module.add_descriptor_set(db,
                                      vectors,
                                      "L2",
                                      2,
                                      "Flat",
                                      {"embeddings_provider": "test",
                                       "embeddings_model": "model-v1",
                                       "embeddings_pretrained": "test",
                                       "embeddings_fingerprint": "fingerprint",
                                       "custom": 7})
            module.update_descriptor_set(db, vectors, {"embeddings_model": "model-v2"})
            metadata = module.find_descriptor_set(db, vectors)
            self.assertEqual(metadata["embeddings_fingerprint"], "fingerprint")
            self.assertEqual(metadata["embeddings_model"], "model-v2")
            self.assertEqual(metadata["custom"], 7)
            vector = np.array([1., 0.], dtype=np.float32).tobytes()
            status, response, _ = execute_query(db, [
                {"AddEntity": {"class": owner, "_ref": 1}},
                {"AddDescriptor": {"class": vectors, "connect": {"ref": 1, "class": "HasVector"},
                                   "properties": {"id": "first"}}}], [vector])
            self.assertEqual(status, 0, response)
            response, _ = db.query([{"FindDescriptor": {"with_class": vectors,
                                                        "engine": "Flat", "metric": "L2", "k_neighbors": 1,
                                                        "constraints": predicate("id", "==", "first"), "results": {"list": ["id"]}}}], [vector])
            self.assertEqual(response[0]["FindDescriptor"]["entities"][0]["id"], "first")
            module.delete_descriptor_set(db, vectors)
            self.assertIsNone(module.find_descriptor_set(db, vectors))
            self.assertEqual(Utils(db).count_entities(owner), 1)
        finally:
            Utils(db).remove_descriptorset(vectors)
            db.query([{"DeleteEntity": {"with_class": owner}}])
            db.close()
