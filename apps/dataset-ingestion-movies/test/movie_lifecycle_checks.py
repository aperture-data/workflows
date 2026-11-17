"""Explicit Compose unit checks; the original four live pytest cases are unchanged."""
from contextlib import redirect_stdout
import importlib.util
import io
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

# Keep the real dataframe runtime loaded across temporary dependency stubs.
import pandas
from aperturedb.Constraints import predicate


APP = Path(os.getenv('WORKFLOW_APP', '/app'))


class Client:
    def __init__(self, name, events):
        self.name = name
        self.events = events
        self.closed = False
        self.close_calls = 0

    def close(self):
        self.closed = True
        self.close_calls += 1
        self.events.append('close:' + self.name)


class MovieLifecycleTests(unittest.TestCase):
    def run_workflow(self, *, setup_error=False, preparation_error=False,
                     loading_error=False, loader_errors=0, sample_count=-1):
        self.events = []
        self.setup = Client('setup', self.events)
        self.loading = Client('loading', self.events)
        self.prepared = []
        self.connections = []
        self.ingested = None
        expected = 1 if sample_count == 1 else 2

        def connect():
            self.assertLess(len(self.connections), 2)
            if not self.connections:
                client = self.setup
            else:
                self.assertEqual(len(self.prepared), expected)
                self.assertTrue(self.setup.closed)
                client = self.loading
            self.connections.append(client)
            self.events.append('connect:' + client.name)
            return client

        def cleanup(client):
            self.assertIs(client, self.setup)
            self.assertFalse(client.closed)
            self.events.append('cleanup')

        def model(client, *args, **kwargs):
            self.assertIs(client, self.setup)
            self.assertFalse(client.closed)
            self.assertEqual(kwargs['model_name'], 'ViT-B/16')
            self.events.append('model')
            if setup_error:
                raise ValueError('model initialization failed')
            return 'original-model'

        def prepare(record, embedder, ingest_posters, embed_tagline):
            self.assertTrue(self.setup.closed)
            self.assertEqual(self.setup.close_calls, 1)
            self.assertEqual(len(self.connections), 1)
            self.assertEqual(embedder, 'original-model')
            self.assertTrue(ingest_posters)
            self.assertTrue(embed_tagline)
            self.events.append('prepare')
            if preparation_error:
                raise ValueError('record preparation failed')
            self.prepared.append(record)
            return (record, [])

        def utils(client):
            self.assertIs(client, self.loading)
            self.assertFalse(client.closed)
            self.events.append('utils')
            return 'fresh-utils'

        def indexes(value):
            self.assertEqual(value, 'fresh-utils')
            self.events.append('indexes')

        owner = self

        class Loader:
            error_counter = loader_errors

            def __init__(self, client):
                owner.assertIs(client, owner.loading)
                owner.assertFalse(client.closed)
                owner.events.append('loader')

            @staticmethod
            def setSuccessStatus(statuses):
                owner.assertEqual(statuses, [0, 2])

            def ingest(self, parser, **kwargs):
                owner.assertFalse(owner.loading.closed)
                owner.assertEqual(len(owner.prepared), expected)
                owner.ingested = parser
                owner.assertEqual(kwargs, {'batchsize': 10, 'numthreads': 8, 'stats': True})
                owner.events.append('ingest')
                if loading_error:
                    raise ConnectionError('loading failed')

        dataset = SimpleNamespace(
            metadata=SimpleNamespace(record_sets=[SimpleNamespace(uuid='credits'),
                                                  SimpleNamespace(uuid='movies')]),
            records=lambda record_set: ([{'tmdb_5000_movies.csv/id': value} for value in [1, 2]]
                                        if record_set == 'movies' else
                                        [{'tmdb_5000_credits.csv/movie_id': value} for value in [1, 2]]))
        dependencies = {
            'aperturedb.Constraints': SimpleNamespace(predicate=predicate),
            'aperturedb.ParallelLoader': SimpleNamespace(ParallelLoader=Loader),
            'aperturedb.CommonLibrary': SimpleNamespace(create_connector=connect, execute_query=Mock()),
            'aperturedb.Utils': SimpleNamespace(Utils=utils),
            'embeddings': SimpleNamespace(Embedder=SimpleNamespace(from_new_descriptor_set=model),
                                          DEFAULT_MODEL='unchanged'),
            'movie_record': SimpleNamespace(make_movie_with_all_connections=prepare,
                                            create_indexes=indexes, DATASET_NAME='fixture'),
            'movie_parser': SimpleNamespace(MovieParser=lambda collection: list(collection)),
            'movie_dataset': SimpleNamespace(load_movie_dataset=lambda *args: dataset),
        }
        spec = importlib.util.spec_from_file_location('movie_lifecycle_subject', APP / 'ingest_movies.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, dependencies):
            spec.loader.exec_module(module)
        with patch.object(module, 'cleanup_movies', cleanup), \
                patch.object(module, 'tqdm', lambda records: records), \
                redirect_stdout(io.StringIO()):
            module.ingest_movies(ingest_posters=True, embed_tagline=True,
                                 sample_count=sample_count, metadata_file=None, archive_file=None)

    def test_setup_closes_before_preparation_and_fresh_client_loads_all_records(self):
        self.run_workflow()
        self.assertEqual(len(self.prepared), 2)
        self.assertEqual(len(self.ingested), 2)
        self.assertEqual(self.events, ['connect:setup', 'cleanup', 'model', 'close:setup',
                                       'prepare', 'prepare', 'connect:loading', 'utils',
                                       'indexes', 'loader', 'ingest', 'close:loading'])
        self.assertEqual(self.loading.close_calls, 1)

    def test_setup_failure_closes_initial_client_without_preparing_or_connecting_again(self):
        with self.assertRaisesRegex(ValueError, 'model initialization failed'):
            self.run_workflow(setup_error=True)
        self.assertEqual(self.setup.close_calls, 1)
        self.assertEqual(len(self.connections), 1)
        self.assertEqual(self.prepared, [])

    def test_preparation_failure_never_opens_a_loading_connection(self):
        with self.assertRaisesRegex(ValueError, 'record preparation failed'):
            self.run_workflow(preparation_error=True)
        self.assertEqual(self.setup.close_calls, 1)
        self.assertEqual(len(self.connections), 1)
        self.assertFalse(self.loading.closed)

    def test_loading_failure_propagates_and_closes_the_fresh_client(self):
        with self.assertRaisesRegex(ConnectionError, 'loading failed'):
            self.run_workflow(loading_error=True)
        self.assertEqual(self.setup.close_calls, 1)
        self.assertEqual(self.loading.close_calls, 1)

    def test_worker_errors_fail_the_workflow_and_close_the_fresh_client(self):
        with self.assertRaisesRegex(RuntimeError, 'failed'):
            self.run_workflow(loader_errors=1)
        self.assertEqual(self.loading.close_calls, 1)
        self.assertIn('ingest', self.events)

    def test_sample_limit_is_applied_before_preparation_and_loading(self):
        self.run_workflow(sample_count=1)
        self.assertEqual(len(self.prepared), 1)
        self.assertEqual(len(self.ingested), 1)
        self.assertEqual(self.loading.close_calls, 1)


if __name__ == '__main__':
    unittest.main()
