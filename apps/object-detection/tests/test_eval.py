"""Run in the object-detection Compose image with this directory mounted at /tests."""
from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
import io
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np

sys.path.insert(0, '/app')
import eval as app


class Pool:
    def __init__(self, ids, processed=(), fail_write=False):
        self.ids = ids
        self.processed = set(processed)
        self.fail_write = fail_write
        self.selected = []
        self.written = []
        self.utils = Mock()

    @contextmanager
    def get_utils(self):
        yield self.utils

    @contextmanager
    def get_connection(self):
        yield self

    def query(self, query):
        params = query[0]['FindImage']
        self.selected = [value for value in self.ids if value not in self.processed]
        if 'limit' in params:
            self.selected = self.selected[:params['limit']]
        return [{'FindImage': {'status': 0, 'entities': [
            {'_uniqueid': value} for value in self.selected]}}], []

    def execute_query(self, query):
        if self.fail_write:
            return 1, [], []
        value = query[0]['FindImage']['constraints'][2]
        self.processed.add(value)
        self.written.append(value)
        return 0, [], []


class Dataset:
    def __init__(self, client, query, **kwargs):
        self.query = deepcopy(query)
        self.ids = client.selected[:]

    def __len__(self):
        return len(self.ids)


class Loader:
    def __init__(self, dataset, **kwargs):
        self.dataset = dataset

    def __len__(self):
        return len(self.dataset)

    def __iter__(self):
        for value in self.dataset.ids:
            yield np.zeros((1, 2, 2, 3), dtype=np.uint8), [value]


class DetectionLifecycleTests(unittest.TestCase):
    def run_app(self, pool, infer_error=False, limit=0):
        self.workers = []
        self.datasets = []
        native_thread = threading.Thread

        def thread(*args, **kwargs):
            worker = native_thread(*args, **kwargs)
            self.workers.append(worker)
            return worker

        def dataset(*args, **kwargs):
            result = Dataset(*args, **kwargs)
            self.datasets.append(result)
            return result

        detector = Mock(classes=[])
        detections = {'boxes': [], 'scores': [], 'labels': []}
        detector.infer.side_effect = (
            [detections, ValueError('inference failed')] if infer_error else
            lambda image: detections)
        params = SimpleNamespace(model_name='model', clean=False,
                                 max_retrieved=limit, confidence_threshold=0.7)
        with patch.object(app, 'ConnectionPool', return_value=pool), \
                patch.object(app, 'BboxDetector', return_value=detector), \
                patch.object(app.PyTorchDataset, 'ApertureDBDataset', side_effect=dataset), \
                patch.object(app, 'DataLoader', Loader), \
                patch.object(app.threading, 'Thread', side_effect=thread), \
                redirect_stdout(io.StringIO()):
            try:
                app.main(params)
            finally:
                self.assertTrue(all(not worker.is_alive() for worker in self.workers))

    def test_candidate_ids_stay_fixed_while_progress_changes_and_limit_is_preserved(self):
        pool = Pool(['already', 'one', 'two', 'three'], processed=['already'])
        self.run_app(pool, limit=2)
        self.assertEqual(pool.selected, ['one', 'two'])
        self.assertCountEqual(pool.written, ['one', 'two'])
        self.assertEqual(self.datasets[0].query[0]['FindImage']['constraints'],
                         app.predicate('_uniqueid', 'in', ['one', 'two']))
        self.assertEqual(len(self.workers), 4)

    def test_inference_error_stops_and_joins_all_push_workers(self):
        pool = Pool(['one', 'two', 'three'])
        with self.assertRaisesRegex(ValueError, 'inference failed'):
            self.run_app(pool, infer_error=True)
        self.assertEqual(len(self.workers), 4)
        self.assertEqual(pool.written, ['one'])

    def test_write_error_reaches_caller_after_workers_are_joined(self):
        with self.assertRaisesRegex(RuntimeError, 'Failed to push object detections') as caught:
            self.run_app(Pool(['one', 'two'], fail_write=True))
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)
        self.assertEqual(len(self.workers), 4)

    def test_already_processed_selection_does_not_start_model_or_workers(self):
        self.run_app(Pool(['done'], processed=['done']))
        self.assertEqual(self.workers, [])
        self.assertEqual(self.datasets, [])


if __name__ == '__main__':
    unittest.main()
