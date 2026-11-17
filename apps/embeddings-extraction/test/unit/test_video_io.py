"""Video extraction must not complete after failed inference or uncertain writes."""
from contextlib import contextmanager
import importlib.util
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from aperturedb.Constraints import predicate


APP = Path(os.getenv('WORKFLOW_APP', '/app'))


class Pool:
    def __init__(self):
        self.template = Mock()
        self.borrowed = False
        self.execute_query = Mock(return_value=(
            0, [{'FindVideo': {'returned': 1, 'entities': [{'_uniqueid': 'video'}]}}], []))

    @contextmanager
    def get_connection(self):
        if self.borrowed:
            raise AssertionError('Nested connection borrow')
        self.borrowed = True
        try:
            yield self.template
        finally:
            self.borrowed = False


class VideoIOTests(unittest.TestCase):
    def setUp(self):
        self.execute = Mock(return_value=(
            0, [{'FindVideo': {'returned': 1, 'entities': [{'_uniqueid': 'video'}]}}], []))
        dependencies = {
            'aperturedb': SimpleNamespace(QueryGenerator=SimpleNamespace(QueryGenerator=object)),
            'aperturedb.Constraints': SimpleNamespace(predicate=predicate),
            'aperturedb.CommonLibrary': SimpleNamespace(execute_query=self.execute),
        }
        modules = patch.dict(sys.modules, dependencies)
        modules.start()
        self.addCleanup(modules.stop)
        spec = importlib.util.spec_from_file_location('video_io_subject', APP / 'videos.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.pool = Pool()
        self.embedder = Mock(descriptor_set='VideoEmbeddings')
        self.generator = module.FindVideoQueryGenerator(
            self.pool, self.embedder, 'finished', sample_rate_fps=1)
        self.execute.reset_mock()
        self.pool.template.reset_mock()
        self.frames = [b'frame0', b'frame2', b'frame4', b'frame6']
        self.vectors = [SimpleNamespace(tobytes=lambda: b'vector') for _ in self.frames]
        self.embedder.embed_images.return_value = self.vectors
        self.response = [{'FindVideo': {'status': 0, 'returned': 1, 'entities': [
            {'_uniqueid': 'video', '_frame_count': 8, '_fps': 2}]}}]
        self.extracted = [{'FindVideo': {'status': 0}},
                          {'ExtractFrames': {'status': 0, 'returned': 4}}]

    def handle(self):
        return self.generator.response_handler([], [], self.response, [])

    def test_response_handler_uses_strict_sdk_validation(self):
        self.assertIs(self.generator.strict_response_validation, True)

    def test_failed_frame_extraction_never_embeds_or_attempts_completion(self):
        self.generator._execute_fresh = Mock(return_value=(
            1, [{'ExtractFrames': {'status': -1}}], []))
        with self.assertRaises(RuntimeError):
            self.handle()
        self.embedder.embed_images.assert_not_called()
        self.assertEqual(self.generator._execute_fresh.call_count, 1)

    def test_short_frame_response_never_embeds_or_attempts_completion(self):
        self.generator._execute_fresh = Mock(return_value=(
            0, self.extracted, self.frames[:-1]))
        with self.assertRaises(RuntimeError):
            self.handle()
        self.embedder.embed_images.assert_not_called()
        self.assertEqual(self.generator._execute_fresh.call_count, 1)

    def test_short_embedding_batch_never_attempts_completion(self):
        self.generator._execute_fresh = Mock(return_value=(0, self.extracted, self.frames))
        self.embedder.embed_images.return_value = self.vectors[:-1]
        with self.assertRaises(RuntimeError):
            self.handle()
        self.embedder.embed_images.assert_called_once_with(self.frames)
        self.assertEqual(self.generator._execute_fresh.call_count, 1)

    def test_model_exception_is_not_converted_into_empty_success(self):
        self.generator._execute_fresh = Mock(return_value=(0, self.extracted, self.frames))
        failure = ValueError('model failed')
        self.embedder.embed_images.side_effect = failure
        with self.assertRaises(Exception) as caught:
            self.handle()
        error = caught.exception
        while error is not None and error is not failure:
            error = error.__cause__
        self.assertIs(error, failure)
        self.embedder.embed_images.assert_called_once_with(self.frames)
        self.assertEqual(self.generator._execute_fresh.call_count, 1)

    def test_failed_write_reaches_caller_without_retry(self):
        self.generator._execute_fresh = Mock(side_effect=[
            (0, self.extracted, self.frames), (1, [{'status': -1}], [])])
        with self.assertRaises(RuntimeError):
            self.handle()
        self.assertEqual(self.generator._execute_fresh.call_count, 2)

    def test_uncertain_transport_closes_fresh_client_without_replaying_write(self):
        fresh = Mock()
        self.pool.template.clone.return_value = fresh
        failure = ConnectionError('request outcome is unknown')
        self.execute.side_effect = failure
        query = [{'UpdateVideo': {'properties': {'finished': True}}}]
        with self.assertRaises(ConnectionError) as caught:
            self.generator._execute_fresh(query, [b'vector'])
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.execute.call_count, 1)
        self.pool.template.clone.assert_called_once_with()
        fresh.close.assert_called_once_with()
        self.pool.template.query.assert_not_called()
        self.assertFalse(self.pool.borrowed)

    def test_fresh_connections_are_released_before_cpu_work_and_preserve_sampling(self):
        extraction_client, write_client = Mock(), Mock()
        self.pool.template.clone.side_effect = [extraction_client, write_client]
        self.execute.side_effect = [(0, self.extracted, self.frames), (0, [], [])]

        def embed(frames):
            self.assertFalse(self.pool.borrowed)
            extraction_client.close.assert_called_once_with()
            self.assertEqual(frames, self.frames)
            return self.vectors

        self.embedder.embed_images.side_effect = embed
        self.handle()
        self.assertEqual(self.execute.call_count, 2)
        self.assertIs(self.execute.call_args_list[0].args[0], extraction_client)
        self.assertIs(self.execute.call_args_list[1].args[0], write_client)
        extraction_client.close.assert_called_once_with()
        write_client.close.assert_called_once_with()
        self.assertFalse(self.pool.borrowed)
        extraction = self.execute.call_args_list[0].args[1]
        self.assertEqual(extraction[1]['ExtractFrames']['frame_spec'], [0, 2, 4, 6])
        write = self.execute.call_args_list[1].args[1]
        clips = [command['AddClip']['frame_number_range']
                 for command in write if 'AddClip' in command]
        self.assertEqual(clips, [{'start': 0, 'stop': 2}, {'start': 2, 'stop': 4},
                                 {'start': 4, 'stop': 6}, {'start': 6, 'stop': 8}])
        self.assertEqual(sum('AddDescriptor' in command for command in write), 4)
        self.assertEqual(write[-1]['UpdateVideo']['properties'], {'finished': True})
        self.assertEqual(self.execute.call_args_list[1].args[2], [b'vector'] * 4)

    def test_joined_video_worker_failure_makes_the_workflow_fail(self):
        class Querier:
            joined = False

            def query(self, *args, **kwargs):
                self.joined = True

            @property
            def error_counter(self):
                if not self.joined:
                    raise AssertionError('Checked errors before query joined')
                return 1

        querier = Querier()
        embedder_factory = Mock()
        embedder_factory.from_new_descriptor_set.return_value = self.embedder
        dependencies = {
            'aperturedb': SimpleNamespace(ParallelQuery=SimpleNamespace(
                ParallelQuery=lambda client: querier)),
            'clip': Mock(),
            'embeddings': SimpleNamespace(Embedder=embedder_factory),
            'connection_pool': SimpleNamespace(ConnectionPool=lambda: self.pool),
            'images': Mock(),
            'pdfs': Mock(),
            'videos': SimpleNamespace(FindVideoQueryGenerator=lambda *args, **kwargs: self.generator),
        }
        spec = importlib.util.spec_from_file_location(
            'video_workflow_subject', APP / 'extract_embeddings.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, dependencies):
            spec.loader.exec_module(module)
        params = SimpleNamespace(log_level='ERROR', clean=False,
                                 extract_images=False, extract_pdfs=False,
                                 extract_videos=True, model_name='ViT-B/16',
                                 sample_rate_fps=1, numthreads=1)
        with self.assertRaisesRegex(RuntimeError, 'failed'):
            module.main(params)
        self.assertTrue(querier.joined)


if __name__ == '__main__':
    unittest.main()
