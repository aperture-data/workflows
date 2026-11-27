"""OCR callback transports must not survive long CPU phases or hide write errors."""
from contextlib import contextmanager, redirect_stdout
import importlib.util
import io
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from aperturedb.Constraints import predicate
from aperturedb.Connector import Connector
from aperturedb.Configuration import Configuration
from aperturedb_workflows import connection_pool as shared_pool
from fresh_connection_pool import FreshConnectionPool


APP = Path(os.getenv('WORKFLOW_APP', '/workflow-app'))


class FreshPoolTests(unittest.TestCase):
    def setUp(self):
        self.template = Mock()
        self.client = Mock()
        self.template.clone.return_value = self.client
        self.pool = FreshConnectionPool(pool_size=1,
                                       connection_factory=lambda: self.template)

    def test_each_checkout_uses_and_closes_a_new_transport_and_returns_capacity(self):
        second = Mock()
        self.template.clone.side_effect = [self.client, second]
        with self.pool.get_connection() as client:
            self.assertIs(client, self.client)
            self.assertEqual(self.pool.available(), 0)
        self.client.close.assert_called_once_with()
        with self.pool.get_connection() as client:
            self.assertIs(client, second)
        second.close.assert_called_once_with()
        self.assertEqual(self.pool.available(), 1)
        self.template.query.assert_not_called()

    def test_inherited_execute_uses_fresh_client_once_and_closes_on_uncertain_failure(self):
        failure = ConnectionError('request outcome is unknown')
        with patch.object(shared_pool, 'execute_query', side_effect=failure) as execute:
            with self.assertRaises(ConnectionError) as caught:
                self.pool.execute_query([{'UpdateImage': {}}], [b'vector'])
        self.assertIs(caught.exception, failure)
        execute.assert_called_once_with(self.client, [{'UpdateImage': {}}], [b'vector'])
        self.client.close.assert_called_once_with()
        self.assertEqual(self.pool.available(), 1)
        self.template.query.assert_not_called()

    def test_inherited_query_and_get_utils_both_use_the_fresh_checkout(self):
        self.client.query.return_value = ([], [])
        self.assertEqual(self.pool.query([{'GetStatus': {}}]), ([], []))
        self.client.query.assert_called_once_with([{'GetStatus': {}}], [])
        self.client.reset_mock()
        with patch.object(shared_pool, 'Utils', side_effect=lambda client: SimpleNamespace(client=client)):
            with self.pool.get_utils() as utils:
                self.assertIs(utils.client, self.client)
                self.assertEqual(self.pool.available(), 0)
        self.client.close.assert_called_once_with()
        self.assertEqual(self.pool.available(), 1)

    def test_failed_clone_does_not_lose_pool_capacity(self):
        self.template.clone.side_effect = RuntimeError('clone failed')
        with self.assertRaisesRegex(RuntimeError, 'clone failed'):
            with self.pool.get_connection():
                self.fail('Failed clone yielded a connection')
        self.assertEqual(self.pool.available(), 1)

    def test_closed_constructor_checkout_can_still_clone_its_effective_verified_config(self):
        config = Configuration(name='ocr-test', host='fixture.invalid', port=55551, username='fixture',
                               password='fixture', use_ssl=True, verify_hostname=True,
                               ca_cert='/ca/ca.crt')
        template = Connector(config=config)
        pool = FreshConnectionPool(pool_size=1, connection_factory=lambda: template)
        with pool.get_connection() as client:
            self.assertIs(client.config, config)
        clone = client.clone()
        try:
            self.assertIs(clone.config, config)
            self.assertTrue(clone.config.use_ssl)
            self.assertTrue(clone.config.verify_hostname)
            self.assertEqual(clone.config.ca_cert, '/ca/ca.crt')
        finally:
            clone.close()


class ErrorPropagationTests(unittest.TestCase):
    def load_generator(self, name, factory):
        dependencies = {
            'aperturedb_workflows.embeddings': SimpleNamespace(Embedder=object),
            'pytesseract': Mock(), 'torch': Mock(), 'numpy': Mock(),
            'PIL': SimpleNamespace(Image=Mock()), 'fitz': Mock(),
            'aperturedb_workflows.text_extraction.segmentation': SimpleNamespace(TextSegmenter=Mock()),
            'aperturedb_workflows.text_extraction.schema': SimpleNamespace(TextBlock=object, Segment=object),
        }
        spec = importlib.util.spec_from_file_location('ocr_' + name, APP / (name + '.py'))
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, dependencies):
            spec.loader.exec_module(module)
        pool = Mock()
        command = 'FindImage' if name == 'image_ocr' else 'FindBlob'
        pool.execute_query.return_value = (
            0, [{command: {'returned': 0, 'entities': []}}], [])
        return getattr(module, factory)(pool, Mock(context_length=77), 'done', Mock(), True)

    def test_image_and_pdf_callbacks_enable_the_existing_strict_sdk_policy(self):
        for name, factory in [('image_ocr', 'FindImageOCRQueryGenerator'),
                              ('pdf_ocr', 'FindPDFOCRQueryGenerator')]:
            with self.subTest(kind=name), redirect_stdout(io.StringIO()):
                generator = self.load_generator(name, factory)
                self.assertIs(generator.strict_response_validation, True)

    def test_joined_callback_errors_fail_each_requested_ocr_phase(self):
        for phase in ['Image', 'PDF']:
            with self.subTest(phase=phase):
                class Querier:
                    joined = False

                    def query(self, *args, **kwargs):
                        self.joined = True

                    @property
                    def error_counter(self):
                        if not self.joined:
                            raise AssertionError('Errors checked before worker join')
                        return 1

                querier = Querier()
                pool = Mock()
                pool.get_connection.return_value.__enter__ = Mock(return_value=Mock())
                pool.get_connection.return_value.__exit__ = Mock(return_value=False)
                dependencies = {
                    'aperturedb': SimpleNamespace(CommonLibrary=Mock(), ParallelQuery=SimpleNamespace(
                        ParallelQuery=lambda client: querier)),
                    'aperturedb.Constraints': SimpleNamespace(predicate=predicate),
                    'aperturedb_workflows.embeddings': SimpleNamespace(Embedder=Mock()),
                    'fresh_connection_pool': SimpleNamespace(FreshConnectionPool=lambda: pool),
                    'ocr': SimpleNamespace(OCR=Mock()),
                    'aperturedb_workflows.wf_argparse': SimpleNamespace(ArgumentParser=object),
                    'image_ocr': SimpleNamespace(FindImageOCRQueryGenerator=Mock()),
                    'pdf_ocr': SimpleNamespace(FindPDFOCRQueryGenerator=Mock()),
                }
                spec = importlib.util.spec_from_file_location('ocr_workflow_subject', APP / 'app.py')
                module = importlib.util.module_from_spec(spec)
                with patch.dict(sys.modules, dependencies):
                    spec.loader.exec_module(module)
                params = SimpleNamespace(log_level='CRITICAL', ocr_method='tesseract', clean=False,
                                         extract_image_text=phase == 'Image', extract_pdf_text=phase == 'PDF',
                                         model_name='ViT-B/16', generate_embeddings=True, numthreads=1)
                with redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, phase + ' OCR extraction failed'):
                    module.main(params)
                self.assertTrue(querier.joined)


if __name__ == '__main__':
    unittest.main()
