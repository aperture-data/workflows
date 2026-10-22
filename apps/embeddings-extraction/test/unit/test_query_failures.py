"""Count-query failures must not terminate the workflow successfully."""
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


APP = Path('/app')
DEPENDENCIES = {
    'aperturedb': SimpleNamespace(QueryGenerator=SimpleNamespace(QueryGenerator=object)),
    'connection_pool': Mock(),
    'cv2': Mock(),
    'numpy': Mock(),
    'PIL': Mock(),
    'text_extraction.text_extractor': Mock(),
    'text_extraction.segmentation': Mock(),
    'text_extraction.schema': SimpleNamespace(TextBlock=object, Segment=object),
}


def generators():
    for name, command, class_name in [
        ('images', 'FindImage', 'FindImageQueryGenerator'),
        ('pdfs', 'FindBlob', 'FindPDFQueryGenerator'),
        ('videos', 'FindVideo', 'FindVideoQueryGenerator'),
    ]:
        spec = importlib.util.spec_from_file_location(name, APP / f'{name}.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, DEPENDENCIES):
            spec.loader.exec_module(module)
        yield command, getattr(module, class_name)


class QueryFailureTests(unittest.TestCase):
    def test_server_errors_preserve_diagnostic(self):
        for command, generator in generators():
            with self.subTest(command=command):
                pool = Mock()
                pool.execute_query.return_value = (
                    1, {'status': -1, 'info': '_mime_type is not a valid constraints key'}, [])
                with self.assertRaisesRegex(RuntimeError, '_mime_type is not a valid'):
                    generator(pool, Mock(), 'done')

    def test_failed_status_is_not_treated_as_a_count(self):
        for command, generator in generators():
            with self.subTest(command=command):
                pool = Mock()
                pool.execute_query.return_value = (1, [{command: {'count': 0}}], [])
                with self.assertRaises(RuntimeError):
                    generator(pool, Mock(), 'done')

    def test_malformed_responses_fail(self):
        for command, generator in generators():
            for response in ([], None, [{}], [{command: {}}]):
                with self.subTest(command=command, response=response):
                    pool = Mock()
                    pool.execute_query.return_value = (0, response, [])
                    with self.assertRaises(RuntimeError):
                        generator(pool, Mock(), 'done')

    def test_successful_counts_including_empty_results(self):
        for command, generator in generators():
            for count in (0, 1):
                with self.subTest(command=command, count=count):
                    pool = Mock()
                    pool.execute_query.return_value = (0, [{command: {'count': count}}], [])
                    self.assertEqual(len(generator(pool, Mock(), 'done')), count)

    def test_connection_failures_propagate(self):
        for command, generator in generators():
            with self.subTest(command=command):
                pool = Mock()
                failure = ConnectionError('connection lost')
                pool.execute_query.side_effect = failure
                with self.assertRaises(ConnectionError) as caught:
                    generator(pool, Mock(), 'done')
                self.assertIs(caught.exception, failure)

    def test_pdf_count_and_pages_use_the_same_completion_marker(self):
        generator = next(factory for command, factory in generators()
                         if command == 'FindBlob')
        pool = Mock()
        pool.execute_query.return_value = (0, [{'FindBlob': {'count': 1}}], [])
        query_generator = generator(pool, Mock(), 'custom_done')
        count_query = pool.execute_query.call_args.args[0]
        page_query, _ = query_generator.getitem(0)
        constraints = page_query[0]['FindBlob']['constraints']
        self.assertEqual(count_query[0]['FindBlob']['constraints'], constraints)
        self.assertIn(['$custom_done', '!=', True], constraints)
        self.assertIn(['$_mime_type', '==', 'application/pdf'], constraints)
