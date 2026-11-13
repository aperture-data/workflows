from aperturedb.Constraints import predicate
from aperturedb.CommonLibrary import create_connector
import sys

db = create_connector()

query = [{'FindBlob': {'constraints': predicate(
    '_mime_type', '==', 'application/pdf'), 'results': {'all_properties': True}}}]


def create_filter(n, i):
    run_query = [{'FindEntity': {'with_class': 'WorkflowSpec', '_ref': 1, 'constraints': [
        'all', predicate('workflow_name', '==', n), predicate('workflow_id', '==', i)]}}]
    return run_query


results, _ = db.query(query)
if isinstance(results, list):
    if results[0]['FindBlob']['returned'] == 0:
        print("No PDFs")
        sys.exit(1)
    pdfs = {}
    print("PDFS:")
    for e in results[0]['FindBlob']['entities']:
        print(e)
else:
    print(f"Query Failed: {results}")
