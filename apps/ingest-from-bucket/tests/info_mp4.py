from aperturedb.CommonLibrary import create_connector

db = create_connector()

query = [{
    "FindVideo": {
        "constraints": {
            "adb_mime_type": ["==","video/mp4"]
        },
        "results": {
            "all_properties":True 
        }
    }
}]

def create_filter(n,i):
    run_query = [{
        "FindEntity": {
            "with_class": "WorkflowSpec",
            "_ref":1,
            "constraints" : { \
                    "workflow_name": ["==",n], \
                    "workflow_id": ["==",i], \
            } \
        }
    }]
    return run_query


results,_ = db.query(query)
if isinstance(results,list):
    if results[0]['FindVideo']['returned'] == 0:
        print("No Videos")
        sys.exit(1)
    pdfs = {}
    print("Videos:")
    for e in results[0]['FindVideo']['entities']:
        print(e)
else:
    print(f"Query Failed: {results}")
