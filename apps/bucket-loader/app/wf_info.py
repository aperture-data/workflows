from aperturedb.CommonLibrary import create_connector

db = create_connector()

spec_query = [{
    "FindEntity": {
        "with_class": "WorkflowSpec",
        "_ref":1,
        "results": {
            "list": ["workflow_name", "workflow_id", "workflow_create_date", "workflow_end_date"]
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

run_end_query = [{
    "FindEntity": {
        "with_class": "WorkflowRun",
        "is_connected_to": {
            "ref":1
        },
        "results": {
            "list": ["workflow_id","workflow_create_date","workflow_end_date"]
        }
    }
}]

results,_ = db.query(spec_query)
if isinstance(results,list):
    def optional( odict, key ):
        return odict[key] if key in odict else None
    if results[0]['FindEntity']['returned'] == 0:
        print("No Workflows")
        sys.exit(1)
    wf = {}
    for e in results[0]['FindEntity']['entities']:
        name = e['workflow_name']
        wid = e['workflow_id']
        start = e['workflow_create_date']
        end = optional( e,'workflow_end_date')
        if not name in wf:
            wf[name] = {}
        wf[name][wid] = { "started" : start, "ended": end } 
        rq = create_filter( name, wid )
        run_query = rq + run_end_query
        run_results,_ = db.query(run_query)
        if run_results[1]['FindEntity']['returned'] == 0:
            continue
        wf[name][wid]['runs'] = {}
        for rune in run_results[1]['FindEntity']['entities']:
            rid = rune['workflow_id']
            wstart = rune['workflow_create_date']
            wend = optional( rune, 'workflow_end_date' )
            wf[name][wid]['runs'][rid] = {'started': wstart , 'ended': wend }




    for w in wf.keys():
        print(f"* workflow = {w}")
        for r,rv in wf[w].items():
            print(f"  * instance = {r}") 
            print(f"   - created {rv['started']}") 
            if rv['ended']: 
                print(f"   - ended {rv['ended']}") 
            if "runs" in wf[w][r]:
                for (rr,rk) in wf[w][r]['runs'].items():
                    print(f"   * run = {rr}") 
                    print(f"    - created {rk['started']}") 
                    if rk['ended']: 
                        print(f"    - ended {rk['ended']}") 


