from aperturedb.CommonLibrary import create_connector
from aperturedb.Query import ObjectType 
import sys

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
            "list": ["workflow_id", "workflow_create_date", "workflow_end_date", "wf_linked_types"]
        }
    }
}]

def create_linked(wf_id,ent,wclass=None ):
    run_linked_query = [{
        f"Find{ent}" : {
            "constraints": {
                "wf_workflow_id": [ "==", wf_id ]
            },
            "results": {
                "count":True
            }
        }
    }]
    if wclass:
        run_linked_query[0]["FindEntity"]["with_class"] = wclass
    return run_linked_query

results,_ = db.query(spec_query)

known = [ t.value for t in ObjectType ]
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
            wlt = optional( rune, 'wf_linked_types' )
            run_info = {'started': wstart , 'ended': wend ,\
                    'types':wlt  }
            if wlt is not None:
                run_info['per_type'] = {}
                for t in wlt:
                    if t in known:
                        lq=  create_linked(rid,t[1:])
                        find = f"Find{t[1:]}"
                    else:
                        lq = create_linked(rid,"Entity", t )
                        find = f"FindEntity" 
                    linked_res,_ = db.query(lq)
                    run_info['per_type'][t] = linked_res[0][find]["count"]
            wf[name][wid]['runs'][rid] = run_info 









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
                    if rk['types']: 
                        print(f"    - types {rk['types']}") 
                    if 'per_type' in rk:
                        for t in rk['per_type']:
                            print(f"     * {t} =  {rk['per_type'][t]}") 


