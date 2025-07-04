#!/usr/bin/python3
# spec.py
import datetime as dt
from aperturedb.Query import ObjectType
from typing import Iterator, Optional, Tuple
from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import execute_query

class WorkflowSpec:
    def get_all_existing_entity_types(self):
        u = Utils(self.db)
        schema = u.get_schema()
        print(schema)
        return  [] if schema['entities'] is None else schema["entities"]["classes"].keys()

    @staticmethod
    def entity_filter(item):
        return item not in [ "WorkflowSpec", "WorkflowItem" ]
    def __init__(self,db,workflow_name,spec_id, clean=False):
        self.db = db
        self.spec_id = spec_id
        self.deletable_types = list(filter(self.entity_filter, self.get_all_existing_entity_types()))
        self.workflow_name = workflow_name
    
        
        response,_ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "constraints": {
                        "workflow_id": ["==", self.spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_create_date" ]
                    }
                }
            }]
        )
        if "returned" not in response[0]["FindEntity"]:
            raise Exception("Error Initializing WorkflowSpec")
        if response[0]["FindEntity"]["returned"] != 0:
            ent = response[0]["FindEntity"]["entities"][0]
            if not clean:
                raise Exception(f"Unable to initialize WorkflowSpec; existing spec (created at {ent['workflow_create_date']})")
            else:
                self.clean()
        response,_ = self.execute_query([
            {
                "AddEntity": {
                    "class": "WorkflowSpec",
                    "properties": {
                        "workflow_id": self.spec_id,
                        "workflow_name":self.workflow_name,
                        "workflow_create_date": dt.datetime.now().isoformat()
                    }
                }
            }]
        )
        add_res = response[0]["AddEntity"]
        if "status" not in add_res: 
            raise Exception("Error Initializing WorkflowSpec, failed to add spec")
        if  add_res["status"] != 0: 
            raise Exception("Error Initializing WorkflowSpec, adding spec did not return ok") 


    def execute_query(self,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        """Execute a query on ApertureDB and return the results

        TODO: Support mock
        """
        status, results, result_blobs = execute_query(
            client=self.db,
            query=query,
            blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
        )
        return results, result_blobs

    def clean(self):
        res,_ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", self.spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_create_date" ]
                    }
                }
            },{ 
                "FindEntity": { 
                    "with_class": "WorkflowRun",
                    "_ref":2,
                    "is_connected_to": {
                        "ref":1
                    }
                }
            },{ 
                "DeleteEntity": { 
                    "ref":2
                }
            }]
        )
        res,_ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", self.spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_create_date" ]
                    }
                }
            },{ 
                "DeleteEntity": { 
                    "ref":1
                }
            }]
        )
    def delete_run_id_data(self,run_id):
        pass
    def delete_spec_data(self,spec_id):
        known_objects = [m.value for m in ObjectType]

        base = [
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_id" ]
                    }
                }
            },{
                "FindEntity": {
                    "with_class": "WorkflowRun",
                    "_ref":2,
                    "is_connected_to": {
                        "ref":1
                    }
                }
            }]
        for type_to_delete in self.deletable_types:
            dtype = "Entity"
            if type_to_delete.startswith("_"):
                if type_to_delete in known_objects:
                    dtype = type_to_delete[1:]
            query =  base + [ {
                f"Find{dtype}": {
                    "ref":3,
                    "is_connected_to": {
                        "ref":2
                    }
                }
            }, {
                f"Delete{dype}": {
                    "ref":3
                }
            }]
            res,_ = self.execute_query(query) 

    @staticmethod
    def delete_spec( wf_name, spec_id ):
        self.delete_spec_data(wf_name,spec_id)
    @staticmethod
    def delete_all_data(workflow_name):
        # find specs of this type
        res,_ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_name": ["==",workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_id" ]
                    }
                }
            }]
        )
        specs = res[0]["FindEntity"]["entities"]
        for spec in specs:
            this.delete_spec_data( spec['workflow_id'] )

    def get_spec_find_query(self):
        return [{
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", self.spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "results": {
                        "list": [ "workflow_id" ]
                    }
                }
            }]

    def add_run(self, run_id ):
        res,_ = self.execute_query( self.get_spec_find_query() + 
            [{
                "AddEntity": {
                    "class": "WorkflowRun",
                    "properties": {
                        "workflow_id": run_id,
                        "workflow_create_date": dt.datetime.now().isoformat()
                    },
                    "connect": {
                        "ref":1,
                        "direction":"in",
                        "class":"WorkflowCreated"
                    }
                }
            }]

        )
    def finish_run(self,run_id):
        res,_ = self.execute_query( self.get_spec_find_query() + 
            [{
                "UpdateEntity": {
                    "with_class": "WorkflowRun",
                    "constraints": {
                        "workflow_id": ["==", run_id]
                        },
                    "properties": {
                        "workflow_end_date": dt.datetime.now().isoformat()
                    }
                }
            }]
        )
    def finish_spec(self):
        res,_ = self.execute_query( 
            [{
                "UpdateEntity": {
                    "with_class": "WorkflowSpec",
                    "constraints": {
                        "workflow_id": ["==", self.spec_id],
                        "workflow_name": ["==",self.workflow_name]
                    },
                    "properties": {
                        "workflow_end_date": dt.datetime.now().isoformat()
                    }
                }
            }]
        )
        
