#!/usr/bin/python3
# spec.py
import datetime as dt
from aperturedb.Query import ObjectType
from typing import Iterator, Optional, Tuple
from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector
import logging
import utils

logger = logging.getLogger(__name__)

class WorkflowSpec:
    @staticmethod
    def get_all_existing_entity_types(db):
        u = Utils(db)
        schema = u.get_schema()
        return  [] if schema['entities'] is None else schema["entities"]["classes"].keys()

    @staticmethod
    def entity_filter(item):
        return item not in [ "WorkflowSpec", "WorkflowItem" ]

    def __init__(self,db:Connector, workflow_name:str, spec_id:str, clean:bool=False):
        self.db = db
        self.spec_id = str(spec_id)
        self.deletable_types = list(filter(self.entity_filter, self.get_all_existing_entity_types(self.db)))
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
        logger.info(f"Create Workflow Spec {self.spec_id}")


    def execute_query(self,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        return  self.execute_query_with_db(
            client=self.db,
            query=query,
            blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
        )

    @staticmethod
    def execute_query_with_db(client,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        status, results, result_blobs = execute_query(
            client=client,
            query=query,
            blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
        )
        return results, result_blobs

    def clean(self):
        self.clean_spec(self.db,self.workflow_name,self.spec_id)

    @classmethod
    def clean_spec(cls,db,workflow_name,spec_id):
        logger.info(f"Cleaning spec {spec_id}")
        res,_ = cls.execute_query_with_db(db,[
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", spec_id],
                        "workflow_name": ["==",workflow_name]
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
        if not isinstance(res,list):
            raise Exception(f"Failed to execute clean query for WorkflowRun {res}")
        logger.info(f"Removed {res[2]['DeleteEntity']['count']} WorkflowRun")
        res,_ = cls.execute_query_with_db(db,[
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", spec_id],
                        "workflow_name": ["==",workflow_name]
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

        if not isinstance(res,list):
            raise Exception(f"Failed to execute clean query for WorkflowSpec {res}")
        logger.info(f"Removed {res[1]['DeleteEntity']['count']} WorkflowSpec")
        print(f"Workflow {spec_id} removed")

    @classmethod
    def delete_spec_data(cls,db,workflow_name,spec_id):
        logger.info(f"Deleting data for {spec_id}")
        known_objects = [m.value for m in ObjectType]

        base = [
            {
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":1,
                    "constraints": {
                        "workflow_id": ["==", spec_id],
                        "workflow_name": ["==",workflow_name]
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
                    },
                    "results": {
                        "count" : True
                    }
                }
            }]
        res,_ = cls.execute_query_with_db(db,base) 
        if res[0]["FindEntity"]["returned"] == 0 :
            logger.warn("No spec {spec_id} found.")
            return
        if res[1]["FindEntity"]["count"] == 0 :
            logger.warn("No runs assocated spec {spec_id} found.")
        else:
            deletable_types = list(filter(cls.entity_filter, cls.get_all_existing_entity_types(db)))
            for type_to_delete in deletable_types:
                dtype = "Entity"
                if type_to_delete.startswith("_"):
                    if type_to_delete in known_objects:
                        dtype = type_to_delete[1:]
                query =  base + [ {
                    f"Find{dtype}": {
                        "_ref":3,
                        "is_connected_to": {
                            "ref":2
                        }
                    }
                }, {
                    f"Delete{dtype}": {
                        "ref":3
                    }
                }]
                res,_ = cls.execute_query_with_db(db,query) 
                if not isinstance(res,list):
                    raise Exception(f"Failed deleting {dtype} for workflow {spec_id}")
        cls.clean_spec(db, workflow_name, spec_id )

    @classmethod
    def delete_spec(cls,db, wf_name, spec_id ):
        cls.delete_spec_data(db,wf_name,spec_id)

    @classmethod
    def delete_all_data(cls,db,workflow_name):
        # find specs of this type
        res,_ = cls.execute_query_with_db(db,[
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
            cls.delete_spec_data(db, workflow_name, spec['workflow_id'] )

    @classmethod
    def delete_all_creator_key(cls,db,creator_key):
        logger.info(f"Cleaning database from creator key {creator_key}")
        known_objects = [m.value for m in ObjectType]
        for type_to_clean in known_objects:
            otype = type_to_clean[1:] 
            res,_ = cls.execute_query_with_db(db,[
                {
                    f"Find{otype}": {
                        "_ref":1,
                        "constraints": {
                            "wf_creator_key": ["==", creator_key ]
                        },
                        "results": {
                            "count":True
                        }
                    }
                },{
                    f"Delete{otype}": {
                        "ref":1
                    }
                }]
            )
            if isinstance(res,list):
                deleted = res[1][f"Delete{otype}"]["count"]
                if otype == "Entity":
                    noun = "Entities" if deleted != 1 else otype
                else:
                    noun = otype+"s" if deleted != 1 else otype
                print(f"Deleted {deleted} {noun} matching creator_key {creator_key}")
            else:
                raise Exception(f"Failed bucket clean delete query for type {otype} on resource {provider}/{bucket} : {res}")

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

    def get_run_find_query(self,run_id):
        specq =  self.get_spec_find_query() 
        del specq[0]["FindEntity"]["results"]
        return specq + [
            { 
                "FindEntity": { 
                    "with_class": "WorkflowRun",
                    "_ref":2,
                    "constraints": {
                        "workflow_id" : ["==",run_id]
                    },
                    "is_connected_to": {
                        "ref":1
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
        add_res = res[1]["AddEntity"]
        if "status" not in add_res: 
            raise Exception("Error Initializing WorkflowRun, failed to add run")
        logger.info(f"Create Workflow Run {run_id}")
    def link_objects(self,run_id,object_type):
        is_entity = False
        otype = object_type
        known_objects = [m.value for m in ObjectType] + ["_Document"]
        if object_type in known_objects:
            otype = object_type[1:]
            if object_type == "_Document":
                otype = "Blob"
        else:
            is_entity = True
            otype = "Entity"
        runq =  self.get_run_find_query(run_id) 
        linkq = runq + \
                [{
                    f"Find{otype}": {
                        "_ref":3,
                        "constraints": {
                            "wf_workflow_id": ["==",run_id]
                        },
                        "results": {
                            "list": ["wf_sha1_hash"],
                            "count":True
                        }
                    }
                },{
                    "AddConnection" : {
                        "src":2,
                        "dst":3,
                        "class":"WorkflowAdded"
                    }

                    }]
        if is_entity:
            linkq[2]["FindEntity"]["with_class"] = object_type
        res,_ = self.execute_query(linkq)
        count = res[2][f"Find{otype}"]["count"]
        logger.info(f"Linked {count} {otype}") 

    def finish_run(self,run_id, extra_props = {}):
        props = extra_props
        props["workflow_end_date"] =  dt.datetime.now().isoformat()
        res,_ = self.execute_query( self.get_spec_find_query() + 
            [{
                "UpdateEntity": {
                    "with_class": "WorkflowRun",
                    "constraints": {
                        "workflow_id": ["==", run_id]
                        },
                    "properties": props
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
        
