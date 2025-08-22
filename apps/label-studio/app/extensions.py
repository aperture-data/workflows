# override of extensions.py that goes in the io_storages/aperturedb folder.
# Adds workflow specific data to objects created by label studio

from dataclasses import dataclass
import os

import logging
logger = logging.getLogger(__name__)
@dataclass
class extension_iface():
    ref:int
    object_ref:int
    object_id:str
    run_ref:int = None

workflow_name = os.environ.get("WORKFLOW_NAME")
spec_id = os.environ.get("WORKFLOW_SPEC_ID")
run_id = os.environ.get("WORKFLOW_RUN_ID")

import hashlib

def hash_string(string):
    return hashlib.sha1(string.encode('utf-8')).hexdigest()

def modify_annotation_add_props( annotation_props:object,  ctx:extension_iface ):
    annotation_props["workflow_id"] = run_id
    annotation_props["wf_creator"] = workflow_name
    # wf_creator_key - not sure this is useful, what configuration would we want
    #   to remove that is beyond scope of run?
    # wf_sha1_hash - don't need, this is always created as linked to an object.
    return annotation_props

def modify_bbox_add_props( bbox_props:object,  ctx:extension_iface ):
    bbox_props["workflow_id"] = run_id
    bbox_props["wf_creator"] = workflow_name
    return bbox_props

# return a list of aperturedb commands to add to the query when saving an annotation
def append_to_save_annotation( ctx:extension_iface ):
    logger.error(f"A2SA {ctx}")
    last_ref = ctx.ref
    ref = last_ref + 1
    # this means if a different run modifies an annotation it will also link to
    # it.
    annotation_key = hash_string( f"{run_id}_ann_{ctx.object_id}" )
    logger.error(f"A2SA NAME {workflow_name} SPEC {spec_id} RUN {run_id} key {annotation_key}")
    query = [{
                "FindEntity": {
                    "with_class": "WorkflowSpec",
                    "_ref":ref,
                    "constraints": {
                        "workflow_id": ["==", spec_id],
                        "workflow_name": ["==",workflow_name]
                    }
                }
            },{
                "FindEntity": { 
                    "with_class": "WorkflowRun",
                    "_ref":ref+1,
                    "constraints": {
                        "workflow_id" : ["==",run_id]
                    },
                    "is_connected_to": {
                        "ref":ref
                    }
                }
            },{
            "AddConnection": {
                "src":ref+1,
                "dst":ctx.object_ref,
                "class":"WorkflowAdded",
                "properties": {
                    "wf_sha1_hash": annotation_key,
                    "wf_creator": workflow_name,
                    "wf_workflow_id": run_id
                },
                "if_not_found": {
                    "wf_sha1_hash": ["==",annotation_key]
                }
            }
    }]
    ctx.run_ref = ref+1
    return query

def append_to_save_bbox( ctx:extension_iface ):
    logger.error(f"A2SB {ctx}")
    bbox_key = hash_string( f"{run_id}_bbox_{ctx.object_id}" )
    query = [{
        "AddConnection": {
            "src":ctx.run_ref,
            "dst":ctx.object_ref,
            "class":"WorkflowAdded",
            "properties": {
                "wf_sha1_hash": bbox_key,
                "wf_creator": workflow_name,
                "wf_workflow_id": run_id
            },
            "if_not_found": {
                "wf_sha1_hash": ["==",bbox_key]
            }
        }
    }]

    return query
