# override of extensions.py that goes in the io_storages/aperturedb folder.
from dataclasses import dataclass
import os

import logging
logger = logging.getLogger(__name__)
@dataclass
class extension_iface():
    ref:int
    object_ref:int
    object_id:int
    run_ref:int = None

workflow_name = os.environ.get("WORKFLOW_NAME")
spec_id = os.environ.get("WORKFLOW_SPEC_ID")
run_id = os.environ.get("WORKFLOW_RUN_ID")


# return a list of aperturedb commands to add to the query when saving an annotation
def append_to_save_annotation( ctx:extension_iface ):
    logger.error(f"A2SA {ctx}")
    last_ref = ctx.ref
    ref = last_ref + 1
    annotation_key = f"{run_id}_ann_{ctx.object_id}"
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
                    "wf_creator_key": annotation_key
                },
                "if_not_found": {
                    "wf_creator_key": ["==",annotation_key]
                }
            }
    }]
    ctx.run_ref = ref+1
    return query
def append_to_save_bbox( ctx:extension_iface ):
    logger.error(f"A2SB {ctx}")
    bbox_id = "{run_id}_bbox_{ctx.object_id}"
    query = [{
        "AddConnection": {
            "src":ctx.run_ref,
            "dst":ctx.object_ref,
            "class":"WorkflowAdded",
            "properties": {
                "wf_creator_key": bbox_id
            },
            "if_not_found": {
                "wf_creator_key": ["==",bbox_id]
            }
        }
    }]

    return query
