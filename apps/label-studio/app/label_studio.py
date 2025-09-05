#label_studio.py - ApertureData's Label Studio workflow
import logging
import sys
import os
from uuid import uuid4


from aperturedb.CommonLibrary import create_connector 
from wf_argparse import ArgumentParser
from status import Status
from status_tools import WorkFlowError,StatusUpdater
from enum import Enum as PyEnum
from prometheus_client import Enum

from spec import WorkflowSpec


import subprocess
import re
import json


logger = logging.getLogger(__name__)

class LabelStudioPhase(PyEnum):
    STARTING = "starting"
    INITIALIZING = "initializing" 
    PROCESSING = "processing"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


def main(args):
    updater = StatusUpdater()
    updater.post_update(completed=10, phase="initializing",
            phases=["initializing","processing","serving","finished"],
            status=LabelStudioPhase.STARTING) 

    db = create_connector()

    def set_state(phase,completeness=None):
        if completeness is not None:
            updater.post_update(phase=phase,completed=completeness)
        else:
            updater.post_update(phase=phase)

    if args.delete:
        set_state(LabelStudioPhase.PROCESSING,completeness=0)
        WorkflowSpec.delete_spec(db, "label-studio", args.spec_id )
        set_state(LabelStudioPhase.FINISHED,completeness=100)
        sys.exit(0)
    elif args.delete_all:
        set_state(LabelStudioPhase.PROCESSING,completeness=0)
        WorkflowSpec.delete_all_data( db, "label-studio") 
        set_state(LabelStudioPhase.FINISHED,completeness=100)
        sys.exit(0)
    elif args.delete_all_ls_data:
        # note we don't verify host/database - a host could not be available,
        # but data could have been loaded from it.
        set_state(LabelStudioPhase.PROCESSING,completeness=0)
        WorkflowSpec.delete_all_creator( "label-studio" )
        set_state(LabelStudioPhase.FINISHED,completeness=100)
        sys.exit(0)



    def add_common_vars( env ):

        env["LABEL_STUDIO_DEBUG"]="FALSE" 
        env["LABEL_STUDIO_APERTUREDB_KEY"]=db.config.deflate()
        env["LABEL_STUDIO_LOG_CONFIG_YAML"]="/app/workflows_logging.yaml"
        full_path = None
        if "DB_HOST_PUBLIC" in os.environ:
            # generate path for cloud
            full_path = "https://{}/labelstudio".format(os.environ['DB_HOST_PUBLIC'])
            logger.info(f"Set url from DB_HOST_PUBLIC: {full_path}")
        if args.label_studio_url_path is not None:
            if full_path is not None:
                logger.warning("Overriding full path with explicit workflow option")
            full_path = args.label_studio_url_path
            logger.info(f"Set url from workflow argument: {full_path}")

        if full_path:
            m = re.match("(https?)://([^/]*)(.*)",full_path)
            if m is None:
                raise Exception(f"Bad format for url path: {full_path}")

            proto,host,subpath = m.groups()
            # if host is farm\d+.*.*.aperturedata it's a long cloud name
            # we want to make it farm\d+.cloud.aperturedata
            if re.match(".*(farm\d+)\.[^.]*\.[^.]*\.aperturedata",host ):
                oldhost = host
                host = re.sub("(farm\d+)\.[^.]*\.[^.]*","\\1.cloud",host)
                logger.debug(f"Changed {oldhost} to {host}")
                full_path = "{}://{}{}".format(proto,host,subpath)
            logger.debug(f"Subpath = {subpath}") 

            # strip trailing /
            if subpath[-1:] == '/':
                logger.debug("Subpath had trailing slash, stripped.") 
                subpath = sub_path[:-1]

            logger.debug(f"Path is {full_path} and {subpath}")
            
            env["LABEL_STUDIO_HOST"] = full_path
            env["LABEL_STUDIO_STATIC_PATH"] = "{}/static/".format( subpath )
            env["LABEL_STUDIO_URL_BASE"] = "{}".format( subpath )
        if args.label_studio_default_storage_limit is not None:
            env["LABEL_STUDIO_APERTUREDB_DEFAULT_LIMIT"] = str(args.label_studio_default_storage_limit)
        env["LABEL_STUDIO_APERTUREDB_RO_PREDS"] = str(args.label_studio_storage_annotations_ro)
        env["LABEL_STUDIO_APERTUREDB_DEFAULT_LOAD_PREDS"] = str(args.label_studio_default_import_annotations)


    logger.info("Preparing for Label Studio configuration.")
    # do config for LS
    cfg_env = os.environ.copy()
    add_common_vars(cfg_env)
    cfg_env["LABEL_STUDIO_USERNAME"]=args.label_studio_user
    cfg_env["LABEL_STUDIO_PASSWORD"]=args.label_studio_password
    if args.label_studio_token:
        cfg_env["LABEL_STUDIO_USER_TOKEN"]=args.label_studio_token
    if args.label_studio_default_project_name != '':
        cfg_env["LABEL_STUDIO_CREATE_PROJ_TITLE"] = args.label_studio_default_project_name
    if args.label_studio_default_storage_name != '':
        cfg_env["LABEL_STUDIO_DEFAULT_CLOUD_STORAGE"]="aperturedb"
        with open( "/app/cloud.json" ,"w") as fp:
            storage_config = {
                    'title': args.label_studio_default_storage_name
                    }
            json.dump(storage_config,fp)
        cfg_env["LABEL_STUDIO_CLOUD_STORAGE_JSON_PATH"]="/app/cloud.json"
    spec = WorkflowSpec( db, "label-studio", args.spec_id, clean=args.clean )
    set_state(LabelStudioPhase.INITIALIZING,completeness=25)

    ret = subprocess.run("bash /app/label_studio_init.sh", shell=True, env=cfg_env)
    set_state(LabelStudioPhase.INITIALIZING,completeness=70)
    run_id = uuid4()
    spec.add_run( run_id )

    if ret.returncode != 0:
        logger.error("Label Studio configuration failed.")
        set_state(LabelStudioPhase.FAILED,completeness=100)
        sys.exit(2)
    else:
        logger.info("Label Studio configuration suceeded.")


        spec.update_run(str(run_id), {
            "wf_linked_types" : ["LS_annotation", "_BoundingBox"],
            "wf_creator": "label-studio" 
            })
        ls_env = os.environ.copy()
        add_common_vars(ls_env)
        ls_env["WORKFLOW_NAME"]="label-studio"
        ls_env["WORKFLOW_SPEC_ID"]=args.spec_id 
        ls_env["WORKFLOW_RUN_ID"]=str(run_id)
        ls_env["LABEL_STUDIO_CONFIGURED_STORAGE_BACKENDS"]="aperturedb gcs s3" 
        logger.error(f" ENV FOR MAIN IS: {ls_env}")

        logger.info("Preparing to start Label Studio.")
        subprocess.run( "bash /app/label_studio_run.sh", shell=True,env=ls_env)
        logger.info("Label Studio finished")



        spec.finish_run(str(run_id))
    set_state(LabelStudioPhase.FINISHED,completeness=100)
    spec.finish_spec()

def get_args():
    obj = ArgumentParser(support_legacy_envars=True)

    # configuration options
    obj.add_argument("--label-studio-default-project-name",type=str,default="ApertureDB Labeling",
            help="Name of a project to be created automatically (set to '' to disable")
    obj.add_argument("--label-studio-default-storage-name",type=str,default="ApertureDB",
            help="Name of the aperturedb storage to be created automatically (set to '' to disable")
    obj.add_argument("--label-studio-default-storage-limit",type=int,default=500,
            help="Value to be set as the default of images to be ingested on a sync in LS.") 
    obj.add_argument("--label-studio-default-import-annotations",type=bool,default=True,
            help="Whether to by default import Bboxes as annotations") 
    obj.add_argument("--label-studio-storage-annotations-ro",type=bool,default=False,
            help="If bbox annotations imported in LS are readonly ( default False )") 
    # hosting options
    obj.add_argument("--label-studio-url-path",type=str,default=None,
            help="Path to host label studio on other than /. Supply full external path: eg. http://localhost:8888/labelstudio ")
    # login options
    obj.add_argument("--label-studio-token",type=str,default=None, help="User token for label studio") 
    obj.add_argument("--label-studio-user",type=str,default=None,required=True, help="User for label studio") 
    obj.add_argument("--label-studio-password",type=str,default=None,required=True, help="User password for label studio") 

    # workflow options
    obj.add_argument("--spec-id",type=str,default=None,
            help="Spec id for this workflow. Default is random uuid.")

    # cleaning options
    obj.add_argument("--clean",type=bool,default=False,
            help="Whether the workflow should clean previous data loaded from this workflow name")
    obj.add_argument("--delete",type=bool,default=False,
            help="Whether the workflow should clean data associated with provided spec_id, then stop.")
    obj.add_argument("--delete-all",type=bool,default=False,
            help="Whether the workflow should clean all data from this workflow, then stop.")
    obj.add_argument("--delete-all-ls-data",type=bool,default=False,
            help="Whether the workflow should clean all data created by label studio in the database") 
    obj.add_argument('--log-level', type=str,
                 default='WARNING')


    params = obj.parse_args()
    logging.basicConfig(level=params.log_level.upper(), force=True)
    to_sanitize = []

    sanitized_params = {k: v if v is None or k not in to_sanitize else "**HIDDEN**" for k,v in params.__dict__.items()}
    logger.info(f"Parsed arguments: {sanitized_params}")

    if not(params.delete or params.delete_all or params.delete_all_ls_data) and \
            (params.label_studio_user is None or params.label_studio_password is None):
        logger.error("configuration for label studio user and password are "\
                    "required for modes other than delete, delete-all and delete-all-ls-data.")
        raise ValueError("--label-studio-user and/or --label-studio-password missing")

    if params.label_studio_default_storage_name != "" and \
    params.label_studio_default_project_name ==  "": 
        raise ArgumentError("Default storage cannot be set when default project is unset.")

    params.spec_id_created = params.spec_id is None
    if params.spec_id_created:
        params.spec_id = str(uuid4())

    return params



if __name__ == '__main__':
    args = get_args()
    main(args)
