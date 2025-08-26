#label_studio.py - ApertureData's Label Studio workflow
import logging
import sys
import os
from uuid import uuid4


from aperturedb.CommonLibrary import create_connector 
from wf_argparse import ArgumentParser

from spec import WorkflowSpec

import subprocess
import re


logger = logging.getLogger(__name__)

def main(args):
    db = create_connector()

    if args.delete:
        WorkflowSpec.delete( "label-studio", args.spec_id )
        sys.exit(0)
    elif args.delete_all:
        WorkflowSpec.delete_all( "label-studio") 
        sys.exit(0)
    elif args.delete_all_ls_data:
        # note we don't verify host/database - a host could not be available,
        # but data could have been loaded from it.
        WorkflowSpec.delete_all_creator( "label-studio" )
        sys.exit(0)



    def add_path_vars( env ):

        full_path = None
        if os.environ['DB_HOST_PUBLIC']:
            # generate path for cloud
            full_path = "https://{}/labelstudio".format(os.environ['DB_HOST_PUBLIC'])
            logger.error(f"Set url from DB_HOST_PUBLIC: {full_path}")
        if args.label_studio_url_path is not None:
            if full_path is not None:
                logger.warning("Overriding full path with explicit workflow option")
            full_path = args.label_studio_url_path
            logger.error(f"Set url from workflow argument: {full_path}")

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
                logger.error(f"Changed {oldhost} to {host}")
                full_path = "{}://{}{}".format(proto,host,subpath)
            logger.error(f"Subpath = {subpath}") 

            # strip trailing /
            if subpath[-1:] == '/':
                logger.error("Subpath had trailing slash, stripped.") 
                subpath = sub_path[:-1]

            logger.error(f"Path is {full_path} and {subpath}")
            
            env["LABEL_STUDIO_HOST"] = full_path
            env["LABEL_STUDIO_STATIC_PATH"] = "{}/static".format( subpath )
            env["LABEL_STUDIO_URL_BASE"] = "{}".format( subpath )


    logger.info("Preparing for Label Studio configuration.")
    # do config for LS
    cfg_env = os.environ.copy()
    add_path_vars(cfg_env)
    cfg_env["LABEL_STUDIO_USERNAME"]=args.label_studio_user
    cfg_env["LABEL_STUDIO_PASSWORD"]=args.label_studio_password
    if args.label_studio_token:
        cfg_env["LABEL_STUDIO_USER_TOKEN"]=args.label_studio_token
    ret = subprocess.run("bash /app/label_studio_init.sh", shell=True, env=cfg_env)
    if ret.returncode != 0:
        logger.error("Label Studio configuration failed.")
    else:
        logger.info("Label Studio configuration suceeded.")
        spec = WorkflowSpec( db, "label-studio", args.spec_id, clean=args.clean )

        run_id = uuid4()
        spec.add_run( run_id )

        spec.update_run(str(run_id), {
            "wf_linked_types" : ["LS_annotation", "_BoundingBox"],
            "wf_creator": "label-studio" 
            })
        ls_env = os.environ.copy()
        add_path_vars(ls_env)
        ls_env["WORKFLOW_NAME"]="label-studio"
        ls_env["WORKFLOW_SPEC_ID"]=args.spec_id 
        ls_env["WORKFLOW_RUN_ID"]=str(run_id)
        ls_env["LABEL_STUDIO_CONFIGURED_STORAGE_BACKENDS"]="aperturedb gcs s3" 
        ls_env["LABEL_STUDIO_APERTUREDB_KEY"]=db.config.deflate()
        logger.error(f" ENV FOR MAIN IS: {ls_env}")

        logger.info("Preparing to start Label Studio.")
        subprocess.run( "bash /app/label_studio_run.sh", shell=True,env=ls_env)
        logger.info("Label Studio finished")



        spec.finish_run(str(run_id))
    spec.finish_spec()

def get_args():
    obj = ArgumentParser(support_legacy_envars=True)

    # hosting options
    obj.add_argument("--label-studio-url-path",type=str,default=None,
            help="Path to host label studio on other than /. Supply full external path: eg. http://localhost:8888/labelstudio ")
    # login options
    obj.add_argument("--label-studio-token",type=str,default=None,
            help="User token for label studio") 
    obj.add_argument("--label-studio-user",type=str,default=None,required=True,
            help="User for label studio") 
    obj.add_argument("--label-studio-password",type=str,default=None,required=True,
            help="User password for label studio") 

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

    params.spec_id_created = params.spec_id is None
    if params.spec_id_created:
        params.spec_id = str(uuid4())

    return params



if __name__ == '__main__':
    args = get_args()
    main(args)
