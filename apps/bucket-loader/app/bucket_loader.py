#bucket_loader.py - ApertureData's bucket loading workflow
import logging
import sys
from uuid import uuid4

from aperturedb.CommonLibrary import create_connector, execute_query

from wf_argparse import ArgumentParser

from provider import AWSProvider,GCSProvider
from ingester import ImageIngester,VideoIngester,DocumentIngester
from spec import WorkflowSpec


logger = logging.getLogger(__name__)

def main(args):
    db = create_connector()

    if args.delete:
        WorkflowSpec.delete( "bucket-loader", args.spec_id )
        sys.exit(1)
    elif args.delete_all:
        WorkflowSpec.delete_all( "bucket-loader")
        sys.exit(1)


    spec = WorkflowSpec( db, "bucket-loader", args.spec_id, clean=args.clean )
    #if args.clean:
    #    clean_data( generate_spec_key(args.cloud_provider,args.bucket))

    run_id = uuid4()
    spec.add_run( run_id )
    provider = None
    if args.cloud_provider == "s3":
        provider = AWSProvider(args.bucket, args.aws_access_key_id, args.aws_secret_access_key)
    elif args.cloud_provider == "gs":
        provider = GCSProvider(args.bucket, args.gcp_service_account_key)

    if not provider.verify():
        logger.error("Workflow cannot continue, configuration incorrect")
        spec.finish_run(run_id)
        spec.finish_spec()
        sys.exit(1)



    ingestions = [
            ImageIngester( provider) if args.ingest_images else None,
            VideoIngester( provider) if args.ingest_videos else None,
            DocumentIngester( provider) if args.ingest_pdfs else None
            ]
    for ingestion in ingestions:
        if ingestion is None:
            continue
        # determines how many to ingest
        ingestion.set_add_object_paths( args.add_object_paths )
        ingestion.prepare()
        ingestion.load(db)

    spec.finish_run(run_id)
    spec.finish_spec()

def get_args():
    obj = ArgumentParser(support_legacy_envars=True)

    obj.add_argument("--cloud-provider",type=str, choices=["s3","gs"], required=True,
            help="Whether the workflow should ingest supported image types")
    obj.add_argument("--aws-access-key-id",type=str,default=None,
            help="The AWS Access Key for loading data using AWS") 
    obj.add_argument("--aws-secret-access-key",type=str,default=None,
            help="The AWS Secret Key for loading data using AWS")
    obj.add_argument("--gcp-service-account-key",type=str, default = None,
            help="The service account information for loading data using GCP") 
    obj.add_argument("--bucket",type=str,required=True,
            help="Which bucket to ingest data from") 
    obj.add_argument("--ingest-images",type=bool,default=False,
            help="Whether the workflow should ingest supported image types")
    obj.add_argument("--ingest-videos",type=bool,default=False,
            help="Whether the workflow should ingest supported video types")
    obj.add_argument("--ingest-pdfs",type=bool,default=False,
            help="Whether the workflow should ingest supported document types")
    obj.add_argument("--ingest-entities",type=bool,default=False,
            help="Whether the workflow should ingest supported entities types")
    obj.add_argument("--add-object-paths",type=bool,default=False,
            help="Whether the workflow should add a property - `adb_resource_path` to all bucket items added")
    obj.add_argument("--spec-id",type=str,default=None,
            help="Spec id for this workflow. Default is random uuid.")
    obj.add_argument("--clean",type=bool,default=False,
            help="Whether the workflow should clean previous data loaded from this bucket")
    obj.add_argument("--delete",type=bool,default=False,
            help="Whether the workflow should clean data associated with provided spec_id, then stop.")
    obj.add_argument("--delete-all",type=bool,default=False,
            help="Whether the workflow should clean all data from this workflow, then stop.")
    obj.add_argument("--clean-bucket",type=bool,default=False,
            help="Whether the workflow should clean previous data loaded from this bucket")
    obj.add_argument('--log-level', type=str,
                 default='WARNING')


    params = obj.parse_args()
    logging.basicConfig(level=params.log_level.upper(), force=True)
    to_sanitize = ["aws_access_key_id", "aws_secret_access_key", "gcp_service_account_key"]

    sanitized_params = {k: v if v is None or k not in to_sanitize else "**HIDDEN**" for k,v in params.__dict__.items()}
    logger.info(f"Parsed arguments: {sanitized_params}")

    if not (params.ingest_images or params.ingest_videos or params.ingest_pdfs or params.ingest_entities):
        logger.error("Workflow cannot proceed, nothing was selected to ingest")
        sys.exit(1)

    if params.cloud_provider == "s3":
        logger.info("Loading from S3")
        if not params.aws_access_key_id or not params.aws_secret_access_key:
            logger.error("Workflow cannot proceed, missing configuration for S3")
            sys.exit(1)
    elif params.cloud_provider == "gs":
        logger.info("Loading from GCP")
        if not params.gcp_service_account_key:
            logger.error("Workflow cannot proceed, missing configuration for GS")
            sys.exit(1)
    params.spec_id_created = params.spec_id is None
    if params.spec_id_created:
        params.spec_id = str(uuid4())

    return params



if __name__ == '__main__':
    args = get_args()
    main(args)
