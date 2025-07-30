#sql_loader.py - ApertureData's SQL loading workflow
import logging
import sys
from uuid import uuid4

from aperturedb.CommonLibrary import create_connector, execute_query

from wf_argparse import ArgumentParser

from ingest import ImageIngester, EntityIngester,SQLProvider,ConnectionIngester,EntityMapper,PDFIngester
from scan import scan
from spec import WorkflowSpec
from utils import TableType, TableSpec,ConnectionSpec,CommandlineType


logger = logging.getLogger(__name__)

def main(args):
    db = create_connector()

    if args.delete:
        WorkflowSpec.delete( "sql-loader", args.spec_id )
        sys.exit(1)
    elif args.delete_all:
        WorkflowSpec.delete_all( "sql-loader")
        sys.exit(1)


    spec = WorkflowSpec( db, "sql-loader", args.spec_id, clean=args.clean )

    run_id = uuid4()
    spec.add_run( run_id )
    provider = SQLProvider(args.sql_host, args.sql_user, args.sql_password, args.sql_database, port = args.sql_port)

    if not provider.verify():
        logger.error("Workflow cannot continue, configuration incorrect")
        spec.finish_run(run_id)
        spec.finish_spec()
        sys.exit(1)

    tables = scan( provider.get_engine(),
            args.image_tables, args.pdf_tables, args.tables_to_ignore,args.columns_to_ignore,
            args.url_columns_for_binary_data, args.table_to_entity_mapping,
            args.automatic_foreign_key, args.foreign_key_entity_mapping,
            args.undefined_blob_action == 'error' )

    emapper = EntityMapper()

    def create_ingester(table_info):
        ingester = None
        if table_info.entity_type is TableType.ENTITY:
            ingester = EntityIngester(provider,table_info)
        elif table_info.entity_type is TableType.CONNECTION:
            ingester = ConnectionIngester(provider,table_info)
        elif table_info.entity_type is TableType.PDF:
            ingester = PDFIngester(provider,table_info)
        elif table_info.entity_type is TableType.IMAGE:
            ingester = ImageIngester(provider,table_info)
        else:
            raise Exception(f"Unhandled table type in create_ingester: {table_info.entity_type}")
        ingester.set_workflow_id( str(run_id))
        ingester.set_entity_mapper( emapper )
        return ingester

    ingestions = [ create_ingester(info) for info in tables ]


    linked_types_to_run = set()
    for ingestion in ingestions:
        if ingestion is None:
            continue
        ingestion.prepare()
        ingestion.load(db)
        linked_types_to_run.update( ingestion.get_types_added() )

    spec.finish_run(str(run_id), { "wf_linked_types" : list(linked_types_to_run) } )
    spec.finish_spec()

def get_args():
    obj = ArgumentParser(support_legacy_envars=True)

    # connection options
    obj.add_argument("--sql-host",type=str,required=True,
            help="Which SQL Host to ingest data from") 
    obj.add_argument("--sql-port",type=int,default=None,
            help="Port for the SQL server ( if not default)")
    obj.add_argument("--sql-user",type=str,required=True,
            help="Name of the SQL user associated with the host") 
    obj.add_argument("--sql-password",type=str,required=True,
            help="Password for the SQL user") 
    obj.add_argument("--sql-database",type=str,required=True,
            help="Which Database to ingest data from") 

    # workflow options
    obj.add_argument("--spec-id",type=str,default=None,
            help="Spec id for this workflow. Default is random uuid.")
    obj.add_argument('--image-tables',default=None,
        help="Tables to generate Images from")
    obj.add_argument('--pdf-tables',default=None,
        help="Tables to generate Images from")
    obj.add_argument('--undefined-blob-action',choices=['ignore','error'], default='ignore', 
        help="Handling of blob columns that aren't expected. Ignore doesn't include them. Error aborts ingest.") 
    obj.add_argument('--url-columns-for-binary-data',default=None,type=CommandlineType.column_list,
        help="Column names which are url links to binary data") 
    obj.add_argument( '--tables-to-ignore', default=None, type=CommandlineType.table_list,
        help="Tables to ignore") 
    obj.add_argument( '--columns-to-ignore', default=None, type=CommandlineType.column_list,
        help="Columns to ignore") 
    obj.add_argument( '--table-to-entity-mapping', default=None, type=CommandlineType.item_map,
        help="Mapping of table names to entity names") 

    # connection options
    obj.add_argument( '--foreign-key-entity-mapping', default=None, type=CommandlineType.item_map,
        help="Mapping of foreign keys to their source table and column")
    obj.add_argument( '--automatic-foreign-key', default=False, type=bool,
        help="Enable mapping of regularly named forign keys")

    # cleaning options
    obj.add_argument("--clean",type=bool,default=False,
            help="Whether the workflow should clean previous data loaded from this bucket")
    obj.add_argument("--delete",type=bool,default=False,
            help="Whether the workflow should clean data associated with provided spec_id, then stop.")
    obj.add_argument("--delete-all",type=bool,default=False,
            help="Whether the workflow should clean all data from this workflow, then stop.")
    obj.add_argument("--clean-database",type=bool,default=False,
            help="Whether the workflow should clean previous data loaded from this database")
    obj.add_argument('--log-level', type=str,
                 default='WARNING')


    params = obj.parse_args()
    logging.basicConfig(level=params.log_level.upper(), force=True)
    to_sanitize = ["sql-password"] 

    sanitized_params = {k: v if v is None or k not in to_sanitize else "**HIDDEN**" for k,v in params.__dict__.items()}
    logger.info(f"Parsed arguments: {sanitized_params}")

    params.spec_id_created = params.spec_id is None
    if params.spec_id_created:
        params.spec_id = str(uuid4())

    return params



if __name__ == '__main__':
    args = get_args()
    main(args)
