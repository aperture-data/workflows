import pandas as pd
from argparse import ArgumentParser
from pathlib import Path
import subprocess
import sys
import logging
import fnmatch
from typing import List
from dataclasses import dataclass

from sqlalchemy import insert, update, Table, Column, Integer, LargeBinary, MetaData, BLOB, String
from sqlalchemy import URL,create_engine
from sqlalchemy import inspect
import sqlalchemy as sql

from utils import TableType, TableSpec,ConnectionSpec,CommandlineType

from typing import TypedDict 


logger = logging.getLogger(__name__)

def scan(engine:sql.Engine,
        image_tables:list, pdf_tables:list,
        table_ignore_list:list, column_ignore_list:list,
        url_columns:list, table_to_entity_map:dict,
        automatic_fk_mapping:bool,
        fk_map:dict,
        error_on_unused_binary:bool ) -> List[TableSpec] :
    """
    Scans the database given user configuration, and produces a list of
    specifications for tables to import.

    Arguments:
    engine: A SQLAlchemy engine representing the SQL server. Should be already verified to work.
    image_tables: a list of table names ( just the table, not the schema/db with it) that should be treated as a source for image entities
    pdf_tables: a list of table names ( just the table, not the schema/db with it ) that should be treated as a source for pdf entities.
    )
    table_ignore_list: a list of glob-compatable tables to ignore.
    column_ignore_list: a list of glob-compatable columns to ignore. Must include table portion to match.
    url_columns:  a list of glob-compatable columns to treat as urls that have a binary.
    table_to_entity_map: a dictionary which maps table names to entity names.
    automatic_fk_mapping: boolean to enable automatic identification of foreign keys. Must be in recognized format.
    fk_map: map of source to foreign key. Source is "table that does not own the key" String to String.
    error_on_unused_binary: boolean to enable throwing an error if a table has a binary column that is unused.


    Returns:
        List of 0 or more TableSpec or ConnectionSpec

        ConnectionSpecs will always be last to enable loading by walking through the
        list in order.

    Automatic Foreign Key mapping

    There are serveral ways to automatically map foreign keys.
    * fk_{table}_{col}
    * foreign key definitions in the db
    * unique table names across the db.

    We implement the first here.
    """

    selected_tables = []
    if image_tables is None:
        image_tables = []
    if pdf_tables is None:
        pdf_tables = []
    if table_ignore_list is None:
        table_ignore_list = []
    if column_ignore_list is None:
        column_ignore_list = []
    if url_columns is None:
        url_columns = []
    if table_to_entity_map is None:
        table_to_entity_map = {}
    if fk_map is None:
        fk_map = {}

    @dataclass
    class FkColMapping:
        col:Column
        table:Table
        is_automatic:bool
        target:str = None


    class FkMap(TypedDict):
        x: str
        y: FkColMapping
    potential_fks = FkMap() # maps a potential fk column to its source
    all_cols = {} # all used colds that aren't fks ( and thus potential targets)
    create_connections = automatic_fk_mapping or len(fk_map.keys()) != 0


    with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine)
        for table in meta.sorted_tables:
            used_cols = []
            url_cols = []
            bin_cols = []
            pk = None
            table_log = f"* Table = {table.name}"
            skip=False
            for pat in table_ignore_list: 
                if fnmatch.fnmatch( table.name, pat ):
                    logger.info( table_log + "- Skipped")
                    skip=True
                    break
            if skip:
                continue
            logger.info(table_log)

            table_type = TableType.ENTITY
            entity_name = table.name
            expect_binary = False
            has_binary = False
            is_url = False

            if table.name in table_to_entity_map:
                entity_name = table_to_entity_map[table.name]
                logger.info(f" @ Mapped {table.name} to {entity_name}")

            if table.name in image_tables:
                logger.info(f" ! Table is an image table")
                expect_binary = True
                table_type = TableType.IMAGE 
            elif table.name in pdf_tables:
                logger.info(f" ! Table is an pdf table")
                expect_binary = True
                table_type = TableType.PDF


            for col in table.columns:
                skip=False
                gen = col.type.as_generic()
                col_is_binary = isinstance(gen,LargeBinary)
                full_col_name = "{}.{}".format(table.name, col.name)
                col_log = f"  * Column = {col.name} {col.type} {gen}"

                for pat in column_ignore_list:
                    if fnmatch.fnmatch( full_col_name, pat):
                        skip=True
                        break
                if skip:
                    logger.info(col_log + " - Skipped")
                    continue

                fk_is_ignored = False
                fk_used = False

                for name in fk_map.keys():
                    if name == full_col_name:
                        # force fk key to be 
                        if fk_map[name] == "":
                            fk_is_ignored = True
                        else:
                            logger.info(col_log + " - Foriegn Key")
                            fkm = FkColMapping(table=table, col=col, is_automatic=False)
                            fkm.target = fk_map[name]
                            potential_fks[full_col_name] = fkm 
                            
                            continue

                if automatic_fk_mapping and not fk_is_ignored:
                    if col.name.startswith("fk_"):
                        potential_fks[full_col_name] = FkColMapping(table=table,
                                col=col, is_automatic=True)
                        logger.info(col_log + " - Foriegn Key (Auto)")
                        continue
                    # could do auto on foriegn key def in db.

                for pat in url_columns:
                    if fnmatch.fnmatch( full_col_name,pat):
                            is_url = True
                            break

                if is_url:
                    if col_is_binary: 
                        raise Exception(f"Column for url isn't a string; {full_col_name} is binary.")
                    elif not isinstance(gen,String):
                        raise Exception(f"Column for url isn't a string; {full_col_name} is {type(gen)} .")
                    logger.info(col_log+" - URL")
                    url_cols.append(col.name)
                    has_binary = True
                elif col_is_binary: 
                    logger.info(col_log+"- Binary")
                    if has_binary:
                        print(f"Ignoring additional binary column in table = {ful_col_name}")
                    else:
                        bin_cols.append(col.name)
                    has_binary = True
                else: 
                    logger.info(col_log) 
                    used_cols.append(col.name)

                all_cols[full_col_name] = [ table, col ]


            if has_binary:
                if not expect_binary:
                    if error_on_unused_binary:
                        raise Exception(f"Table {table.name} had a binary column but it was not expected.")
                    else:
                        logger.warning(f"Ignoring binary column in {table.name}")
            elif expect_binary and not has_binary:
                raise Exception(f"Was expecting a binary in {table.name}, but didn't find any.")

            #if has_binary or len(url_cols) != 0:
            if True: # need pk if we are linking with a connection and not going to ask twice
                table_info = inspect(table).primary_key
                if len(table_info.columns) != 1:
                    raise Exception("Exactly 1 column is required to be a primary key")
                pk = table_info.columns[0].name
            logger.info(f"Primary key is {pk}")
            selected_tables.append(
                TableSpec(table=table,prop_columns=used_cols,
                    url_columns=url_cols,bin_columns=bin_cols,name=entity_name,
                    entity_type = table_type,primary_key=pk))

    for fk_to_connect in potential_fks.keys():
        fkmap = potential_fks[fk_to_connect]
        target = None
        if fkmap.is_automatic:
            # our automatic mapping : fk_table_col
            split = fk_to_connect.split("_")
            if len(split) != 3:
                raise Exception(f"Unable to automatically handle foreign key {fk_to_connect}")
            target = "{}.{}".format(split[1],split[2])
        else:
            target = fkmap.target

        if target not in all_cols.keys():
            raise Exception(f"Cannot find target column for {fk_to_connect}: ({target})")
        (ttbl,tcol) = all_cols[target]
        # we require target column to be the primary key right now.
        #  ultimately we could support any kind of linkage, but we would need to
        #  map back to a way to create a link, which requires a unique key of
        #  some sort; for situations where it linked to like fk_category_group that was not
        #  unique, as long as we could generate a jump to the unique items, (
        #  like say category.id ( which multiple entries would share a group ) )
        # - we can do it, but making a testcase for this is potentially complicated.
        logger.info(f"Creating Connections from {fk_to_connect} to {target}")
        filtered_source_specs = list(filter(lambda spec: spec.entity_type is not TableType.CONNECTION
                    and spec.table.name == fkmap.table.name, selected_tables))
        if len(filtered_source_specs) != 1:
            raise Exception(f"Expected 1 matching source table named "\
                    f"{fkmap.table.name} for {fk_to_connect}, but had {len(filtered_source_specs)}")
        source_spec = filtered_source_specs[0]

        filtered_tgt_specs = list(filter(lambda spec: spec.entity_type is not TableType.CONNECTION
                    and spec.table.name == ttbl.name, selected_tables))
        if len(filtered_tgt_specs) != 1:
            raise Exception(f"Expected 1 matching target table named "\
                    f"{ttbl} for {target}, but had {len(filtered_tgt_specs)}")
        tgt_spec = filtered_tgt_specs[0]

        if tgt_spec.primary_key != tcol.name:
            raise Exception(f"Target column for {fk_to_connect} is not primary"\
            f"key of table {ttbl} ( target is {tcol}, primary key is {tgt_spec.primary_key}")


        fk_on_source_side=fk_to_connect.split(".")[1]
        selected_tables.append(
                ConnectionSpec(table=fkmap.table,target_table=ttbl,prop_columns=[
                    fk_on_source_side, source_spec.primary_key],
                    source_link_col=fk_on_source_side,
                    primary_key=source_spec.primary_key,target_col=tcol.name)) 

    return selected_tables


def get_opts():
    parser = ArgumentParser()
    parser.add_argument('-c','--connection-string',required=True,help="Connection string to db") 
    parser.add_argument('-g','--generated-path',default="generated/",
        help="Path to put generated output") 
    parser.add_argument('-I','--image-tables',default=None,
        help="Tables to generate Images from")
    parser.add_argument('-P','--pdf-tables',default=None,
        help="Tables to generate Images from")
    parser.add_argument('-X','--undefined-blob-action',choices=['ignore','error'], default='ignore', 
        help="Handling of blob columns that aren't expected. Ignore doesn't include them. Error aborts ingest.") 
    parser.add_argument('-U','--url-columns-for-binary-data',default=None,type=CommandlineType.column_list,
        help="Column names which are url links to binary data") 
    parser.add_argument( '-T', '--tables-to-ignore', default=None, type=CommandlineType.table_list,
        help="Tables to ignore") 
    parser.add_argument( '-C', '--columns-to-ignore', default=None, type=CommandlineType.column_list,
        help="Columns to ignore") 
    parser.add_argument('-M', '--table-to-entity-map', default=None, type=CommandlineType.item_map,
        help="Mapping of table names to entity names") 
    parser.add_argument('-A', '--automatic-fk-map', default=False,type=bool,
        help="Automatically map fks") 
    parser.add_argument('-F', '--fk-map', default=None, type=CommandlineType.item_map,
        help="FK mapping")
    parser.add_argument('--log-level', type=str, default='INFO')
    args= parser.parse_args()
    print(f"Column Ignore: {args.columns_to_ignore}")
    print(f"Table Ignore: {args.tables_to_ignore}")
    print(f"Table-Entity Map: {args.table_to_entity_map}")
    return args


if __name__ == '__main__':

    args = get_opts()
    engine = create_engine(args.connection_string)
    logging.basicConfig(level=args.log_level.upper(), force=True)
    res = scan(engine, args.image_tables, args.pdf_tables,
            args.tables_to_ignore,args.columns_to_ignore,
            args.url_columns_for_binary_data, args.table_to_entity_map,
            args.automatic_fk_map, args.fk_map,
            args.undefined_blob_action == 'error' )
    print("Specifications")
    for r in res:
        print(f"++ {r}")
