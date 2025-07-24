import pandas as pd
from argparse import ArgumentParser
from pathlib import Path
import subprocess
import sys
import fnmatch
from typing import List

from sqlalchemy import insert, update, Table, Column, Integer, LargeBinary, MetaData, BLOB, String
from sqlalchemy import URL,create_engine
import sqlalchemy as sql

import utils



def scan(connection_string,table_ignore_list:list, column_ignore_list:list,
        url_columns:list, table_to_entity_map:dict,
        error_on_unused_binary:bool ) -> List[utils.TableSpec] :

    selected_tables = []
    if table_ignore_list is None:
        table_ignore_list = []
    if column_ignore_list is None:
        column_ignore_list = []
    if url_columns is None:
        url_columns = []
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine)
        for table in meta.sorted_tables:
            used_cols = []
            url_cols = []
            bin_cols = []
            print(f"* Table = {table.name}",end="")
            skip=False
            for pat in table_ignore_list: 
                if fnmatch.fnmatch( table.name, pat ):
                    print("- Skipped")
                    skip=True
                    break
            if skip:
                continue
            print("")

            table_type = "entity"
            entity_name = table.name
            expect_binary = False
            has_binary = False
            is_url = False

            if table.name in args.image_tables:
                print(f" ! Table is an image table")
                expect_binary = True
                table_type = "image"
            elif table.name in args.pdf_tables:
                print(f" ! Table is an pdf table")
                expect_binary = True
                table_type = "pdf"

            if table.name in table_to_entity_map:
                entity_name = table_to_entity_map[table.name]

            for col in table.columns:
                skip=False
                gen = col.type.as_generic()
                col_is_binary = isinstance(gen,LargeBinary)
                full_col_name = "{}.{}".format(table.name, col.name),
                print(f"  * Column = {col.name} {col.type} {gen}",end="") 

                for pat in column_ignore_list:
                    if fnmatch.fnmatch( full_col_name, pat):
                        skip=True
                        break
                if skip:
                    print(" - Skipped")
                    continue

                for pat in url_columns:
                    if fnmatch.fnmatch("{}.{}".format(table.name, col.name), pat):
                        is_url = True
                        break

                if is_url:
                    if is_binary: 
                        raise Exception(f"Column for url isn't a string; {full_col_name} is binary.")
                    elif not insinstance(gen,String):
                        raise Exception(f"Column for url isn't a string; {full_col_name} is {type(gen)} .")
                        print(" - URL")
                        url_cols.append(col.name)
                elif col_is_binary: 
                    print("- Binary")
                    if has_binary:
                        print(f"Ignoring additional binary column in table = {ful_col_name}")
                    else:
                        bin_cols.append(col.name)
                    has_binary = True
                else: 
                    print("")
                    used_cols.append(col.name)

            if has_binary:
                if not expect_binary:
                    if error_on_unused_binary:
                        raise Exception(f"Table {table.name} had a binary column but it was not expected.")
            elif expect_binary and not has_binary:
                raise Exception(f"Was expecting a binary in {table.name}, but didn't find any.")
            selected_tables.append(
                utils.TableSpec(table=table,prop_columns=used_cols,
                    url_columns=url_cols,bin_columns=bin_cols,name=entity_name,
                    entity_type = table_type))

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
    parser.add_argument('-U','--url-columns-for-binary-data',default=None,type=utils.CommandlineType.column_list,
        help="Column names which are url links to binary data") 
    parser.add_argument( '-T', '--tables-to-ignore', default=None, type=utils.CommandlineType.table_list,
        help="Tables to ignore") 
    parser.add_argument( '-C', '--columns-to-ignore', default=None, type=utils.CommandlineType.column_list,
        help="Columns to ignore") 
    parser.add_argument('-M', '--table-to-entity-map', default=None, type=utils.CommandlineType.item_map,
        help="Mapping of table names to entity names") 
    args= parser.parse_args()
    print(args.columns_to_ignore)
    print(args.tables_to_ignore)
    print(args.table_to_entity_map)
    return args


if __name__ == '__main__':

    args = get_opts()
    res = scan(args.connection_string,args.tables_to_ignore,args.columns_to_ignore,
            args.url_columns_for_binary_data, args.table_to_entity_map,
            args.undefined_blob_action == 'error' )
    print(res)
