# utils.py - utilities for SQL workflow
from dataclasses import dataclass,field
from typing import List
from sqlalchemy import Table
from enum import Enum

import hashlib

def hash_string(string):
    return hashlib.sha1(string.encode('utf-8')).hexdigest()

class TableType(Enum):
    ENTITY = 1
    IMAGE = 2
    PDF = 3
    CONNECTION = 4

@dataclass
class TableSpec:
    table: Table
    prop_columns: List[str]
    bin_columns: List[str]
    url_columns: List[str]
    name: str
    entity_type: TableType
    primary_key:str

@dataclass
class ConnectionSpec:
    table: Table  # should be tablespec?
    target_table: Table
    prop_columns: List[str]
    primary_key:str # of source table
    source_link_col: str # the 'foreign key' column in the source table
    target_col:str # simple column name of the target, on the target side.
    entity_type:TableType = TableType.CONNECTION
    bin_columns: List[str] = field( default_factory= list )
    url_columns: List[str] = field( default_factory=list)

class CommandlineType:
    @staticmethod
    def table_list(input_str):
        items = input_str.split(',')
        return items

    @staticmethod
    def column_list(input_str):
        items = input_str.split(',')
        for item in items:
            item = item.strip()
            if item == "":
                continue
            parts = item.split('.')
            if len(parts) != 2:
                raise Exception(f"Column list item \"{item}\" does not have 2 parts; expect table.column")
        return items

    @staticmethod
    def item_map(input_str):
        output = {}
        items = input_str.split(',')
        for item in items:
            item = item.strip()
            if item == "":
                continue
            pair = item.split(':')
            if len(pair) != 2:
                raise Exception(f"Map pair item {item} does not have 2 parts; expect key:value")
            k,v = pair
            k = k.strip()
            if k in output:
                raise Exception(f"Map key item {k} already exists in map.")
            output[k]=v.strip()
        return output

