# utils.py - utilities for SQL workflow
from dataclasses import dataclass
from typing import List
from sqlalchemy import Table

@dataclass
class TableSpec:
    table: Table
    prop_columns: List[str]
    bin_columns: List[str]
    url_columns: List[str]
    name: str
    entity_type:str

class CommandlineType:
    @staticmethod
    def table_list(input_str):
        items = input_str.split(',')
        return items
    def column_list(input_str):
        items = input_str.split(',')
        for item in items:
            parts = item.split('.')
            if len(parts) != 2:
                raise Exception(f"Column list item {item} does not have 2 parts; expect table.column")
        return items
    def item_map(input_str):
        output = {}
        items = input_str.split(',')
        for item in items:
            pair = item.split(':')
            if len(pair) != 2:
                raise Exception(f"Map pair item {item} does not have 2 parts; expect key:value")
            k,v = pair
            k = k.strip()
            if k in output:
                raise Exception(f"Map key item {k} already exists in map.")
            output[k]=v.strip()
        return output

