# finder.py - find images in ApertureDB
from aperturedb.CSVParser import CSVParser
import pandas as pd
import sys

class ObjectExistsDataCSV(CSVParser):
    def __init__(self,search_type,search_cmd,csv_column,db_column, filename,df=None):
        self.search_type = search_type
        self.cmd = search_cmd
        self.csv_column = csv_column
        self.db_column = db_column
        self.found_ids = []
        super().__init__(filename,df=df)

    # accessors for user when operation is complete.
    def get_missing_items(self):
        found = pd.DataFrame( self.found_ids, columns=[self.csv_column])

        missing = pd.merge(self.df, found, indicator=True, how='left') \
                .query( '_merge=="left_only"') \
                .drop('_merge',axis=1)
        return missing
    def get_found_items(self):
        found_ids = pd.DataFrame( self.found_ids, columns=[self.csv_column])

        found = pd.merge(self.df, found_ids, indicator=True, how='left') \
                .query( '_merge=="both"') \
                .drop('_merge',axis=1)
        return found


    def getitem(self,idx):
        blobs = []
        query = [{self.cmd: {
                   "constraints": {
                        self.db_column : ["==",self.df.at[idx, self.csv_column] ]
                        },
                    "results": {
                        "count": True
                        }
                    }
		}]

        if self.cmd == "FindEntity":
            query[0][self.cmd]["with_class"] = self.search_type
        if self.cmd == "FindImage":
            query[0][self.cmd]["blobs"] = False
        return query,blobs

    # handles responses from server
    def response_handler(self,query,query_blobs,response,resp_blobs):
        # if response is list, success.
        if not isinstance(response,dict):
            for resp_idx in range(len(response)):
                if isinstance(response[resp_idx],dict):
                    find_resp = response[resp_idx][self.cmd]
                    find_query = query[resp_idx][self.cmd]
                    target_guid = find_query["constraints"][self.db_column][1]
                    # if count was 1 for the guid in the request, it exists.
                    was_found = find_resp["count"] == 1
                    if was_found:
                        self.found_ids.append(target_guid)


class ImageExistsDataCSV(ObjectExistsDataCSV):
    def __init__(self,csv_column,db_column,filename,df=None):
        super().__init__("_Image","FindImage",csv_column,db_column,filename,df)

class PolygonExistsDataCSV(ObjectExistsDataCSV):
    def __init__(self,csv_column,db_column,filename,df=None):
        super().__init__("_Polygon","FindPolygon",csv_column,db_column,filename,df)

class ConnectionExistsDataCSV(ObjectExistsDataCSV):
    def __init__(self,csv_column,db_column,filename,df=None):
        super().__init__("_Connection","FindConnection",csv_column,db_column,filename,df)

