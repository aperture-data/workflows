# ingest.py - for ingest from SQL

from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ImageDataCSV import ImageDataProcessor
import pandas as pd
from utils import hash_string,TableSpec
from sqlalchemy import URL,create_engine,Connection


class SQLEntityData(EntityDataCSV):
    def __init__(self, filename:str, sql_resource_name, primary_key_column_name,  sqldb:Connection, **kwargs):
        pass
    def getitem(self,idx):
        query,blobs = super(EntityDataCSV,self).getitem(idx)
        idx = self.df.index.start + idx
        resource_name = "{}/{}".format(sql_resource_name, self.df[idx][primary_key_column_name])
        object_hash = hash_string(resource_name) 

        props = query[0][self.command]["properties"]
        props["wf_sha1_hash"] = object_hash
        props["constraint_wf_sha1_hash"] = object_hash


        return query,blobs

class SQLBinaryData(SQLEntityData,ImageDataProcessor):
    """
    injects binary data into entity loading, either from SQL or from a url.
    binary_lookup_file is internal type - rows should be ordered exactly the
    same as input file.  expects first column to be:
      - url = get data from this urls
      - db_{name}  get data from connection,  name is the column to select for lookup
    """
    def __init__(self,
            filename:str, binary_df:str,
            sqldb:Connection, table_name:str = None, **kwargs):
        ImageDataProcessor.__init__(
            self, check_image=False, n_download_retries=3)
        super(SQLEntityData,self).__init__(filename, **kwargs)

        self.blob_df = binary_df 
        col_info = self.blob_df.columns[0]
        if col_info == "url":
            self.mode = "url" 
        elif col_info.startswith("db_"):
            self.mode = "db"
        else:
            raise Exception(f"binary table malformed; first column was {col_info}")


        self.table = table_name
        if self.mode =="db":
            self.column_name = col_info[len("db_"):]
            for db_col in db_tbl:
                if db_col.name == self.column_name:
                    self.column = db_col
        self.command = "AddBlob" 
    def set_image_mode(self,is_image_mode):
        self.command = "AddImage" if is_image_mode else "AddBlob"
    def getitem(self,idx):
        query,blobs = super(SQLEntityData,self).getitem(idx) 

        idx = self.df.index.start + idx
        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)

        blob = None
        if self.mode == "db":
            value = self.blob_df[idx]["db_"+self.column_name]
            
            select(self.column).where(self.column==value)
            result = conn.execute(blob_query.prepare(conn.engine))
            blob= result.first()[0]
        else: # url
            image_path = os.path.join(
                self.relative_path_prefix, self.df.loc[idx, self.source_type])
            img_ok, img = self.source_loader["url"](image_path)
            blob = img

        if blobs is None:
            blobs = []
        blobs.insert(0,blob)
        return query,blobs


class Provider:
    pass

class SQLProvider:
    def __init__(self,host:str, user:str, password:str, database:str,
            port:int=None, table:str=None):
    #conn:sqlalchemy.Connection , table: sqlalchemy.Table,
            #columns_to_load):
        self.user = user
        self.password = password
        self.host =host
        self.database = database
        self.engine = None
        self.table = table
    def get_engine(self):
        if self.engine is None:
            self.engine = create_engine(self.as_connection_string())
        return self.engine
    def host_name(self):
        return self.host
    def database_name(self):
        return self.database
    def table_name(self):
        return self.table
    def get_property_names(self):
        pass
    def as_connection_string(self):
        return "postgresql+psycopg://{}:{}@{}/{}".format(
                self.user,self.password,self.host,self.database)

    def verify(self):
        try:
            self.get_engine()
            return True;
        except Exception as e:
            print(f"Failed to connect to SQL server:{e}")
            return False

class Ingester:
    def __init__(self,  source: Provider, info:TableSpec): 
        self.multiple_objects = None
        self.source = source
        self.dataframe = None
        self.workflow_id = None


    def set_workflow_id(self,id):
        self.workflow_id = id

    def prepare(self):
        raise NotImplementedError("Base Class")
    def load(self):
        raise NotImplementedError("Base Class")

    def generate_df(self ):
        table_name = self.source.table_name()
        scheme = "sql://{}/{} ".format(
                self.source.host_name() , self.source.database_name())
        url_column_name = self.source.url_column_name()
        load_time = dt.datetime.now().isoformat()
        full = []
        objects = []
        for row in self.source.get_data():
            full.append(row)
        df = pd.DataFrame(
                columns = [ self.source.get_property_names() ],
                data=full)
        df['wf_creator'] =  "sql_ingestor" 
        df['wf_creator_key'] = hash_string( "{}/{}".format(scheme,table_name)) 
        #df['adb_mime_type'] = "TODO"
        if self.workflow_id:
            df['wf_workflow_id'] = self.workflow_id

        return df


class EntityIngester(Ingester):
    def __init__(self, source: Provider, info:TableSpec): 
        super(EntityIngester,self).__init__(source,info) 

class ImageIngester(Ingester):
    def __init__(self, source: Provider, info:TableSpec): 
        super(ImageIngester,self).__init__(source,info) 
    def prepare(self):
        self.df = super(ImageIngester,self).generate_df()
        self.binary_df = self.df[PK_COL,BIN_COL]
        self.df.drop([PK_COL,BIN_COL],inplace=True)

        print(self.df)
        print(self.binary_df)
    def load(self,db):
        print("Ready to load")
        csv_data = SQLBinaryDataCSV( filename=None,df=self.df)
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "image" if cnt == 1 else "images"
        print(f"Finished uploading {cnt} {noun}")

