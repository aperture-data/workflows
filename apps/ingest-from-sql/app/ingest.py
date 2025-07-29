# ingest.py - for ingest from SQL

from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ImageDataCSV import ImageDataProcessor,ImageDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
import pandas as pd
from utils import hash_string,TableSpec,ConnectionSpec
from sqlalchemy import URL,create_engine,Connection,MetaData,select
import copy
import datetime as dt

from aperturedb.ParallelLoader import ParallelLoader

class EntityMapper:
    def __init__(self):
        self.map = {}

    def add_mapping(self,table_name,entity_name):
        self.map[table_name]=entity_name

    def get_mapping(self,table_name):
        if not table_name in self.map:
            print(f"{table_name} not in entity map!")
        return self.map[table_name]


class SQLBaseDataCSV():
    #def __init__(self, filename:str, sql_resource_name, primary_key_column_name,  sqldb:Connection, **kwargs):
    def __init__(self, sql_resource_prefix, primary_key_column_name, **kwargs):
        print("*****************SBD**********************")
        print(f" {sql_resource_prefix} // {primary_key_column_name}")
        #super().__init__(filename, **kwargs)
        self.sql_resource_prefix = sql_resource_prefix
        self.primary_key_column_name = primary_key_column_name
    def injected_getitem(self,idx,pre_func):
        query,blobs = pre_func(self,idx)
        #print(f"IJGI {query}")
        idx = self.df.index.start + idx
       # print(self.df.loc[idx])
        resource_name = "{}/{}".format(self.sql_resource_prefix,
                self.df.loc[idx,self.primary_key_column_name])
        object_hash = hash_string(resource_name) 

        props = query[0][self.command]["properties"]
        props["wf_sha1_hash"] = object_hash
        query[0][self.command]["if_not_found"]= { "wf_sha1_hash" : ["==", object_hash ]}

        print(f"Object {resource_name} -> {object_hash}")
        #print(f"IJGI {query}")
        return query,blobs

class SQLEntityDataCSV(SQLBaseDataCSV,EntityDataCSV):
    def __init__(self, filename:str, sql_resource_prefix, primary_key_column_name, **kwargs):
        EntityDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)
    def getitem(self,idx):
        return self.injected_getitem( idx, EntityDataCSV.getitem )
class SQLConnectionDataCSV(SQLBaseDataCSV,ConnectionDataCSV):
    def __init__(self, filename:str, sql_resource_prefix, primary_key_column_name, **kwargs):
        ConnectionDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)
    def getitem(self,idx):
        query,blobs = super().getitem(idx)
        def query_hash(q):
            return q[list(q.keys())[0]]['constraints']['wf_sha1_hash'][1]
        hash1 = query_hash( query[0] )
        hash2 = query_hash( query[1] )
        print(f"Connecting {hash1} and {hash2}")
        object_hash = hash_string(f"{hash1}{hash2}") 
        props = query[2]["AddConnection"]["properties"]
        query[2]["AddConnection"]["if_not_found"] = { "wf_sha1_hash":
                ["==",object_hash]}
        props["wf_sha1_hash"] = object_hash
        return query,blobs

class SQLBinaryDataCSV(SQLBaseDataCSV,EntityDataCSV,ImageDataProcessor):
    """
    injects binary data into entity loading, either from SQL or from a url.
    binary_lookup_file is internal type - rows should be ordered exactly the
    same as input file.  expects first column to be:
      - url = get data from this urls
      - db_{name}  get data from connection,  name is the column to select for lookup
    """
#    def __init__(self, filename:str, binary_df:str, sqldb:Connection, table_name:str, primary_key:str, **kwargs):
    def __init__(self, filename:str, binary_df:str, sql_resource_prefix,
            primary_key_column_name, **kwargs):
        EntityDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)

        ImageDataProcessor.__init__( self, check_image=False, n_download_retries=3)

        self.blob_df = binary_df 
        col_info = self.blob_df.columns[0]
        if col_info == "url":
            self.mode = "url" 
        elif binary_df is not None:
            self.mode = "db"
        else:
            raise Exception(f"binary table malformed; first column was {col_info} and no binary_df passed")


        #self.table = table_name
        if self.mode =="db":
            self.column_name = "data"
        self.command = "AddBlob" 
    def set_image_mode(self,is_image_mode):
        self.command = "AddImage" if is_image_mode else "AddBlob"
    def get_indices(self):
        return {
            "entity": {
                "_Image": [ "wf_sha1_hash"] 
            }
        }   
    def getitem_binary(self,idx):
        custom_fields = {}
        blobs = []
        q = []
        #if self.format_given:
        #    custom_fields["format"] = self.df.loc[idx, IMG_FORMAT]
        ai = self._basic_command(idx, custom_fields)
        # Each getitem query should be properly defined with a ref.
        # A ref shouldb be added to each of the commands from getitem implementation.
        # This is because a transformer or ref updater in the PQ
        # will need to know which command to update.
        ai[self.command]["_ref"] = 1
        #blobs.append(img)
        q.append(ai)

        return q, blobs

    def getitem(self,idx):
        query,blobs = self.injected_getitem(idx,SQLBinaryDataCSV.getitem_binary) 

        idx = self.df.index.start + idx

        blob = None
        if self.mode == "db":
            blob = self.blob_df.loc[idx,self.column_name]
            
        else: # url
            image_path = os.path.join(
                self.relative_path_prefix, self.df.loc[idx, self.source_type])
            img_ok, img = self.source_loader["url"](image_path)
            blob = img

        if blobs is None:
            blobs = []
        blobs.insert(0,blob)
        return query,blobs
    def validate(self):
        pass


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

    def as_table(self,info):
        new = copy.copy(self)
        new.info = info
        new.table = info.table.name
        return new

    def get_hash_prefix(self):
        return f"postgres://{self.host}/{self.database}"
    def get_table_hash_prefix(self):
        return f"{self.get_hash_prefix()}/{self.table_name()}"
    def get_property_names(self):
        pass

    def url_column_name(self):
        pass

    def get_data_cols(self):
        return self.info.prop_columns + self.info.bin_columns + self.info.url_columns 
    def get_pk_col(self):
        return self.info.primary_key
    def get_bin_col(self):
        return self.info.bin_columns[0]

    def get_data(self):
        meta = MetaData()
        meta.reflect(bind=self.engine,only=[self.table])
        t = meta.sorted_tables[0]
        to_select=[]
        all_ret_cols = self.info.prop_columns + self.info.bin_columns + self.info.url_columns 
        print(all_ret_cols)
        for c in t.columns:
            if c.name in all_ret_cols:
                to_select.append(c)
        stmt = select(*to_select)
        data=[]
        with self.engine.connect() as conn:
            print(stmt)
            rows = conn.execute(stmt.compile(self.engine))
            for r in rows:
                data.append(r)

        return data


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
        self.source = source.as_table(info)
        self.dataframe = None
        self.workflow_id = None
        self.info = info


    def set_workflow_id(self,id):
        self.workflow_id = id

    def set_entity_mapper(self,m):
        self.emapper = m

    def get_entity_mapper(self):
        return self.emapper

    def prepare(self):
        raise NotImplementedError("Base Class")
    def load(self):
        raise NotImplementedError("Base Class")

    def generate_df(self ):
        table_name = self.source.table_name()
        scheme = "sql://{}/{} ".format(
                self.source.host_name() , self.source.database_name())
        #url_column_name = self.info.url_columns
        load_time = dt.datetime.now().isoformat()
        full = []
        objects = []
        for row in self.source.get_data():
            full.append(row)
        df = pd.DataFrame(
                columns =  self.source.get_data_cols(),
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
    def prepare(self):
        print("Prepare Entity")
        self.df = super(EntityIngester,self).generate_df()
        print(f"Columns: {self.df.columns}")
        print(self.df)
        self.emapper.add_mapping( self.source.table_name(),
                self.source.table_name())
    def load(self,db):
        print("Ready to load")
        ename = self.emapper.get_mapping(self.info.table.name)
        self.df.insert(0,"EntityClass", ename)
        print(self.df)
        csv_data = SQLEntityDataCSV( filename=None,df=self.df,
                primary_key_column_name=self.source.get_pk_col(),
                sql_resource_prefix=self.source.get_table_hash_prefix())
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "entity" if cnt == 1 else "entities"
        print(f"Finished uploading {cnt} {noun}")

        print(f"Columns: {self.df.columns}")
        print(self.df)

class ImageIngester(Ingester):
    def __init__(self, source: Provider, info:TableSpec): 
        super(ImageIngester,self).__init__(source,info) 
    def prepare(self):
        self.df = super(ImageIngester,self).generate_df()
        print(self.df)
        self.binary_df = \
            self.df[[self.source.get_pk_col(),self.source.get_bin_col()]].copy()
        print(self.df.columns)
        self.df.drop(columns=[self.source.get_bin_col()],inplace=True)

        print(self.df)
        print(self.binary_df)
        self.binary_df.rename(columns={self.source.get_bin_col():"data"},inplace=True)
        self.emapper.add_mapping( self.source.table_name(),
                "_Image")

    def load(self,db):
        print("Ready to load")
        csv_data = SQLBinaryDataCSV(
                filename=None,sql_resource_prefix=self.source.get_table_hash_prefix(),
                primary_key_column_name=self.source.get_pk_col(),
                df=self.df,binary_df=self.binary_df)
        csv_data.set_image_mode(True)
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "image" if cnt == 1 else "images"
        print(f"Finished uploading {cnt} {noun}")

class ConnectionIngester(Ingester):
    def __init__(self, source: Provider, info:ConnectionSpec): 
        super(ConnectionIngester,self).__init__(source,info) 
    def prepare(self):
        print("Prepare Connection")
        self.df = super(ConnectionIngester,self).generate_df()
        from_t = self.info.table.name
        from_e = self.emapper.get_mapping(self.info.table.name)
        to_t = self.info.foreign_table.name
        to_e = self.emapper.get_mapping(self.info.foreign_table.name)
        def cap_first(instr):
            if instr.startswith("_"):
                return  instr[1:].capitalize()
            else:
                return instr.capitalize()

        sql_resource_prefix = self.source.get_hash_prefix()
        def change_to_sha( prefix, value ):
            print(f"changing {prefix} {value}")
            resource_name = "{}/{}".format(prefix, value )
            return  hash_string(resource_name) 
        from_prefix = f"{sql_resource_prefix}/{from_t}"
        to_prefix = f"{sql_resource_prefix}/{to_t}"
        print(f"Connection hash from {from_prefix} to {to_prefix}")
        self.df.insert(0,"ConnectionClass",f"{cap_first(from_e)}to{cap_first(to_e)}")
        fc = self.info.prop_columns[0]
        tc = self.info.prop_columns[1]
        self.df[ fc ] =  self.df[ fc ].apply( lambda val: change_to_sha( from_prefix, val  ))
        self.df[ tc ] =  self.df[ tc ].apply( lambda val: change_to_sha( to_prefix, val ))
        self.df.rename( columns={ self.info.prop_columns[0]: f"{from_e}@wf_sha1_hash" } ,inplace=True)
        self.df.rename( columns={self.info.prop_columns[1]: f"{to_e}@wf_sha1_hash"}, inplace=True)

        ## TODO - handle missing fk *HERE* ( since we have the data in a df )
    def load(self,db):
        print("Ready to load")
        csv_data = SQLConnectionDataCSV( filename=None,df=self.df,
                sql_resource_prefix=self.source.get_table_hash_prefix(),
                primary_key_column_name=self.source.get_pk_col())
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "connection" if cnt == 1 else "connections"
        print(f"Finished uploading {cnt} {noun}")

        print(f"Columns: {self.df.columns}")
        print(self.df)
