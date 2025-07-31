# ingest.py - for ingest from SQL

import logging
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ImageDataCSV import ImageDataProcessor,ImageDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.Sources import Sources
from aperturedb.Query import ObjectType
import pandas as pd
from utils import hash_string,TableSpec,ConnectionSpec
from sqlalchemy import URL,create_engine,Connection,MetaData,select
import copy
import datetime as dt
import magic

from aperturedb.ParallelLoader import ParallelLoader

logger = logging.getLogger(__name__)

class EntityMapper:
    """
    Provider interface between Ingestors for data translation and sharing
    """
    def __init__(self):
        self.map = {}
        self.requests = {}
        self.results = {}

    def add_mapping(self, table_name, entity_name):
        self.map[table_name]=entity_name

    def get_mapping(self, table_name):
        if not table_name in self.map:
            logger.warning(f"{table_name} not in entity map!")
        return self.map[table_name]
    def set_column_request(self, table_name, column_name):
        if table_name not in self.requests:
            self.requests[table_name] = set()
        
        self.requests[table_name].add(column_name)
    def fulfill_requests(self, table_name, df):
        if table_name in self.requests:
            if table_name not in self.results:
                self.results[table_name] = {}
            self.results[table_name] = df[ list(self.requests[table_name]) ].copy()

    def get_request_data(self, table_name, column_name):
        return self.results[table_name][column_name]


class SQLBaseDataCSV():
    """
    SQLBaseDataCSV can't be based on EntityDataCSV, because our Binary data classes 
     can't use Entity's getitem, but our EntityData can.
     So we have a single function which takes a core getitem and applies our
     common SQL ingest actions.

     Note that this requires `self.command` - so it expects to be in an object
     that is also an EntityDataCSV.
    """
    def __init__(self, sql_resource_prefix, primary_key_column_name, **kwargs):
        logger.info(f"SQLBaseDataCSV: {sql_resource_prefix} // {primary_key_column_name}")
        self.sql_resource_prefix = sql_resource_prefix
        self.primary_key_column_name = primary_key_column_name

    def injected_getitem(self, idx, base_getitem):
        query,blobs = base_getitem(self,idx)
        idx = self.df.index.start + idx
        resource_name = "{}/{}".format(self.sql_resource_prefix,
                self.df.loc[idx,self.primary_key_column_name])
        object_hash = hash_string(resource_name) 

        props = query[0][self.command]["properties"]
        props["wf_sha1_hash"] = object_hash
        query[0][self.command]["if_not_found"]= { "wf_sha1_hash" : ["==", object_hash ]}

        logger.info(f"SQL Object {resource_name} -> {object_hash}")
        logger.debug("Query = {query}")
        return query,blobs


class SQLEntityDataCSV(SQLBaseDataCSV, EntityDataCSV):
    """
    Creates a CSV for a table which is an Entity.
    """
    def __init__(self, filename:str, sql_resource_prefix, primary_key_column_name, **kwargs):
        EntityDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)

    def getitem(self, idx):
        return self.injected_getitem( idx, EntityDataCSV.getitem )


class SQLConnectionDataCSV(SQLBaseDataCSV, ConnectionDataCSV):
    """
    Creates a CSV for a relationship between two tables.
    """
    def __init__(self, filename:str, sql_resource_prefix, primary_key_column_name, **kwargs):
        ConnectionDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)

    def getitem(self, idx):
        query,blobs = super().getitem(idx)
        def query_hash(q):
            return q[list(q.keys())[0]]['constraints']['wf_sha1_hash'][1]
        hash1 = query_hash( query[0] )
        hash2 = query_hash( query[1] )
        logger.debug(f"Connecting {hash1} and {hash2}")
        object_hash = hash_string(f"{hash1}{hash2}") 
        props = query[2]["AddConnection"]["properties"]
        query[2]["AddConnection"]["if_not_found"] = { "wf_sha1_hash":
                ["==",object_hash]}
        props["wf_sha1_hash"] = object_hash
        return query,blobs

class SQLBinaryDataCSV(SQLBaseDataCSV, EntityDataCSV, ImageDataProcessor):
    """
    Creates a CSV for a table which has binary data associated.

    Can be Image or Blob.

    The binary data is injected into entity loading, either from SQL or from a url.
    binary_lookup_file is internal type - rows should be ordered exactly the
    same as input file.  expects first column to be:
      - url = get data from this urls
      - db_{name}  get data from connection,  name is the column to select for lookup
    """
    def __init__(self, filename:str, binary_df:str, sql_resource_prefix,
            primary_key_column_name, **kwargs):
        EntityDataCSV.__init__(self,filename,**kwargs)
        SQLBaseDataCSV.__init__(self, sql_resource_prefix,primary_key_column_name)
        ImageDataProcessor.__init__( self, check_image=False, n_download_retries=3)

        self.blob_df = binary_df 

        self.column_name = "data"
        self.command = "AddBlob" 

    def set_image_mode(self, is_image_mode):
        self.command = "AddImage" if is_image_mode else "AddBlob"

    def get_indices(self):
        entity_type = "_Blob" if self.command == "AddBlob" else "_Image"
        return {
            "entity": {
                entity_type: [ "wf_sha1_hash"] 
            }
        }

    def getitem_binary(self, idx):
        custom_fields = {}
        blobs = []
        q = []
        ai = self._basic_command(idx, custom_fields)
        ai[self.command]["_ref"] = 1
        q.append(ai)

        return q, blobs

    def getitem(self, idx):
        query,blobs = self.injected_getitem(idx,SQLBinaryDataCSV.getitem_binary) 

        idx = self.df.index.start + idx

        blob = self.blob_df.loc[idx,self.column_name]
        query[0][self.command]["properties"]["adb_mime_type"] = magic.from_buffer(blob,mime=True)

        if blobs is None:
            blobs = []
        blobs.insert(0,blob)
        return query,blobs

    def validate(self):
        pass


class Provider:
    """
    for eventual merging with load from bucket
    """
    pass

class SQLProvider(Provider):
    """
    Wraps access to SQL database.
    """
    def __init__(self, host:str, user:str, password:str, database:str,
            port:int=None, table:str=None):
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
        return None if len(self.info.bin_columns) == 0 else self.info.bin_columns[0]

    def get_url_col(self):
        return None if len(self.info.url_columns) == 0 else self.info.url_columns[0]

    def get_data(self):
        meta = MetaData()
        meta.reflect(bind=self.engine,only=[self.table])
        t = meta.sorted_tables[0]
        to_select=[]
        all_ret_cols = self.info.prop_columns + self.info.bin_columns + self.info.url_columns 
        logger.debug(f"SQLProvider.get_data: {all_ret_cols}")
        available_cols = { c.name:c for c in t.columns }
        for c in all_ret_cols:
            if c in available_cols.keys(): 
                to_select.append(available_cols[c])
            else:
                raise Exception(f"SQLProvider.get_data: Tried to get data for {c}, but wasn't in table for {t.name} on server?")
        stmt = select(*to_select)
        data=[]
        with self.engine.connect() as conn:
            logger.debug(f"SQLProvider:{stmt}")
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
            logger.error(f"Failed to connect to SQL server:{e}")
            return False


class Ingester:
    """
    Wraps interfacing between a provider and a adb csv parser
    """
    def __init__(self,  source: Provider, info:TableSpec): 
        self.multiple_objects = None
        self.source = source.as_table(info)
        self.dataframe = None
        self.workflow_id = None
        self.info = info
        self.types_added = None

    def set_workflow_id(self, id):
        self.workflow_id = id

    def set_entity_mapper(self, m):
        self.emapper = m

    def get_entity_mapper(self):
        return self.emapper

    def get_types_added(self):
        return [] if self.types_added is None else self.types_added

    def prepare(self):
        raise NotImplementedError("Base Class")

    def load(self):
        raise NotImplementedError("Base Class")

    def load_urls(self, df):
        sources=Sources(3)
        pk = self.source.get_pk_col()
        url_col = self.source.get_url_col()
        new_data = []
        for idx,row in df.iterrows():
            url = row[url_col]
            binary_data = sources.load_from_http_url(url, lambda buf: True) 

            new_data.append([row[pk],binary_data[1]])

        return pd.DataFrame(columns=[pk,url_col],data=new_data)

    def process_requests(self):
        """
        notify entity mapper of column data requests.
        """
        # default has no requests.
        pass

    def generate_df(self ):
        """
        Creates the basic data by reading from the source. Injects some workflow data.
        """
        table_name = self.source.table_name()
        scheme = "sql://{}/{} ".format(
                self.source.host_name() , self.source.database_name())
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
        if self.workflow_id:
            df['wf_workflow_id'] = self.workflow_id

        return df


class EntityIngester(Ingester):
    def __init__(self, source: Provider, info:TableSpec): 
        super(EntityIngester,self).__init__(source,info) 

    def prepare(self):
        logger.info("Prepare Entity")
        self.df = super(EntityIngester,self).generate_df()
        # these are data requests for mostly unmanipulated stuff, so
        # want to hand them off first.
        self.emapper.fulfill_requests( self.source.table_name(), self.df )
        logger.debug("Entity DataFrame input")
        logger.debug(f"Columns: {self.df.columns}")
        logger.debug(self.df)
        self.emapper.add_mapping( self.source.table_name(),
                self.info.name)

    def load(self, db):
        logger.info("Ready to load entities")
        ename = self.emapper.get_mapping(self.info.table.name)
        self.df.insert(0,"EntityClass", ename)
        logger.debug("Entity DataFrame output")
        logger.debug(self.df)
        csv_data = SQLEntityDataCSV( filename=None,df=self.df,
                primary_key_column_name=self.source.get_pk_col(),
                sql_resource_prefix=self.source.get_table_hash_prefix())
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "entity" if cnt == 1 else "entities"
        logger.info(f"Finished uploading {cnt} {noun}")

        self.types_added = [self.info.name]

class PDFIngester(Ingester):
    """
    Ingests PDFs into ApertureDB
    """
    def __init__(self, source: Provider, info:TableSpec): 
        super(PDFIngester,self).__init__(source,info) 

    def prepare(self):
        self.df = super(PDFIngester,self).generate_df()
        # these are data requests for mostly unmanipulated stuff, so
        # want to hand them off first.
        self.emapper.fulfill_requests( self.source.table_name(), self.df )
        logger.debug("PDF DataFrame input")
        logger.debug(self.df)
        drop_col=None
        if not self.source.get_bin_col():
            self.binary_df = self.load_urls( self.df )
            drop_col = self.source.get_url_col()
        else:
            self.binary_df = \
                self.df[[self.source.get_pk_col(),self.source.get_bin_col()]].copy()
            drop_col = self.source.get_bin_col()
        logger.debug(self.df.columns)
        self.df.drop(columns=[drop_col],inplace=True)
        self.df["document_type"] = "pdf"

        logger.debug("PDF DataFrame output")
        logger.debug(self.df)
        logger.debug(self.binary_df)
        self.binary_df.rename(columns={drop_col:"data"},inplace=True)
        self.emapper.add_mapping( self.source.table_name(),
                "_Blob")

    def load(self, db):
        logger.info("Ready to load PDFs")
        csv_data = SQLBinaryDataCSV(
                filename=None,sql_resource_prefix=self.source.get_table_hash_prefix(),
                primary_key_column_name=self.source.get_pk_col(),
                df=self.df,binary_df=self.binary_df)
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "blob" if cnt == 1 else "blobs"
        logger.info(f"Finished uploading {cnt} {noun}")
        self.types_added = [ObjectType.BLOB.value]

class ImageIngester(Ingester):
    """
    Ingests Images into ApertureDB
    """
    def __init__(self, source: Provider, info:TableSpec): 
        super(ImageIngester,self).__init__(source,info) 

    def prepare(self):
        self.df = super(ImageIngester,self).generate_df()
        # these are data requests for mostly unmanipulated stuff, so
        # want to hand them off first.
        self.emapper.fulfill_requests( self.source.table_name(), self.df )
        logger.debug("Image DataFrame input")
        logger.debug(self.df)
        drop_col=None
        if not self.source.get_bin_col():
            self.binary_df = self.load_urls( self.df )
            drop_col = self.source.get_url_col()
        else:
            self.binary_df = \
                self.df[[self.source.get_pk_col(),self.source.get_bin_col()]].copy()
            drop_col = self.source.get_bin_col()
        logger.debug(self.df.columns)
        self.df.drop(columns=[drop_col],inplace=True)

        logger.debug("Image DataFrame output")
        logger.debug(self.df)
        logger.debug(self.binary_df)
        self.binary_df.rename(columns={drop_col:"data"},inplace=True)
        self.emapper.add_mapping( self.source.table_name(),
                "_Image")

    def load(self, db):
        logger.info("Ready to load images")
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
        logger.info(f"Finished uploading {cnt} {noun}")
        self.types_added = [ObjectType.IMAGE.value]


class ConnectionIngester(Ingester):
    """
    Ingests Table Connections into ApertureDB
    """
    def __init__(self, source: Provider, info:ConnectionSpec): 
        super(ConnectionIngester,self).__init__(source,info) 

    def process_requests(self):
        # we need the pk column from target table to potentially filter any
        # values that are missing.
        self.emapper.set_column_request(self.info.target_table.name,
                self.info.target_col )

    def prepare(self):
        logger.info("Prepare Connection")
        self.df = super(ConnectionIngester,self).generate_df()

        # we know that since connections are last that our requests will be
        # done.
        fk_vals = self.emapper.get_request_data( self.info.target_table.name,
                self.info.target_col ).to_frame()
        fk_vals["remote_side"] ="True"
        logging.info(f"Connection: mapping {self.info.target_col}:{self.info.source_link_col}")
        #  data from foreign table comes in with it's name.
        #  we rename it to be the 
        fk_vals.rename(columns={ self.info.target_col:self.info.source_link_col},inplace=True)
        merged = self.df.merge(fk_vals,on=self.info.source_link_col,indicator=True,how='left')
        existing= merged[merged["_merge"] != "both" ]
        missing= merged[merged["_merge"] == "left_only" ]
        if missing.shape[0] != 0:
            logging.warning(f"Had {missing.shape[0]} missing.")
            for i,r in missing.iterrows():
                logging.warning("For {} = {}, {} = {}: No Match.".format( 
                        self.info.primary_key, r[self.info.primary_key],
                        self.info.source_link_col, r[self.info.source_link_col]))
            merged = merged[ merged["_merge"] == "both" ]
            self.df = merged.drop("_merge",axis=1).reset_index(drop=True)


        from_tbl = self.info.table.name
        from_ele = self.emapper.get_mapping(self.info.table.name)
        to_tbl = self.info.target_table.name
        to_ele = self.emapper.get_mapping(self.info.target_table.name)
        def cap_first(instr):
            if instr.startswith("_"):
                return  instr[1:].capitalize()
            else:
                return instr.capitalize()

        sql_resource_prefix = self.source.get_hash_prefix()
        def change_to_sha( prefix, value ):
            logger.info(f"connection creating sha for \"{prefix}/{value}\"")
            resource_name = "{}/{}".format(prefix, value )
            return  hash_string(resource_name) 
        from_prefix = f"{sql_resource_prefix}/{from_tbl}"
        to_prefix = f"{sql_resource_prefix}/{to_tbl}"
        logger.info(f"Connection hash from {from_prefix} to {to_prefix}")
        self.df.insert(0,"ConnectionClass",f"{cap_first(from_ele)}to{cap_first(to_ele)}")
        from_col = self.info.primary_key
        to_col = self.info.source_link_col 
        self.df[ from_col ] =  self.df[ from_col ].apply( lambda val: change_to_sha( from_prefix, val  ))
        self.df[ to_col ] =  self.df[ to_col ].apply( lambda val: change_to_sha( to_prefix, val ))
        self.df.rename( columns={ from_col: f"{from_ele}@wf_sha1_hash" } ,inplace=True)
        self.df.rename( columns={ to_col: f"{to_ele}@wf_sha1_hash"}, inplace=True)
        logger.debug("Connection DataFrame output")
        logger.debug(f"Columns: {self.df.columns}")
        logger.debug(self.df)

    def load(self, db):
        logger.info("Ready to load connection")
        csv_data = SQLConnectionDataCSV( filename=None,df=self.df,
                sql_resource_prefix=self.source.get_table_hash_prefix(),
                primary_key_column_name=self.source.get_pk_col())
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "connection" if cnt == 1 else "connections"
        logger.info(f"Finished uploading {cnt} {noun}")

        self.types_added = [ObjectType.CONNECTION.value]
