#!/bin/env python3
# ingester.py - traverses a cloud bucket and finds matching items

import pandas as pd
import datetime as dt
import mimetypes
import os
import sys
import logging
from typing import Iterator, Optional, Tuple

import utils
from provider import Provider,CustomSources

from aperturedb.Query import ObjectType
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.VideoDataCSV import VideoDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import execute_query

mimetypes.init()

logger = logging.getLogger(__name__)
IMAGE_TYPES_TO_LOAD = ['image/png', 'image/jpeg'] 
DOC_TYPES_TO_LOAD = ['application/pdf' ]
VIDEO_TYPES_TO_LOAD = ['video/mp4']

class MergeData:
    def __init__(self,properties_df:pd.DataFrame, all_blobs=True, error_missing = False):
        self.properties_df = properties_df
        self.all_blobs = all_blobs
        self.error_missing = error_missing

# Ingester - uses provider to retrieve list of files, creates dataframe,
#  and passes to appropriate CSVLoader
# This is the base class, each subclass configures the loader and filter.
class Ingester:
    # guess_types - don't download each file to determine the type - do this
    # based off extension.
    def __init__(self,object_type: ObjectType,  source: Provider, guess_types=True , add_object_paths=False):
        self.object_type = object_type
        self.multiple_objects = None
        self.guess_types = guess_types
        self.source = source
        self.dataframe = None
        self.add_object_paths = add_object_paths
        self.merge_data = None
        self.workflow_id = None
        self.check_for_existing = False
        self.maximum_object_count = None

    def get_object_types(self):
        return self.multiple_objects if self.multiple_objects else [ self.object_type]
    def set_add_object_paths(self,add_object_paths:bool ):
        self.add_object_paths = add_object_paths

    def set_merge_data(self,merge_data: MergeData):
        self.merge_data = merge_data
    def set_workflow_id(self,id):
        self.workflow_id = id
    def set_check_for_existing(self,check):
        self.check_for_existing = check
    def set_maximum_object_count(self,count):
        self.maximum_object_count = count

    def prepare(self):
        raise NotImplementedError("Base Class")
    def load(self):
        raise NotImplementedError("Base Class")
    def execute_query(self,db,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        """Execute a query on ApertureDB and return the results

        TODO: Support mock
        """
        status, results, result_blobs = execute_query(
            client=db,
            query=query,
            blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
        )
        return results, result_blobs

    def find_existing(self,db,hashes):

        cnt = len(hashes)
        chunk=500
        real_type="None"
        logger.info(f"Finding Existing with list of {cnt}")
        found_hashes = []
        for t in self.get_object_types():
            real_type = "Blob" if t == "_Document" else t.value[1:]
            for i in range(0,cnt,chunk):
                end = (i+chunk if i + chunk <  cnt else cnt)
                q = [{f"Find{real_type}": {
                        "constraints": {
                            "wf_sha1_hash": ["in" , hashes[i:end]]
                            },
                        "results": {
                            "list": ["wf_sha1_hash"]
                        }
                    }
                }]
                #print(q)
                r,_ = self.execute_query(db,q)
                if isinstance(r,list):
                    ents = r[0][f"Find{real_type}"]["entities"]
                    found_hashes.extend( [ e["wf_sha1_hash"] for e in ents ] ) 

        existing = pd.DataFrame( found_hashes , columns=["wf_sha1_hash"])

        merged = self.df.merge(existing,on="wf_sha1_hash",indicator=True,how='left')
        exist_count =  merged[ merged["_merge"] != "left_only" ].shape[0]
        missing_count =  merged[ merged["_merge"] == "left_only" ].shape[0]
        logger.info(f"exist count = {exist_count} missing_count = {missing_count}")
        df = merged[ merged["_merge"] == "left_only" ]
        return df.drop("_merge",axis=1)

    def generate_df(self, paths ):
        bucket = self.source.bucket_name()
        scheme = self.source.url_scheme_name()
        url_column_name = self.source.url_column_name()
        load_time = dt.datetime.now().isoformat()
        full = []
        def set_mime_type(path):
            return mimetypes.guess_type(path)[0]
        objects = []
        for path in paths:
            url = "{}//{}/{}".format(scheme,bucket,path)
            full_path = "{}/{}".format(bucket, path) 
            object_hash = utils.hash_string(url) 
            full.append([path,url, object_hash, load_time ])
            objects.append(full_path)
        df = pd.DataFrame(
                columns = ['filename',url_column_name,'wf_sha1_hash','wf_ingest_date'],
                data=full)
        df['wf_creator'] =  "bucket_ingestor" 
        df['wf_creator_key'] = utils.generate_bucket_hash( scheme,bucket) 
        df['constraint_wf_sha1_hash'] = df['wf_sha1_hash']
        df['adb_mime_type'] =df[ url_column_name].apply(set_mime_type)
        if self.add_object_paths:
            df['wf_object_path'] = objects
        if self.workflow_id:
            df['wf_workflow_id'] = self.workflow_id


        if self.merge_data is not None:
            print("Merging properties in..")
            merged = df.merge( self.merge_data.properties_df, on='filename',
                    how='left' if self.merge_data.all_blobs else 'inner' )
            if self.merge_data.error_missing:
                # missing blob items.
                if len(merged.index) != len(df.index):
                    raise Exception('Error in missing, blob items lost on merge ( missing properties )')
                if len(self.merge_data.index) != len(merged.index):
                    raise Exception('Error in missing, property items lost on merge ( missing blobs )')
            df = merged
                
        df.drop('filename',inplace=True,axis=1)
        return df


class ImageIngester(Ingester):
    def __init__(self, source: Provider, guess_types=True , add_object_paths=False):
        super(ImageIngester,self).__init__(ObjectType.IMAGE,source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            #print(f"Object = {bucket_path} type = {object_type}")
            if object_type in IMAGE_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        self.df = super(ImageIngester,self).generate_df(paths)
        #print(self.df)
    def load(self,db):
        logger.info("Images ready to load")
        if self.maximum_object_count is not None:
            self.df = self.df.head( self.maximum_object_count )
        self.df = self.find_existing(db, self.df["wf_sha1_hash"].tolist())
        if len(self.df.index) == 0:
            logger.warning("No items to load after checking db for matches")
            return True
        logger.warning(f"{len(self.df.index)}???")
        return True
        csv_data = ImageDataCSV( filename=None,df=self.df)
        csv_data.sources = CustomSources( self.source )
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "image" if cnt == 1 else "images"
        print(f"Finished uploading {cnt} {noun}")


class VideoIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(VideoIngester,self).__init__(ObjectType.VIDEO,source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            #print(f"Object = {bucket_path} type = {object_type}")
            if object_type in VIDEO_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        self.df = super(VideoIngester,self).generate_df(paths)
        print(self.df)
    def load(self,db):
        print("Ready to load")
        csv_data = VideoDataCSV( filename=None,df=self.df)
        csv_data.sources = CustomSources( self.source )
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "video" if cnt == 1 else "videos"
        print(f"Finished uploading {cnt} {noun}")


HEADER_PATH = "filename"
HEADER_URL = "url"
HEADER_S3_URL = "s3_url"
HEADER_GS_URL = "gs_url"

class FixedBlobDataCSV(BlobDataCSV):
    def __init__(self, filename: str, check_image: bool = True, n_download_retries: int = 3, **kwargs):


        self.loaders = [self.load_blob, self.load_url,
                        self.load_s3_url, self.load_gs_url]
        self.source_types = [HEADER_PATH,
                             HEADER_URL, HEADER_S3_URL, HEADER_GS_URL]

        self.sources = None
        self.source_loader = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }

        BlobDataCSV.__init__(self, filename, **kwargs)
        self.source_type = self.header[0]
        self.check_buffer = lambda buf: True


    def getitem(self, idx):
        idx = self.df.index.start + idx

        image_path = os.path.join(
            self.relative_path_prefix, self.df.loc[idx, self.source_type])

        blob_ok, blob = self.source_loader[self.source_type](image_path)
        if not blob_ok:
            logger.error("Error loading blob: " + filename)
            raise Exception("Error loading blob: " + filename)

        q = []
        ab = self._basic_command(idx)
        q.append(ab)

        return q, [blob]


    def load_url(self, url):
        return self.sources.load_from_http_url(url, self.check_buffer)

    def load_s3_url(self, s3_url):
        return self.sources.load_from_s3_url(s3_url, self.check_buffer)

    def load_gs_url(self, gs_url):
        return self.sources.load_from_gs_url(gs_url, self.check_buffer)


    def validate(self):

        self.header = list(self.df.columns.values)
    
        if self.header[0] not in self.source_types:
            field = self.header[0]
            allowed = ", ".join(self.source_types)
            raise Exception(
                f"Error with CSV file field: {field}. Allowed values: {allowed}")

class DocumentIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(DocumentIngester,self).__init__("_Document",source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            #print(f"Object = {bucket_path} type = {object_type}")
            if object_type in DOC_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        self.df = super(DocumentIngester,self).generate_df(paths)
        self.df['document_type'] = "pdf"
    def load(self,db):
        print("Ready to load")
        if self.check_for_existing:
            existing = self.find_existing(db, self.df['wf_sha1_hash'].tolist() )
            merged = self.df.merge(existing,on="wf_sha1_hash",indicator=True,how='left')
            print(merged)
            exist_count =  merged[ merged["_merge"] != "left_only" ].shape[0]
            missing_count =  merged[ merged["_merge"] == "left_only" ].shape[0]
            print(f"exist count = {exist_count} missing_count = {missing_count}")
            self.df = merged[ merged["_merge"] != "left_only" ]
            self.df.drop("_merge",inplace=True,axis=1)
            if len(self.df.index) == 0:
                logger.warning("No items to load after checking db for matches")
                return True
        return True
        csv_data = FixedBlobDataCSV( filename=None,df=self.df)
        csv_data.sources = CustomSources( self.source )
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "document" if cnt == 1 else "documents"
        print(f"Finished uploading {cnt} {noun}")


class EntityIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(EntityIngester,self).__init__("_Entity",source,guess_types)
        self.entities = []
        self.merges = {}
        self.sources = CustomSources(self.source)
        self.known_objects = list( filter(lambda x: x != "_Entity",
            [m.value for m in ObjectType])) + [ "_Document", "_Pdf" ]

        self.handled_types = [ "_Image","_Video","_Pdf","_Document" ]
    def prepare(self):
        import pathlib 
        def object_filter(bucket_path):
            return bucket_path.endswith(".adb.csv")
        paths = self.source.scan( object_filter )
        for path in map( pathlib.Path, paths):
            prefix = path.name[:-1 * len(".adb.csv")]
            known_type = None
            for obj in self.known_objects:
                if prefix == obj.lower() or prefix == obj \
                    or prefix == obj[1:].lower() or prefix == obj[1:] :
                    known_type = obj

            # ensure it isn't polygon or _Polygon.
            if known_type and not known_type in self.handled_types:
                    print(f"Skipping {path}, it is a known object type"\
                    f" ({known_type}) that we don't support.")
                    continue

            from io import BytesIO
            bucket = self.source.bucket_name()
            scheme = self.source.url_scheme_name()
            url = "{}//{}/{}".format(scheme,bucket,path)
            df = None
            print(f"Loading url {url} for properties for {known_type}")
            with BytesIO( self.sources.load_object( url )) as csv_fd:
                df = pd.read_csv( csv_fd )
            if not known_type:
                if df.columns[0] != 'EntityClass':
                    raise Exception("First Column in Entity CSV was not 'EntityClass'")
                self.entities.append(df)
                if not self.multiple_objects:
                    self.multiple_objects = []
                self.multiple_objects.append( list(df['EntityClass'].unique()))
            else:
                if known_type in ["_Pdf","_Document"]:
                    known_type = "_Document"
                    self.merges[known_type] = df


        if self.multiple_objects:
            self.multiple_objects = list(set( self.multiple_objects))
    def get_merge(self,object_type):
        print(f"GM {object_type} in {self.merges.keys()}")
        if object_type in self.merges:
            return self.merges[object_type]
        return None

    def load(self,db):
        print("Ready to load")
        if len(self.entities) == 0:
            if len(self.merges) > 0:
                logger.info("All Entities used as properties")
            else:
                logger.warning("No Entities were found to be loaded")
        return
        csv_data = FixedBlobDataCSV( filename=None,df=self.df)
        csv_data.sources = CustomSources( self.source )
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "document" if cnt == 1 else "documents"
        print(f"Finished uploading {cnt} {noun}")
