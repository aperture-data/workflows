#!/bin/env python3
# ingester.py - traverses a cloud bucket and finds matching items

import hashlib
import pandas as pd
import datetime as dt
import mimetypes
import os
import sys
from provider import Provider,CustomSources
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.VideoDataCSV import VideoDataCSV
from aperturedb.ParallelLoader import ParallelLoader
mimetypes.init()

IMAGE_TYPES_TO_LOAD = ['image/png', 'image/jpeg'] 
DOC_TYPES_TO_LOAD = ['application/pdf' ]
VIDEO_TYPES_TO_LOAD = ['video/mp4']

# Ingester - uses provider to retrieve list of files, creates dataframe,
#  and passes to appropriate CSVLoader
# This is the base class, each subclass configures the loader and filter.
class Ingester:
    # guess_types - don't download each file to determine the type - do this
    # based off extension.
    def __init__(self, source: Provider, guess_types=True , add_object_paths=False):
        self.guess_types = guess_types
        self.source = source
        self.dataframe = None
        self.add_object_paths = add_object_paths

    def prepare(self):
        raise NotImplementedError("Base Class")
    def load(self):
        raise NotImplementedError("Base Class")

    def generate_df(self, paths ):
        bucket = self.source.bucket_name()
        scheme = self.source.url_scheme_name()
        url_column_name = self.source.url_column_name()
        load_time = dt.datetime.now().isoformat()
        full = []
        def hash_string(string):
            return hashlib.sha1(string.encode('utf-8')).hexdigest()
        def set_mime_type(path):
            return mimetypes.guess_type(path)[0]
        objects = []
        for path in paths:
            url = "{}//{}/{}".format(scheme,bucket,path)
            full_path = "{}/{}".format(bucket, path) 
            object_hash = hash_string(url) 
            full.append([url, object_hash, load_time ])
            objects.append(full_path)
        df = pd.DataFrame(
                columns = [url_column_name,'wf_sha1_hash','wf_ingest_date'],
                data=full)
        df['wf_creator'] =  "bucket_ingestor" 
        df['wf_creator_key'] = hash_string( "{}/{}".format(scheme,bucket)) 
        df['constraint_wf_sha1_hash'] = df['wf_sha1_hash']
        df['adb_mime_type'] =df[ url_column_name].apply(set_mime_type)
        if self.add_object_paths:
            df['wf_object_path'] = objects
        return df


class ImageIngester(Ingester):
    def __init__(self, source: Provider, guess_types=True , add_object_paths=False):
        super(ImageIngester,self).__init__(source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            #print(f"Object = {bucket_path} type = {object_type}")
            if object_type in IMAGE_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        self.df = super(ImageIngester,self).generate_df(paths)
        print(self.df)
    def load(self,db):
        print("Ready to load")
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
        super(VideoIngester,self).__init__(source,guess_types)
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
        #self.sources = CustomSources(n_download_retries)
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
        super(DocumentIngester,self).__init__(source,guess_types)
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
        print(self.df)
    def load(self,db):
        print("Ready to load")
        csv_data = FixedBlobDataCSV( filename=None,df=self.df)
        csv_data.sources = CustomSources( self.source )
        loader = ParallelLoader(db)
        loader.ingest(csv_data, batchsize=100,
                numthreads=4)
        cnt = len(self.df.index)
        noun = "document" if cnt == 1 else "documents"
        print(f"Finished uploading {cnt} {noun}")


