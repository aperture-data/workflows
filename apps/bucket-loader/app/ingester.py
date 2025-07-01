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
    def __init__(self, source: Provider,  guess_types=True):
        self.guess_types = guess_types
        self.source = source
        self.dataframe = None

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
        for path in paths:
            url = "{}//{}/{}".format(scheme,bucket,path)
            full_path = "{}/{}".format(bucket, path) 
            object_hash = hash_string(url) 
            full.append([url, object_hash, load_time ])
        df = pd.DataFrame(
                columns = [url_column_name,'wf_sha1_hash','wf_ingest_date'],
                data=full)
        df['wf_creator'] =  "bucket_ingestor" 
        df['wf_creator_key'] = hash_string( "{}/{}".format(scheme,bucket)) 
        df['constraint_wf_sha1_hash'] = df['wf_sha1_hash']
        return df


class ImageIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(ImageIngester,self).__init__(source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            print(f"Object = {bucket_path} type = {object_type}")
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

class VideoIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(VideotIngester,self).__init__(source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            print(f"Object = {bucket_path} type = {object_type}")
            if object_type in VIDEO_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        df = super(VideotIngester,self).generate_df(paths)
        print(df)

class DocumentIngester(Ingester):
    def __init__(self, source:Provider, guess_types=True):
        super(DocumentIngester,self).__init__(source,guess_types)
    def prepare(self):
        def object_filter(bucket_path):
            object_type = mimetypes.guess_type(bucket_path)[0]
            print(f"Object = {bucket_path} type = {object_type}")
            if object_type in DOC_TYPES_TO_LOAD:
                return True
            return False
        paths = self.source.scan( object_filter )
        df = super(DocumentIngester,self).generate_df(paths)
        print(df)


