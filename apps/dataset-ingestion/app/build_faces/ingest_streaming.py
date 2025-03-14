import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from queue import Full, Queue

import pandas as pd
import requests
from aperturedb.CommonLibrary import create_connector
from aperturedb.QueryGenerator import QueryGenerator
from typer import Typer
from aperturedb.transformers import common_properties, image_properties


class HTTPStorageURLS():
    def __init__(self, q: Queue, df: pd.DataFrame, executor: ThreadPoolExecutor):
        self.executor = executor
        self.q = q
        self.df = df
        self.row = self.df.iloc[0]
        self.session = requests.Session()
        self.sync()

    def sync(self):
        def download_blob(i, row):
            url = row["url"]
            r = self.session.get(url)
            try:
                self.q.put((row, r.content))
            except Full:
                print("Queue is full")
                time.sleep(1)

        for i, row in enumerate(self.df.to_dict("records")):
            self.executor.submit(download_blob, i, row)
        print(f"Synced to {self.q}")

class GoogleCloudStorage():
    def __init__(self, q: Queue, df: pd.DataFrame, executor: ThreadPoolExecutor):
        self.executor = executor
        self.q = q
        self.df = df
        self.row = self.df.iloc[0]
        gs_url = self.row["gs_url"]
        from google.cloud import storage
        self.client = storage.Client.create_anonymous_client()
        self.source_bucket_name = gs_url.split("/")[2]
        self.source_bucket = self.client.bucket(self.source_bucket_name)
        self.sync()

    def sync(self):
        def download_blob(i, row):
            object_name = row["gs_url"].split("gs://" + self.source_bucket_name + "/")[-1]
            blob = self.source_bucket.blob(object_name).download_as_bytes()
            try:
                self.q.put((row, blob))
            except Full:
                print("Queue is full")
                time.sleep(1)

        for i, row in enumerate(self.df.to_dict("records")):
            self.executor.submit(download_blob, i, row)
        print(f"Synced to {self.q}")

class ObjectStorage(Enum):
    GCS = 1
    HTTP = 2

class Sequence(QueryGenerator):
    def __init__(self, input_csv: str):
        super().__init__()
        self.q = Queue(maxsize=1000)


        self.df = pd.read_csv(input_csv)
        url_type = self.df.columns[0]
        if url_type == "gs_url":
            self.storage = ObjectStorage.GCS
        elif url_type == "url":
            self.storage = ObjectStorage.HTTP
        else:
            raise ValueError("Invalid URL type")
        self.executor = ThreadPoolExecutor(max_workers=64)
        if self.storage == ObjectStorage.GCS:
            self.gcs = GoogleCloudStorage(self.q, self.df, self.executor)
        elif self.storage == ObjectStorage.HTTP:
            self.gcs = HTTPStorageURLS(self.q, self.df, self.executor)
        # Hack to reuse extra 7 (5 for PQ+2 for transformers) items on top of the queue
        # which are used to check if generator has implemented getitem
        # And what is commands per query, and blobs per query.
        self.inspect = 0

    def __del__(self):
        self.executor.shutdown()

    def getitem(self, subscript):
        data = self.q.get()
        if self.inspect < 7:
            self.q.put(data)
            self.inspect += 1
        q = [
            {
                "AddImage": {
                    "properties": data[0]
                }
            }
        ]
        return q, [data[1]]

    def __len__(self):
        return len(self.df)


app = Typer()
@app.command()
def ingest(input_csv: str, batch_size: int, num_workers: int):
    s = Sequence(input_csv)
    client = create_connector()
    from aperturedb.ParallelLoader import ParallelLoader
    loader = ParallelLoader(client=client)
    s = common_properties.CommonProperties(s)
    s = image_properties.ImageProperties(s)
    loader.ingest(s, batch_size, num_workers, True)
    print("Done")


if __name__ == "__main__":
    app()