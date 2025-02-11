import os
import argparse
import pandas as pd

from aperturedb import ImageDataCSV as ImageDataCSV
from aperturedb import ParallelLoader as ParallelLoader
from aperturedb import CommonLibrary
import boto3

def process_queue(queue, params):

    if len(queue) == 0:
        print("Empty queue.")
        return

    print("Processing queue...")
    df = pd.DataFrame()

    df["s3_url"] = queue
    df["wf_s3_url"] = "s3://" + params.bucket + "/" + df["s3_url"]
    df["constraint_wf_s3_url"] = "s3://" + params.bucket + "/" + df["s3_url"]
    df["s3_url"] = "s3://" + params.bucket + "/" + df["s3_url"]

    ofile = "output.images.csv"
    df.to_csv(ofile, index=False)

    data = ImageDataCSV.ImageDataCSV(ofile)
    loader = ParallelLoader.ParallelLoader(CommonLibrary.create_connector())
    loader.ingest(data, numthreads=32, stats=True)

    print("Done.")

def main(params):

    queue = []
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=params.bucket)
    for page in result:
        if "Contents" in page:
            for key in page[ "Contents" ]:
                keyString = key[ "Key" ]
                if keyString.endswith(".jpg"):
                    # print(keyString)
                    queue.append(keyString)
                    # print(len(queue))
                if len(queue) >= params.chunk_size:
                    process_queue(queue, params)
                    queue = []

    if len(queue) > 0:
        process_queue(queue, params)
        queue = []


def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('-limit',  type=int,
                     default=os.environ.get('LIMIT', 100))

    obj.add_argument('-chunk-size',  type=int,
                     default=os.environ.get('CHUNK_SIZE', 1000))

    obj.add_argument('-bucket',  type=str,
                     default=os.environ.get('BUCKET', "demo-workflows-ingest-from-s3"))

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
