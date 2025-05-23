from aperturedb.CommonLibrary import create_connector
from aperturedb.ParallelQuery import execute_batch
import pandas as pd
import os
import sys


count_queries = [
        [{
            "FindImage":{
                "constraints":{
                    "Blurry" : ["!=", None]
                },
                "results":{
                    "count": True
                }
            }
        }],
        [{
            "FindDescriptor":{
                "set": "ViT-B/16",
                "results": {
                    "count": True
                }
            }
        }],
        [{
            "FindDescriptor":{
                "set": "ViT-B/16",
                "results": {
                    "count": True
                }
            }
        }],
        [{
            "FindDescriptor":{
                "set": "facenet_pytorch_embeddings",
                "results": {
                    "count": True
                }
            }
        }],
        [{
            "FindDescriptor":{
                "set": "facenet_pytorch_embeddings",
                "results": {
                    "count": True
                }
            }
        }],
        [{
            "FindImage":{
                "constraints":{
                    "celebahq_id" : ["!=", None],
                    "Blurry" : ["==", None]
                },
                "results":{
                    "count": True
                }
            }
        }],
        [{
            "FindPolygon":{
                "constraints":{
                    "celebahqmask_id" : ["!=", None]
                },
                "results":{
                    "count": True
                }
            }
        }],
        [{
            "FindBoundingBox":{
                "constraints":{
                    "celebahqbbox_id" : ["!=", None]
                },
                "results":{
                    "count": True
                }
            }
        }]
    ]

INPUT_DIR = "/app/input/faces"

in_csvs = [
    "pruned_celebA.csv",
    "celebA.csv_clip_pytorch_embeddings_metadata.adb.csv",
    "celebA.csv_clip_pytorch_embeddings_connection.adb.csv",
    ["celebA.csv_facenet_pytorch_embeddings_metadata.adb.csv", "hqimages.adb.csv_facenet_pytorch_embeddings_metadata.adb.csv"],
    ["celebA.csv_facenet_pytorch_embeddings_connection.adb.csv", "hqimages.adb.csv_facenet_pytorch_embeddings_connection.adb.csv"],
    "hqimages.adb.csv",
    "hqpolygons.adb.csv",
    "hqbboxes.adb.csv"
]
db = create_connector()
results = []
for in_csv, cquery in zip(in_csvs, count_queries):
    if not isinstance(in_csv, list):
        df = pd.read_csv(os.path.join(INPUT_DIR, in_csv))
    else:
        df = pd.concat([pd.read_csv(os.path.join(INPUT_DIR, csv)) for csv in in_csv])

    r, resp, b = execute_batch(q=cquery, blobs=[], db=db)
    print(f"Executing query {cquery} for {in_csv}")
    exp_num = len(df)
    actual_num = resp[0][list(resp[0].keys())[0]]['count']
    print(f"Expected count [{in_csv}]: {exp_num}, Actual count: {actual_num}")
    results.append(exp_num == actual_num)

print(f"Results:{results}")
sys.exit(0 if all(results) else 1)
