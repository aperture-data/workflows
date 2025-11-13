import mlcroissant as mlc
from typer import Typer
import json
import pandas as pd
from tqdm import tqdm

from movie_record import make_movie_with_all_connections, create_indexes, DATASET_NAME
from movie_parser import MovieParser

from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import (
    create_connector,
    execute_query
)
from aperturedb.Utils import Utils

from embeddings import Embedder, DEFAULT_MODEL

# constants
CROISSANT_URL = "https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/croissant/download"

app = Typer()

def deserialize_record(record):
    deserialized = record.decode('utf-8') if isinstance(record, bytes) else record
    if isinstance(deserialized, str):
        try:
            deserialized = json.loads(deserialized)
        except:
            pass
    return deserialized

def cleanup_movies(db):
    """
    Cleanup the movies dataset from ApertureDB.
    """
    query = [
    {
        "DeleteEntity": {
            "constraints": {
                "dataset_name": ["==", DATASET_NAME]
            }
        }
    },
    {
        "DeleteDescriptorSet": {
            "constraints": {
                "dataset_name": ["==", DATASET_NAME]
            }
        }
    }
    ]
    execute_query(db, query=query)

@app.command()
def ingest_movies(ingest_posters: bool = False, embed_tagline: bool = False):
    """
    Ingest the movies dataset into ApertureDB.
    """
    # Fetch the Croissant JSON-LD
    croissant_dataset = mlc.Dataset(CROISSANT_URL)

    # Get record sets in the dataset
    record_sets = croissant_dataset.metadata.record_sets

    # Fetch the records and put them in a DataFrame. The archive, downloads, load into a DataFrame
    # is managed by the croissant library.
    # croisant recrds are ~ DataFrame. TMDB has 2 record sets
    # The first records are the movies, the second are the casts.
    # The association between the two is the movie_id
    record_set_df_0 = pd.DataFrame(croissant_dataset.records(record_set=record_sets[0].uuid))
    record_set_df_1 = pd.DataFrame(croissant_dataset.records(record_set=record_sets[1].uuid))

    # Merge the two DataFrames on the movie_id
    records = record_set_df_0.merge(
        record_set_df_1,
        right_on="tmdb_5000_movies.csv/id",
        left_on="tmdb_5000_credits.csv/movie_id")

    collection = []
    db = create_connector()
    cleanup_movies(db)

    descriptor_set = "wf_embeddings_clip"
    embedder = Embedder.from_new_descriptor_set(
        db, descriptor_set,
        provider="clip",
        model_name="ViT-B/16",
        properties={"type": "text", "source_type": "movie", "dataset_name": DATASET_NAME})
    for record in tqdm(records.iterrows()):
        columns = records.columns
        count = 0
        j = {}
        for c in columns:
            j[c] = deserialize_record(record[1][c])
        count += 1
        movie = make_movie_with_all_connections(j, embedder, ingest_posters, embed_tagline)
        collection.append(movie)



    parser = MovieParser(collection)

    utils = Utils(db)
    create_indexes(utils)
    loader = ParallelLoader(db)
    ParallelLoader.setSuccessStatus([0, 2])
    loader.ingest(parser, batchsize=10, numthreads=8, stats=True)


if __name__ == "__main__":
    app()

