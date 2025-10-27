import mlcroissant as mlc
from typer import Typer
import json
import pandas as pd
from tqdm import tqdm

from movie_record import make_movie_with_all_connections
from movie_parser import MovieParser

from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import (
    create_connector
)

app = Typer()

def deserialize_record(record):
    deserialized = record.decode('utf-8') if isinstance(record, bytes) else record
    if isinstance(deserialized, str):
        try:
            deserialized = json.loads(deserialized)
        except:
            pass
    return deserialized

@app.command()
def ingest_movies():
    # Fetch the Croissant JSON-LD
    croissant_dataset = mlc.Dataset('https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/croissant/download')

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
    for record in tqdm(records.iterrows()):
        columns = records.columns
        count = 0
        j = {}
        for c in columns:
            j[c] = deserialize_record(record[1][c])
        count += 1
        movie = make_movie_with_all_connections(j)
        collection.append(movie)

    parser = MovieParser(collection)
    db = create_connector()
    loader = ParallelLoader(db)
    ParallelLoader.setSuccessStatus([0, 2])
    loader.ingest(parser, batchsize=100, numthreads=8, stats=True)


if __name__ == "__main__":
    app()

