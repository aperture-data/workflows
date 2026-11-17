from aperturedb.Constraints import predicate
from pathlib import Path
from typing import Optional

from typer import Typer, Option
import json
import pandas as pd
from tqdm import tqdm

from movie_record import make_movie_with_all_connections, create_indexes, DATASET_NAME
from movie_parser import MovieParser
from movie_dataset import load_movie_dataset

from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import (
    create_connector,
    execute_query
)
from aperturedb.Utils import Utils

from embeddings import Embedder, DEFAULT_MODEL

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
    """Delete this dataset's graph objects while preserving shared vector definitions."""
    status, response, _ = execute_query(db, [
        {"DeleteDescriptor": {"constraints": predicate("dataset_name", "==", DATASET_NAME)}},
        {"DeleteEntity": {"constraints": predicate("dataset_name", "==", DATASET_NAME)}},
        {"DeleteImage": {"constraints": predicate("dataset_name", "==", DATASET_NAME)}}])
    if status != 0:
        raise RuntimeError(f"Movie cleanup failed: {response}")


@app.command()
def ingest_movies(
        ingest_posters: bool = False,
        embed_tagline: bool = False,
        sample_count: int = -1,
        metadata_file: Optional[Path] = Option(
            None, "--metadata-file", envvar="CROISSANT_METADATA_FILE",
            exists=True, dir_okay=False, readable=True),
        archive_file: Optional[Path] = Option(
            None, "--archive-file", envvar="CROISSANT_ARCHIVE_FILE",
            exists=True, dir_okay=False, readable=True)):
    """
    Ingest the movies dataset into ApertureDB.
    """
    # Local inputs retain the original Croissant record and validation path.
    croissant_dataset = load_movie_dataset(metadata_file, archive_file)

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

    if sample_count > 0:
        records = records.head(sample_count)

    collection = []
    db = create_connector()
    try:
        cleanup_movies(db)

        descriptor_set = "wf_embeddings_clip"
        embedder = Embedder.from_new_descriptor_set(
            db, descriptor_set,
            provider="clip",
            model_name="ViT-B/16",
            properties={"type": "text", "source_type": "movie", "dataset_name": DATASET_NAME})
    finally:
        db.close()
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

    db = create_connector()
    try:
        utils = Utils(db)
        create_indexes(utils)
        loader = ParallelLoader(db)
        ParallelLoader.setSuccessStatus([0, 2])
        loader.ingest(parser, batchsize=10, numthreads=8, stats=True)
        if loader.error_counter:
            raise RuntimeError(
                f"Movie ingestion failed in {loader.error_counter} batches")
    finally:
        db.close()


if __name__ == "__main__":
    app()
