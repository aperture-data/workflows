import os
import logging

from typer import Typer

from images import FindImageQueryGenerator
from aperturedb import ParallelQuery
from connection_pool import ConnectionPool

app = Typer()
CAPTION_IMAGE_PROPERTY = 'wf_caption_image'

@app.command()
def caption_images(
    num_workers:int = int(os.environ.get("NUM_WORKERS", 1)),
    batch_size:int = int(os.environ.get("BATCH_SIZE", 1)),
    log_level:str = os.environ.get("LOG_LEVEL", "WARNING")
):
    logging.basicConfig(level=log_level.upper(), force=True)
    pool = ConnectionPool()
    data = FindImageQueryGenerator(
        pool,
        batch_size=batch_size,
        caption_image_property=CAPTION_IMAGE_PROPERTY)

    print("Running Caption Image...")
    with pool.get_connection() as db:
        querier = ParallelQuery.ParallelQuery(db)
        querier.query(data, batchsize=batch_size, numthreads=num_workers, stats=True)


def main():

    app()

if __name__ == "__main__":
    main()