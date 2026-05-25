import logging

import typer

from images import FindImageQueryGenerator
from aperturedb import ParallelQuery
from connection_pool import ConnectionPool

app = typer.Typer()
CAPTION_IMAGE_PROPERTY = 'wf_caption_image'

@app.command()
def caption_images(
    num_workers: int = typer.Option(None, envvar="NUM_WORKERS", help="Number of concurrent workers"),
    batch_size: int = typer.Option(None, envvar="BATCH_SIZE", help="Batch size for fetching images"),
    log_level: str = typer.Option("WARNING", envvar=["WF_LOG_LEVEL", "LOG_LEVEL"], help="Logging level")
):
    if num_workers is None:
        num_workers = 1
    else:
        num_workers = int(num_workers)

    if batch_size is None:
        batch_size = 1
    else:
        batch_size = int(batch_size)

    logging.basicConfig(level=log_level.upper(), force=True)
    pool = ConnectionPool()
    data = FindImageQueryGenerator(
        pool,
        batch_size=batch_size,
        caption_image_property=CAPTION_IMAGE_PROPERTY)

    print("Running Caption Image...")
    with pool.get_connection() as db:
        querier = ParallelQuery.ParallelQuery(db)
        querier.query(data, batchsize=1, numthreads=num_workers, stats=True)


def main():

    app()

if __name__ == "__main__":
    main()
