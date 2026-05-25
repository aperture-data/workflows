import logging

import typer

from images import FindImageQueryGenerator
from aperturedb import ParallelQuery
from connection_pool import ConnectionPool

app = typer.Typer()
CAPTION_IMAGE_PROPERTY = 'wf_caption_image'

@app.command()
def caption_images(
    num_workers: int = typer.Option(1, envvar="NUM_WORKERS", help="Number of concurrent workers"),
    batch_size: int = typer.Option(1, envvar="BATCH_SIZE", help="Batch size for fetching images"),
    log_level: str = typer.Option("WARNING", envvar=["WF_LOG_LEVEL", "LOG_LEVEL"], help="Logging level")
):
    num_workers = int(num_workers)

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
