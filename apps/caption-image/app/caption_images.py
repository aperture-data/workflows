import logging

import typer

from images import FindImageQueryGenerator
from aperturedb import ParallelQuery
from connection_pool import ConnectionPool

CAPTION_IMAGE_PROPERTY = 'wf_caption_image'

def caption_images(
    num_workers: int = typer.Option(1, envvar="NUM_WORKERS", help="Number of concurrent workers"),
    batch_size: int = typer.Option(1, envvar="BATCH_SIZE", help="Batch size for fetching images"),
    log_level: str = typer.Option("WARNING", envvar=["WF_LOG_LEVEL", "LOG_LEVEL"], help="Logging level")
):
    num_workers = int(num_workers)
    if num_workers <= 0:
        raise ValueError("num_workers must be > 0")

    batch_size = int(batch_size)
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    logging.basicConfig(level=log_level.upper(), force=True)
    logger = logging.getLogger(__name__)
    pool = ConnectionPool()
    data = FindImageQueryGenerator(
        pool,
        batch_size=batch_size,
        caption_image_property=CAPTION_IMAGE_PROPERTY)

    logger.info("Running Caption Image...")
    with pool.get_connection() as db:
        querier = ParallelQuery.ParallelQuery(db)
        querier.query(data, batchsize=1, numthreads=num_workers, stats=True)


def main():
    typer.run(caption_images)

if __name__ == "__main__":
    main()
