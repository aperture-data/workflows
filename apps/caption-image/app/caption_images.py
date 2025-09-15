import logging

from typer import Typer

from images import FindImageQueryGenerator
from aperturedb import ParallelQuery
from connection_pool import ConnectionPool

app = Typer()
DONE_PROPERTY = 'wf_caption_image'

@app.command()
def caption_images(
    num_workers:int = 1,
    batch_size:int = 1,
    log_level:str = "INFO"
):
    logging.basicConfig(level=logging.getLevelName(log_level))
    pool = ConnectionPool()
    data = FindImageQueryGenerator(
        pool,
        done_property=DONE_PROPERTY)

    print("Running Caption Image...")
    with pool.get_connection() as db:
        querier = ParallelQuery.ParallelQuery(db)
        querier.query(data, batchsize=batch_size, numthreads=num_workers, stats=True)


def main():

    app()

if __name__ == "__main__":
    main()