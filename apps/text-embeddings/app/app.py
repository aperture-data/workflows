from aperturedb_io import AperturedbIO
from wf_argparse import ArgumentParser
import logging

from embeddings import Embedder, DEFAULT_MODEL
from schema import Embedding
from uuid import uuid4

logger = logging.getLogger(__name__)


def run_text_embeddings(args):
    logger.info(f"Starting text embeddings")
    segmentation_spec_id = args.input
    embedding_id = args.output

    embedder = Embedder.from_string(model_spec=args.model)

    input_spec_id = args.input
    spec_id = args.output
    run_id = str(uuid4())
    descriptorset_name = args.descriptorset
    engine = args.engine
    with AperturedbIO(
        input_spec_id=input_spec_id,
        spec_id=spec_id,
        run_id=run_id,
        descriptorset_name=descriptorset_name,
        engine=engine,
        embedder=embedder,
    ) as io:
        if args.delete_all:
            io.delete_all()
            return

        if args.delete:
            io.delete_spec(spec_id)
            return
        io.ensure_input_exists()
        if args.clean:
            io.delete_spec(spec_id)
            # continue
        io.ensure_output_does_not_exist()
        io.create_spec()
        io.create_descriptorset()

        for segment in io.get_segments():
            try:
                logger.debug(f"Processing segment {segment.id}")
                # Embed the segment text
                v = embedder.embed_text(segment.text)
                embedding = Embedding(
                    segment_id=segment.id,
                    url=segment.url,
                    text=segment.text,
                    title=segment.title,
                    vector=v,
                )
                io.create_embedding(embedding)
            except Exception as e:
                logger.error(f"Error processing segment {segment.id}: {e}")
                # continue

    logger.info("Done.")


def main(args):
    logging.basicConfig(level=args.log_level, force=True)
    logger.info("Starting text embeddings")
    logger.info(f"Log level: {args.log_level}")
    logger.info(f"Input ID: {args.input}")
    logger.info(f"Output ID: {args.output}")
    run_text_embeddings(args)
    logger.info("Complete.")


def get_args():
    obj = ArgumentParser()

    obj.add_argument('--input',
                     help='The segmentation spec id to use')

    obj.add_argument('--output',
                     help='The text embedding id to use (default generate UUID)')

    obj.add_argument('--model',
                     help='The embedding model to use, of the form "backend model pretrained',
                     default=DEFAULT_MODEL)

    obj.add_argument('--engine',
                     help='The embedding engine to use',
                     default="HNSW")

    obj.add_argument('--clean',
                     type=bool,
                     help='Delete existing spec before creating a new one',
                     default=False)

    obj.add_argument('--delete',
                     type=bool,
                     help='Delete the spec and all its embeddings; don\'t run embedding job',
                     default=False)

    obj.add_argument('--delete-all',
                     help='Delete all specs; don\'t run embedding job',
                     default=False)

    obj.add_argument('--log-level',
                     help='Logging level, e.g. INFO, DEBUG',
                     choices=list(logging._nameToLevel.keys()),
                     default='INFO')

    obj.add_argument('--descriptorset',
                     help='The descriptor set to use for the embeddings; defaults to output name')

    params = obj.parse_args()

    if params.output is None:
        params.output = uuid4()

    if params.descriptorset is None:
        params.descriptorset = params.output

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
