from aperturedb_io import AperturedbIO
from wf_argparse import ArgumentParser
import logging

from text_embedder import TextEmbedder

logger = logging.getLogger(__name__)


def run_extraction_and_segmentation(input_id: str, output_id: str):
    logger.info(f"Starting text embeddings")

    extractor = TextExtractor(css_selector=css_selector, emit_full_text=True)
    segmenter = TextSegmenter()

    with AperturedbIO(crawl_id) as io:
        for doc in io.get_crawl_documents():
            logger.info(f"Processing {doc.url}")
            io.set_document(doc)
            try:
                text_blocks = []
                for block in extractor.extract_blocks(doc.blob, doc.content_type):
                    if isinstance(block, TextBlock):
                        text_blocks.append(block)  # defer for segmentation
                    elif isinstance(block, ImageBlock):
                        io.create_image_block(block)
                    elif isinstance(block, FullTextBlock):
                        io.create_full_text_block(block)

                # Segment text blocks
                for segment in segmenter.segment(text_blocks):
                    io.create_segment(segment)

            except Exception as e:
                logger.exception(f"Failed to process document {doc.url}: {e}")
    logger.info("Done.")


def main(args):
    logging.basicConfig(level=args.log_level, force=True)
    logger.info("Starting text embeddings")
    logger.info(f"Log level: {args.log_level}")
    logger.info(f"Input ID: {args.input}")
    logger.info(f"Output ID: {args.output}")
    run_text_embeddings(args.input, args.output)
    logger.info("Complete.")


def get_args():
    obj = ArgumentParser()

    obj.add_argument('--input',
                     required=True,
                     help='The text extraction id to use')

    obj.add_argument('--output',
                     required=True,
                     help='The text embedding id to use (default generate UUID)')

    obj.add_argument('--log-level',
                     help='Logging level, e.g. INFO, DEBUG',
                     choices=list(logging._nameToLevel.keys()),
                     default='INFO')

    params = obj.parse_args()

    if params.output is None:
        params.output = uuid4()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
