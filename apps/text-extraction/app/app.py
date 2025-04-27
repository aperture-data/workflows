from schema import TextBlock, ImageBlock, FullTextBlock, Segment
from aperturedb_io import AperturedbIO
from wf_argparse import ArgumentParser
import logging
from uuid import uuid4

from text_extractor import TextExtractor
from segmentation import TextSegmenter


logger = logging.getLogger(__name__)


def run_extraction_and_segmentation(args):
    crawl_spec_id = args.input
    logger.info(f"Starting text extraction for crawl: {crawl_spec_id}")

    css_selector = args.css_selector
    extractor = TextExtractor(css_selector=css_selector, emit_full_text=True)
    segmenter = TextSegmenter()

    spec_id = args.output
    run_id = str(uuid4())
    with AperturedbIO(crawl_spec_id, spec_id, run_id) as io:
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

        # TODO: Support incremental mode
        io.ensure_output_does_not_exist()

        io.create_spec()

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
    logger.info("Starting text extraction and segmentation")
    logger.info(f"Parameters: {args}")

    run_extraction_and_segmentation(args)
    logger.info("Complete.")


def get_args():
    obj = ArgumentParser()

    obj.add_argument('--input', '--crawl',
                     help='The crawl spec id to use')

    obj.add_argument('--output',
                     help='The segmentation spec id to use. Defaults to a generated uuid',
                     default=str(uuid4()))

    obj.add_argument('--clean',
                     type=bool,
                     help='Delete existing spec before creating a new one',
                     default=False)

    obj.add_argument('--delete',
                     type=bool,
                     help='Delete the spec and all its segments; don\'t run segmentation',
                     default=False)

    obj.add_argument('--delete-all',
                     help='Delete all specs; don\'t run segmentation',
                     default=False)

    obj.add_argument('--css-selector',
                     help='CSS selector to use for text extraction, e.g. DIV#main-content')

    obj.add_argument('--log-level',
                     help='Logging level, e.g. INFO, DEBUG',
                     choices=list(logging._nameToLevel.keys()),
                     default='INFO')

    params = obj.parse_args()

    assert params.input or params.delete_all or params.delete, \
        "Must specify --input or --delete or --delete-all"

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
