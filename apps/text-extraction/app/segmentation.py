from typing import Iterator
from schema import Segment, TextBlock
from tiktoken import encoding_for_model
from langchain.text_splitter import RecursiveCharacterTextSplitter
import logging

logger = logging.getLogger(__name__)


class TextSegmenter:
    def __init__(
        self,
        model_name: str = "gpt-3.5-turbo",  # Ensure pre-cached in Dockerfile
        max_tokens: int = 300,  # maximum number of tokens per segment
        overlap_tokens: int = 50  # approximate number of tokens to overlap between segments
    ):
        self.model_name = model_name
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.encoder = encoding_for_model(model_name)
        self.splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            chunk_size=max_tokens,
            chunk_overlap=0,  # Not a typo, overlap handled separately
            model_name=model_name,
        )

    def segment(self, blocks: Iterator[TextBlock]) -> Iterator[Segment]:
        buffer = []
        buffer_tokens = 0
        total_tokens = 0
        n_segments = 0

        for block in blocks:
            # TODO: Consider suppressing overlap for some kinds of block
            token_count = len(self.encoder.encode(block.text))
            total_tokens += token_count

            if token_count <= self.max_tokens:
                sub_blocks = [block]
            else:  # block is too long; split
                chunks = self.splitter.split_text(block.text)
                sub_blocks = [block.split(chunk) for chunk in chunks]

            # Now try merging short blocks into segments
            for sub_block in sub_blocks:
                sub_tokens = len(self.encoder.encode(sub_block.text))

                if buffer_tokens + sub_tokens > self.max_tokens and buffer:
                    segment_text = "\n\n".join(b.text for b in buffer)
                    yield Segment(
                        text=segment_text,
                        blocks=buffer.copy(),
                        total_tokens=buffer_tokens,
                    )
                    n_segments += 1

                    # handle overlap
                    overlap = []
                    tokens = 0
                    for b in reversed(buffer):
                        t = len(self.encoder.encode(b.text))
                        if tokens + t > self.overlap_tokens:
                            break
                        tokens += t
                        overlap.insert(0, b)

                    buffer = overlap
                    buffer_tokens = sum(
                        len(self.encoder.encode(b.text)) for b in buffer)

                buffer.append(sub_block)
                buffer_tokens += sub_tokens

        if buffer:  # Finally flush buffer
            segment_text = "\n\n".join(b.text for b in buffer)
            yield Segment(text=segment_text, blocks=buffer.copy(), total_tokens=buffer_tokens)
            n_segments += 1

        logger.info(
            f"Segmented {n_segments} segments with {total_tokens} tokens")
