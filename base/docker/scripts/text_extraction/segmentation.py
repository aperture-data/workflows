from typing import Iterator
from .schema import Segment, TextBlock
from tiktoken import encoding_for_model
from langchain_text_splitters.character import RecursiveCharacterTextSplitter
import logging

logger = logging.getLogger(__name__)


class TextSegmenter:
    """Segment text into chunks suitable for LLM processing.
    Short blocks are combined together.
    Overly-long blocks are split into smaller segments.
    This class uses the tiktoken encoder to count tokens and
    the langchain RecursiveCharacterTextSplitter to split text.
    Successive segments overlap by a specified number of tokens.
    The overlap is not exact, but approximate.

    TODO: max_tokens and overlap_tokens should be configurable
    """

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

    def _token_count(self, text):
        """We use the encoder to count tokens.
        Here we're using the OpenAI tokenizer, but it's not critical,
        and does not tie us to OpenAI.
        """
        return len(self.encoder.encode(text))

    def segment(self, blocks: Iterator[TextBlock]) -> Iterator[Segment]:
        """Turn the sequence of text blocks into segments"""
        buffer = []
        buffer_tokens = 0
        total_tokens = 0
        n_segments = 0
        title = None

        for block in blocks:
            if not title:
                title = block.title

            # TODO: Consider suppressing overlap for some kinds of block
            token_count = self._token_count(block.text)
            total_tokens += token_count

            # If the block is short enough, add it to the buffer
            # Otherwise, split it into smaller blocks
            # and add those to the buffer
            if token_count <= self.max_tokens:
                sub_blocks = [block]
            else:  # block is too long; split
                chunks = self.splitter.split_text(block.text)
                sub_blocks = [block.split(chunk) for chunk in chunks]

            # Now try merging short blocks into segments
            for sub_block in sub_blocks:
                sub_tokens = self._token_count(sub_block.text)

                # If the new block is too long, flush the buffer
                # by emitting a new segment
                if buffer_tokens + sub_tokens > self.max_tokens and buffer:
                    segment_text = "\n\n".join(b.text for b in buffer)
                    yield Segment(
                        text=segment_text,
                        blocks=buffer.copy(),
                        total_tokens=buffer_tokens,
                        title=title
                    )
                    n_segments += 1

                    # Now extract just the next overlap from the buffer
                    overlap = []
                    tokens = 0
                    for b in reversed(buffer):
                        t = self._token_count(b.text)
                        if tokens + t > self.overlap_tokens:
                            break
                        tokens += t
                        overlap.insert(0, b)

                    # Set buffer to be just the overlap
                    buffer = overlap
                    buffer_tokens = sum(
                        self._token_count(b.text) for b in buffer)

                # Add the new block to the buffer
                buffer.append(sub_block)
                buffer_tokens += sub_tokens

        if buffer:  # Finally flush buffer by emitting one more segment
            segment_text = "\n\n".join(b.text for b in buffer)
            yield Segment(text=segment_text, blocks=buffer.copy(), total_tokens=buffer_tokens, title=title)
            n_segments += 1

        logger.info(
            f"Segmented {n_segments} segments with {total_tokens} tokens")
