"""Run with the built app image, network disabled, and a read-only root filesystem."""
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch


def cache_state(cache):
    files = sorted(path for path in cache.rglob('*') if path.is_file())
    if not files:
        raise AssertionError('The built application has no tokenizer assets')
    for path in [cache, *(path for path in cache.rglob('*') if path.is_dir())]:
        if path.stat().st_mode & 0o555 != 0o555:
            raise AssertionError('Tokenizer cache directories must be readable and searchable')
    state = {}
    for path in files:
        if path.stat().st_mode & 0o444 != 0o444 or not os.access(path, os.R_OK):
            raise AssertionError('Tokenizer assets must be readable by the runtime user')
        state[str(path.relative_to(cache))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return state


def main():
    cache = Path(os.environ.get('TIKTOKEN_CACHE_DIR', ''))
    if cache != Path('/opt/tiktoken-cache'):
        raise AssertionError('The application must select its persistent tokenizer cache')
    before = cache_state(cache)
    # Fail even if the external container network isolation was omitted.
    with patch('requests.sessions.Session.request',
               side_effect=AssertionError('Tokenizer contract attempted a network request')):
        from text_extraction.segmentation import TextSegmenter
        from text_extraction.schema import TextBlock
        segmenter = TextSegmenter(max_tokens=100, overlap_tokens=10)
        text = 'Generated text verifies offline tokenizer initialization and segmentation. ' * 40
        encoded = segmenter.encoder.encode(text)
        if not encoded or segmenter.encoder.decode(encoded) != text:
            raise AssertionError('Tokenizer did not round-trip generated text')
        segments = list(segmenter.segment([TextBlock(text=text)]))
        if not segments or any(not segment.text for segment in segments):
            raise AssertionError('Actual TextSegmenter produced no usable segments')
    if cache_state(cache) != before:
        raise AssertionError('Tokenizer initialization modified the built cache')
    print(json.dumps({'state': 'offline_tokenizer_contract_passed',
                      'cache': str(cache), 'cache_files': len(before),
                      'cache_sha256': before, 'cache_unchanged': True,
                      'token_count': len(encoded), 'segment_count': len(segments),
                      'uid': os.getuid(), 'network_requests_allowed': False,
                      'model_or_database_used': False}, sort_keys=True))


if __name__ == '__main__':
    main()
