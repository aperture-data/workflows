# Lightweight wrapper for local proxy service

import json
from typing import List, Optional
import logging
import base64
from .common import PROXY_SOCKET_PATH, UnixHTTPConnection

logger = logging.getLogger(__name__)


def _post_json(
    path: str,
    payload: dict,
    uds_path: str = PROXY_SOCKET_PATH,
) -> dict:
    body = json.dumps(payload).encode("utf-8")
    conn = UnixHTTPConnection(uds_path)
    try:
        conn.putrequest("POST", path)
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders()
        conn.send(body)

        resp = conn.getresponse()
        resp_status = resp.status
        data = resp.read()
    except Exception as e:
        logger.error(f"Error during HTTP request: {e}")
        raise
    finally:
        conn.close()

    if resp_status < 200 or resp_status >= 300:
        logger.error(
            f"HTTP error {resp_status} from embedding proxy: {data.decode('utf-8')}")
        raise RuntimeError(
            f"HTTP error {resp_status} from embedding proxy: {data.decode('utf-8')}"
        )

    return json.loads(data)


def embed_texts(
    texts: List[str],
    provider: str,
    model: str,
    corpus: str,
    uds_path: str = PROXY_SOCKET_PATH,
) -> List[bytes]:
    logger.debug(
        f"Embedding {len(texts)} texts using {provider}/{model}/{corpus}")
    payload = {
        "provider": provider,
        "model": model,
        "corpus": corpus,
        "texts": texts,
    }
    result = _post_json("/v2/embed/texts", payload, uds_path=uds_path)
    return [base64.b64decode(e) for e in result["embeddings"]]


def embed_images(
    images: List[bytes],
    provider: str,
    model: str,
    corpus: str,
    uds_path: str = PROXY_SOCKET_PATH,
) -> List[bytes]:
    logger.debug(
        f"Embedding {len(images)} images using {provider}/{model}/{corpus}")
    payload = {
        "provider": provider,
        "model": model,
        "corpus": corpus,
        "images": [base64.b64encode(i).decode("ascii") for i in images],
    }
    result = _post_json("/v2/embed/images", payload, uds_path=uds_path)
    return [base64.b64decode(e) for e in result["embeddings"]]
