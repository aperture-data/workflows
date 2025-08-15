# All access to the ApertureDB database should go through this module.
# It uses a unix socket connection to connect to a proxy server that handles
# authentication and pooling.
#
# The protocol is roughly based on the HTTP/REST protocol, but has no authentication, and supports a "status" field in the response.

import http.client
import socket
import json
import base64
import uuid
from typing import List, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, uds_path: str):
        super().__init__("localhost")  # host is ignored
        self.uds_path = uds_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.uds_path)


def _multipart(boundary: str, query: List[dict], blobs: Optional[List[bytes]]) -> bytes:
    sep = f"--{boundary}\r\n".encode()
    end = f"--{boundary}--\r\n".encode()
    body = bytearray()
    # query part
    body += sep
    body += b'Content-Disposition: form-data; name="query"\r\n'
    body += b'Content-Type: application/json\r\n\r\n'
    body += json.dumps(query).encode("utf-8") + b"\r\n"
    # blobs parts
    for i, b in enumerate(blobs or []):
        body += sep
        body += f'Content-Disposition: form-data; name="blobs"; filename="blob{i}.bin"\r\n'.encode(
        )
        body += b'Content-Type: application/octet-stream\r\n\r\n'
        body += b + b"\r\n"
    body += end
    return bytes(body)


def execute_query(
    json_query: List[dict],
    blobs: Optional[List[bytes]] = None,
    uds_path: str = "/tmp/aperturedb-proxy.sock",
) -> Tuple[int, List[dict], Optional[List[bytes]]]:
    start_time = datetime.now()
    logger.debug(
        f"Executing query: {json_query} with blobs: {len(blobs) if blobs else 0}")
    boundary = "----apdb-" + uuid.uuid4().hex
    body = _multipart(boundary, json_query, blobs)

    conn = UnixHTTPConnection(uds_path)
    try:
        conn.putrequest("POST", "/aperturedb")
        conn.putheader(
            "Content-Type", f"multipart/form-data; boundary={boundary}")
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders()
        conn.send(body)

        resp = conn.getresponse()
        resp_status = resp.status
        data = resp.read()
    finally:
        conn.close()

    if resp_status < 200 or resp_status >= 300:
        logger.error(
            f"HTTP error {resp_status} from ApertureDB proxy: {data.decode('utf-8')}")
        raise RuntimeError(
            f"HTTP error {resp_status} from ApertureDB proxy: {data.decode('utf-8')}"
        )

    parsed = json.loads(data)
    out_json = parsed["json"]
    out_blobs = [base64.b64decode(s) for s in parsed.get("blobs", [])] or None
    out_status = parsed.get('status', 0)
    elapsed_time = datetime.now() - start_time
    logger.debug(
        f"Query executed successfully in {elapsed_time.total_seconds()} seconds, status: {out_status}, json: {json.dumps(out_json)[:200]}{'...' if len(json.dumps(out_json))  > 200 else ''}, blobs: {len(out_blobs) if out_blobs else 0}")
    return out_status, out_json, out_blobs
