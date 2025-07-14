#!/usr/bin/env python3

# This tool is an MCP stdio bridge that connects to a remote MCP server using streamable HTTP and bearer token authentication.
# It expects two command line arguments:
# 1. The URL of the MCP server to connect to, e.g. "http://localhost:8000/mcp/".
# 2. The bearer token for authentication, e.g. "my-secret-token".

# This bridge exists for two reasons:
# 1. While the MCP protocol now supports remote servers, actual implementations may not support it well.
# 2. Technically bearer token authentication is not supported in the MCP protocol, which requires OAuth.

# This might be configued with something like this in the platform's config (e.g. claude_desktop_config.json):
# {
#     "mcpServers": {
#         "aperturedb": {
#             "command": "python3",
#             "args": [
#                 "/path/to/stdio-bridge.py",
#                 "http://localhost:8000/mcp/",
#                 "secretsquirrel"
#             ]
#         }
#     }
# }

import sys
import json
import urllib.request
import urllib.error

session_id = None  # Assigned by server on first request


def info(msg):
    """Prints an informational message to stderr."""
    print(msg, file=sys.stderr, flush=True)


def extract_jsonrpc_from_sse(body):
    for line in body.splitlines():
        if line.startswith("data:"):
            info(f"Extracting JSON-RPC from SSE line: {line}")
            return json.loads(line[len("data:"):].strip())
    info("No data line found in SSE response, returning empty response")
    return None


def post_json(url, token, req_json):
    global session_id
    data = json.dumps(req_json).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {token}",
            "Content-Length": str(len(data)),
            **({"Mcp-session-id": session_id} if session_id else {})
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()
            response = extract_jsonrpc_from_sse(body)
            if "Mcp-session-id" in resp.headers:
                session_id = resp.headers["Mcp-session-id"]
                info(f"Session ID set to: {session_id}")
            return response
    except urllib.error.HTTPError as e:
        # If server returned an SSE error response, try to extract it
        if e.code == 400:
            body = e.read().decode()
            try:
                return extract_jsonrpc_from_sse(body)
            except Exception:
                return {
                    "jsonrpc": "2.0",
                    "id": req_json.get("id"),
                    "error": {
                        "code": -32000,
                        "message": f"HTTPError {e.code}: {e.reason}"
                    }
                }


def main():
    url, token = sys.argv[1:3]

    info(f"Connecting to {url}")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as e:
            print(json.dumps({
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": f"Invalid JSON: {e}"}
            }), flush=True)
            continue

        if "params" not in req or not isinstance(req["params"], dict):
            req["params"] = {}

        # inject `_meta.progressToken`
        if "_meta" not in req["params"]:
            req["params"]["_meta"] = {
                "progressToken": req.get("id")  # Claude uses id for tracking
            }

        info(f"req={req}")
        resp = post_json(url, token, req)
        info(f"resp={resp}")
        if resp is not None:
            print(json.dumps(resp), flush=True)


if __name__ == '__main__':
    main()
