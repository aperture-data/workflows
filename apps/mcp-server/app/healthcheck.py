#!/usr/bin/env python3
"""
Health check script for MCP server.

This script verifies that the MCP server is running and responding to requests
by using the fastmcp.Client to perform a proper MCP protocol handshake.

Exit codes:
  0 - Server is healthy and responding
  1 - Server is not responding or returned an error
"""

import sys
import asyncio
from fastmcp import Client


async def main():
    """
    Check if the MCP server is healthy using the MCP protocol.
    """
    url = "http://localhost:8000/mcp/"
    timeout = 2
    
    try:
        # Use the fastmcp.Client to connect and list tools
        # This performs a proper MCP protocol handshake
        async with Client(url, timeout=timeout) as client:
            # Try to list tools - this verifies the server is responding properly
            await client.list_tools()
            # If we got here, the server is healthy
            return 0
        
    except asyncio.TimeoutError:
        print(f"Connection to {url} timed out after {timeout}s", file=sys.stderr)
        return 1
        
    except Exception as e:
        # Connection or other errors
        print(f"Health check failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # Run async main and exit with its return code
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

