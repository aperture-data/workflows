# MCP (Model Context Protocol)

This workflow make an ApertureDB instance accessible by an MCP platform, such as Claude or ChatGPT.

## Running in docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.cloud.aperturedata.io \
           -e DB_PASS=password \
           -e WF_LOG_LEVEL=INFO \
           -e WF_AUTH_TOKEN=secretsquirrel \
           -e WF_INPUT=mydescriptorset \
           aperturedata/workflows-mcp-server
```

Parameters: 
* **`LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default WARNING.
* **`WF_AUTH_TOKEN`**: Authorization bearer token to use in API
* **`WF_INPUT`**: Name of descriptorset to use

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Integration 

### MCP Inspector 

1. You can run the MCP inspector using something like:

```
docker run -it -p 6274:6274 -p 6277:6277 allfunc/mcp-inspector:0.14.0
```

2. And visit http://localhost:6274/
3. Select "Streamable HTTP" as the Transport Type.
4. Enter the server URL like:
```
https://workflowstesting.cloud.aperturedata.io/mcp/
```
Because of the way MCP Inspector uses a proxy, you will likely need a fully-qualified domain name.
5. Hit "Connect", and start exploring the tools, resources, and prompt.

### Claude Desktop 

While it is theorically possible to get Claude Desktop to use remote MCP servers, the more reliable path is to use a `stdio` bridge, especially as Claude has trouble with Authentication Bearer tokens.

1. Download [`stdio-bridge.py`](./stdio-bridge.py)
2. Find the Claude configuration directory (e.g. `~/Library/Application\ Support/Claude` on MacOS)
3. Create a file `claude_desktop_config.json` with contents like:
```
{
   "mcpServers": {
    "aperturedb": {
        "command": "python3",
        "args": [
            "/path/to/stdio-bridge.py",
            "http://<hostname>/mcp/",
            "<token>"
        ]
    }
   }
}
```

4. Start or restart Claude Desktop and ask it "What MCP servers do you have access to?"