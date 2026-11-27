# Aperture Nexus

This workflow hosts [aperture-nexus](https://github.com/aperture-data/aperture-nexus)
as shared long-term memory for AI agents. Agents store and recall memories over
MCP. An administrator manages agents in a web portal.

Everything is served under `/nexus/` on port 8000:

| Path | Purpose |
|------|---------|
| `/nexus/admin/` | Admin portal: add, remove and rotate agents; connection instructions |
| `/nexus/mcp/` | MCP endpoint (streamable HTTP) for agents |
| `/nexus/health` | Readiness check |

## Agents and memory

Each agent is a Nexus principal with its own token. The token is shown once, when
the agent is added or its token is rotated. Every MCP request is authenticated, so
removing or rotating an agent takes effect at once. Removing an agent keeps the
memories it stored. Its ID stays reserved while any sessions, contexts or
commits owned by that ID remain. Rotate the token to keep using an existing
agent's identity and memories.

Agents recall memories stored by any agent in their own organization, and change
or delete only memories they stored themselves.

| Tool | What it does |
|------|--------------|
| `remember` | Store text under a session, with an optional purpose and metadata |
| `recall` | Search memories by meaning, or list them by session, agent or department |
| `find_sessions` | Find earlier sessions by the purpose of their work |
| `get_context` | Read everything stored under one context |
| `link` | Record how one of the agent's contexts relates to another context |
| `forget` | Delete a commit, context or session the agent stored |

Text is embedded with `BAAI/bge-small-en-v1.5`, which is part of the image.
Use custom metadata for application fields such as a ticket number. Nexus
reserves storage and response fields, including organization, department,
session and ownership identifiers.

## Admin portal

In ApertureDB Cloud, the workflow's **Connect** button opens the portal already
signed in. The button carries the `WF_ADMIN_TOKEN` that Cloud generated for the
workflow. The portal exchanges it for a session cookie that lasts four hours.

Elsewhere, open `/nexus/admin/` and sign in with the value of `WF_ADMIN_TOKEN`.

The portal has four sections:

* **Agents**: add agents, rotate their tokens and remove them.
* **Memories**: search what agents stored in every organization, by meaning or by
  agent, session, organization and department. Browse sessions, and open a context
  to see who stored it, why, its commits and its links to other contexts. Delete a
  commit, a context or a whole session.
* **Usage**: totals, and sessions, contexts and memories per agent with their last
  activity. A memory is one `remember` call.
* **Connect**: the MCP URL and client setup.

## Connecting an agent

Add the agent in the portal, then use its token.

Claude Code:
```
claude mcp add --transport http nexus https://<host>/nexus/mcp/ --header "Authorization: Bearer <agent token>"
```

Claude Desktop: use the [`stdio-bridge.py`](../mcp-server/stdio-bridge.py) from the
MCP Server workflow with the URL `https://<host>/nexus/mcp/` and the agent token,
as described in the [MCP Server README](../mcp-server/README.md#claude-desktop).

## Running in docker

Export `WF_ADMIN_TOKEN` with a secret you generate before starting Nexus with
Docker or the repository's Compose file. The admin portal requires this token.

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.cloud.aperturedata.io \
           -e DB_PASS="password" \
           -e WF_ADMIN_TOKEN \
           -p 8000:8000 \
           aperturedata/workflows-nexus:dev
```

Parameters:
* **`WF_ADMIN_TOKEN`**: Token that signs the administrator in to the portal. Required.
* **`WF_LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR or CRITICAL. Default WARNING.

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Database details

Nexus stores its data in these classes:

* Entities `NexusUser` (agents), `NexusSession`, `NexusContext` and `NexusCommit`.
* Blobs of class `NexusBlob` holding the stored text.
* Descriptor classes `nexus_text__BAAI/bge-small-en-v1.5` and
  `nexus_context__BAAI/bge-small-en-v1.5` for memories and context purposes.
* Connections such as `nexus_session_context`, `nexus_context_commit` and `nexus_link`.

## Nexus source

`nexus/` is a copy of the aperture-nexus package. `nexus/SOURCE_COMMIT` records
the commit it came from. To update it, run `./sync-nexus.sh [checkout] [revision]`.
The image installs it together with the SDK wheel from the base image.

## Cleaning up

Removing agents keeps their memories. To delete all Nexus data, delete the classes
listed under [Database details](#database-details).
