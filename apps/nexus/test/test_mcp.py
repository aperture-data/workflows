"""MCP endpoint: authentication, memory tools and organization scoping."""

import pytest
from fastmcp.exceptions import ToolError

from conftest import call, initialize, mcp_client, unique

FACT = "The API rate limiter uses a token bucket per user, stored in Redis."


@pytest.fixture(scope="module")
def agents(portal):
    """Two agents in one organization and one in another."""
    org = unique("acme")
    created = {
        "alice": unique("alice"),
        "bob": unique("bob"),
        "carol": unique("carol"),
    }
    tokens = {
        "alice": portal.add_agent(created["alice"], organization=org, department="platform"),
        "bob": portal.add_agent(created["bob"], organization=org, department="support"),
        "carol": portal.add_agent(created["carol"], organization=unique("other")),
    }
    yield {"ids": created, "tokens": tokens}
    for agent_id in created.values():
        portal.remove(agent_id)


def test_requests_need_a_valid_agent_token():
    responses = [initialize(), initialize(authorization="Bearer"),
                 initialize(authorization="Basic YWxpY2U6c2VjcmV0"),
                 initialize("not-a-token"), initialize("nexus.YWxpY2U.wrong")]
    for response in responses:
        assert response.status_code == 401
        assert response.headers["www-authenticate"].startswith("Bearer")


async def test_tools_are_listed(agents):
    async with mcp_client(agents["tokens"]["alice"]) as client:
        names = {tool.name for tool in await client.list_tools()}
    assert names == {"remember", "recall", "find_sessions", "get_context", "link", "forget"}


async def test_remember_then_recall_by_meaning(agents):
    alice = agents["tokens"]["alice"]
    session = unique("rate-limiter")
    stored = await call(alice, "remember", text=FACT, session=session,
                        purpose="Design the API rate limiter", metadata={"ticket": "API-42"})
    assert stored["session"] == session

    found = await call(alice, "recall", query="how do we throttle API requests?", session=session)
    assert found and found[0]["text"] == FACT
    assert found[0]["agent"] == agents["ids"]["alice"]
    assert found[0]["context_id"] == stored["context_id"]
    assert found[0]["ticket"] == "API-42"

    listed = await call(alice, "recall", session=session)
    assert [m["text"] for m in listed] == [FACT]


@pytest.mark.parametrize("field", ["organization", "department", "entry_id", "score"])
async def test_metadata_accepts_only_application_fields(agents, field):
    with pytest.raises(ToolError, match="reserved by Nexus"):
        await call(agents["tokens"]["alice"], "remember", text=FACT,
                   session=unique("metadata"), metadata={field: "example"})


async def test_recall_is_limited_to_the_organization(agents):
    alice, bob, carol = (agents["tokens"][n] for n in ("alice", "bob", "carol"))
    session = unique("shared")
    await call(alice, "remember", text=FACT, session=session)

    assert [m["text"] for m in await call(bob, "recall", query="rate limiter", session=session)] == [FACT]
    assert await call(carol, "recall", query="rate limiter", session=session) == []


async def test_only_the_author_can_forget(agents):
    alice, bob = agents["tokens"]["alice"], agents["tokens"]["bob"]
    session = unique("forget")
    stored = await call(alice, "remember", text="Temporary note to delete.", session=session)

    for arguments in ({"commit_id": stored["commit_id"]}, {"context_id": stored["context_id"]},
                      {"session": session}):
        with pytest.raises(ToolError):
            await call(bob, "forget", **arguments)
    assert await call(bob, "recall", session=session)

    with pytest.raises(ToolError):
        await call(alice, "forget", commit_id=stored["commit_id"], session=session)
    await call(alice, "forget", commit_id=stored["commit_id"])
    assert await call(alice, "recall", session=session) == []


async def test_forget_a_whole_session(agents):
    alice, bob = (agents["tokens"][n] for n in ("alice", "bob"))
    session = unique("cleanup")
    await call(alice, "remember", text="First note.", session=session)
    await call(alice, "remember", text="Second note.", session=session)
    await call(bob, "remember", text="Bob's note.", session=session)
    assert len(await call(alice, "recall", session=session, agent=agents["ids"]["alice"])) == 2

    await call(alice, "forget", session=session)
    assert [m["text"] for m in await call(alice, "recall", session=session)] == ["Bob's note."]
    await call(bob, "forget", session=session)


async def test_get_context_and_link(agents):
    alice, bob, carol = (agents["tokens"][n] for n in ("alice", "bob", "carol"))
    session = unique("contexts")
    first = await call(alice, "remember", text="Chose Redis for the limiter store.",
                       session=session, purpose="Pick the limiter store")
    second = await call(alice, "remember", text="Added a Lua script for atomic refills.",
                        session=session, purpose="Implement the limiter")

    context = await call(bob, "get_context", context_id=first["context_id"])
    assert context["agent"] == agents["ids"]["alice"]
    assert context["purpose"] == "Pick the limiter store"
    assert [e["text"] for e in context["entries"]] == ["Chose Redis for the limiter store."]
    with pytest.raises(ToolError):
        await call(carol, "get_context", context_id=first["context_id"])

    linked = await call(alice, "link", source_context_id=second["context_id"],
                        target_context_id=first["context_id"], relationship="follows")
    assert linked["relationship"] == "follows"
    with pytest.raises(ToolError):
        await call(bob, "link", source_context_id=second["context_id"],
                   target_context_id=first["context_id"], relationship="follows")


async def test_find_sessions_by_purpose(agents):
    alice, carol = agents["tokens"]["alice"], agents["tokens"]["carol"]
    session = unique("billing-migration")
    stored = await call(alice, "remember", text="Invoices move to the new ledger in March.",
                        session=session, purpose="Migrate billing to the new ledger service")

    sessions = await call(alice, "find_sessions", query="billing ledger migration")
    match = next(s for s in sessions if s["session"] == session)
    assert match["agent"] == agents["ids"]["alice"]
    assert match["contexts"][0]["context_id"] == stored["context_id"]
    assert all(s["session"] != session for s in await call(carol, "find_sessions",
                                                           query="billing ledger migration"))
