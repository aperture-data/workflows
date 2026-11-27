"""Admin portal: browsing, searching, tracing and deleting memories, and usage."""

import asyncio
import re

import httpx
import pytest

from conftest import ADMIN, BASE_URL, call, unique

FIRST = "Chose Postgres logical replication for the reporting copy."
SECOND = "Reporting replica lag stays under five seconds at peak load."


@pytest.fixture(scope="module")
def stored(portal):
    """One agent with two linked memories in one session."""
    agent_id, organization, session = unique("reporter"), unique("globex"), unique("reporting-db")
    token = portal.add_agent(agent_id, organization=organization)
    first = asyncio.run(call(token, "remember", text=FIRST, session=session,
                             purpose="Choose how to feed the reporting database"))
    second = asyncio.run(call(token, "remember", text=SECOND, session=session,
                              purpose="Measure replica lag", metadata={"ticket": "OPS-7"}))
    asyncio.run(call(token, "link", source_context_id=second["context_id"],
                     target_context_id=first["context_id"], relationship="follows"))
    yield {"agent_id": agent_id, "token": token, "organization": organization,
           "session": session, "first": first, "second": second}
    portal.remove(agent_id)


def get(portal, path, **params) -> httpx.Response:
    response = portal.http.get(f"{ADMIN}{path}", params=params)
    assert response.status_code == 200, response.text
    return response


def usage_row(portal, agent_id) -> dict:
    page = get(portal, "/usage").text
    row = re.search(rf'data-agent="{re.escape(agent_id)}">(.*?)</tr>', page, re.S).group(1)
    return {name: int(re.search(rf"data-{name}>(\d+)<", row).group(1))
            for name in ("sessions", "contexts", "memories")}


def test_pages_require_sign_in():
    client = httpx.Client(base_url=BASE_URL, follow_redirects=False, timeout=60)
    for path in ("/memories", "/session?id=x", "/context?id=x", "/usage"):
        assert client.get(f"{ADMIN}{path}").status_code == 401, path
    assert client.post(f"{ADMIN}/memories/delete", data={"kind": "session", "id": "x"}).status_code == 403


async def test_search_by_meaning_across_organizations(portal, stored):
    page = get(portal, "/memories", q="how is the reporting database kept in sync?",
               organization=stored["organization"]).text
    assert FIRST in page
    assert stored["agent_id"] in page


async def test_browse_sessions_and_trace_lineage(portal, stored):
    assert stored["session"] in get(portal, "/memories", agent=stored["agent_id"]).text

    session = get(portal, "/session", id=stored["first"]["session_id"]).text
    assert FIRST in session and SECOND in session
    assert "Measure replica lag" in session

    second = get(portal, "/context", id=stored["second"]["context_id"]).text
    assert SECOND in second
    assert "ticket: OPS-7" in second
    assert stored["second"]["commit_id"] in second
    assert re.search(r"This context <strong>follows</strong>\s*<a href=\"[^\"]*"
                     + re.escape(stored["first"]["context_id"]), second)

    first = get(portal, "/context", id=stored["first"]["context_id"]).text
    assert stored["second"]["context_id"] in first
    assert "<strong>follows</strong> this context" in first


async def test_usage_counts_each_agent(portal, stored):
    assert usage_row(portal, stored["agent_id"]) == {"sessions": 1, "contexts": 2, "memories": 2}
    assert "Never" not in re.search(rf'data-agent="{re.escape(stored["agent_id"])}">(.*?)</tr>',
                                    get(portal, "/usage").text, re.S).group(1)


def test_missing_objects_are_not_found(portal):
    assert portal.http.get(f"{ADMIN}/context", params={"id": "missing"}).status_code == 404
    assert portal.http.get(f"{ADMIN}/session", params={"id": "missing"}).status_code == 404


async def test_delete_commit_then_context_then_session(portal, stored):
    first, second = stored["first"], stored["second"]

    response = portal.post("/memories/delete", kind="commit", id=second["commit_id"],
                           context_id=second["context_id"])
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"{ADMIN}/context?id=")
    assert SECOND not in get(portal, "/context", id=second["context_id"]).text
    recalled = await call(stored["token"], "recall", session=stored["session"])
    assert [m["text"] for m in recalled] == [FIRST]

    response = portal.post("/memories/delete", kind="context", id=first["context_id"],
                           session_id=first["session_id"])
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"{ADMIN}/session?id=")
    assert portal.http.get(f"{ADMIN}/context", params={"id": first["context_id"]}).status_code == 404
    assert await call(stored["token"], "recall", session=stored["session"]) == []

    response = portal.post("/memories/delete", kind="session", id=first["session_id"])
    assert response.status_code == 303
    assert response.headers["location"] == f"{ADMIN}/memories?removed=session"
    assert portal.http.get(f"{ADMIN}/session", params={"id": first["session_id"]}).status_code == 404
    assert usage_row(portal, stored["agent_id"])["sessions"] == 0
