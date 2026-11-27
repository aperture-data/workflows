"""Admin portal: sign-in, session protection and agent management."""

import httpx

from conftest import ADMIN, ADMIN_TOKEN, BASE_URL, Portal, call, initialize, unique


def fresh_client() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, follow_redirects=False, timeout=60)


def test_root_redirects_to_portal():
    response = fresh_client().get("/nexus/")
    assert response.status_code == 303
    assert response.headers["location"] == f"{ADMIN}/"


def test_portal_requires_sign_in():
    response = fresh_client().get(f"{ADMIN}/")
    assert response.status_code == 401
    assert "Sign in" in response.text
    assert 'name="csrf"' not in response.text


def test_invalid_link_token_is_rejected():
    response = fresh_client().get(f"{ADMIN}/login", params={"token": "wrong"})
    assert response.status_code == 403
    assert "set-cookie" not in response.headers


def test_link_sign_in_sets_session_and_drops_token():
    response = Portal().login()
    assert response.headers["location"] == f"{ADMIN}/"
    cookie = response.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert f"Path={ADMIN}" in cookie
    assert ADMIN_TOKEN not in cookie
    assert response.headers["cache-control"] == "no-store"


def test_form_sign_in():
    client = fresh_client()
    assert client.post(f"{ADMIN}/login", data={"token": "wrong"}).status_code == 403
    response = client.post(f"{ADMIN}/login", data={"token": ADMIN_TOKEN})
    assert response.status_code == 303
    assert client.get(f"{ADMIN}/").status_code == 200


def test_changes_require_csrf_token(portal):
    agent_id = unique("no-csrf")
    response = portal.http.post(f"{ADMIN}/agents", data={"agent_id": agent_id})
    assert response.status_code == 403
    assert agent_id not in portal.page().text


def test_invalid_and_duplicate_agents_are_rejected(portal):
    assert portal.post("/agents", agent_id="bad id!").status_code == 400
    agent_id = unique("dup")
    portal.add_agent(agent_id)
    response = portal.post("/agents", agent_id=agent_id)
    assert response.status_code == 400
    assert "already exists" in response.text
    portal.remove(agent_id)
    portal.add_agent(agent_id)
    portal.remove(agent_id)


async def test_agent_lifecycle(portal):
    agent_id = unique("lifecycle")
    token = portal.add_agent(agent_id, name="Lifecycle Bot", department="qa")

    listing = portal.page().text
    assert agent_id in listing and "Lifecycle Bot" in listing
    assert token not in listing
    assert "api_key_hash" not in listing
    assert initialize(token).status_code == 200

    rotated = portal.rotate(agent_id)
    assert rotated != token
    assert initialize(token).status_code == 401
    assert initialize(rotated).status_code == 200
    stored = await call(rotated, "remember", text="Rotation keeps memories.",
                        session=unique("rotation"))
    assert stored["commit_id"]

    portal.remove(agent_id)
    assert initialize(rotated).status_code == 401
    assert agent_id not in portal.page().text
    retired = portal.post("/agents", agent_id=agent_id, organization=unique("new-org"))
    assert retired.status_code == 400
    assert "still owns stored data" in retired.text


def test_connect_page_shows_mcp_url(portal):
    response = portal.http.get(f"{ADMIN}/connect")
    assert response.status_code == 200
    assert "/nexus/mcp/" in response.text
