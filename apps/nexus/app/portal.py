"""Admin portal at /nexus/admin/.

ApertureDB Cloud opens /nexus/admin/login?token=<WF_ADMIN_TOKEN> from the
workflow's Connect button. The login exchanges the token for a signed session
cookie and redirects, so the token leaves the address bar at once. Self-hosted
deployments can paste the token into the sign-in form instead.
"""

import hashlib
import hmac
import logging
import secrets
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from aperture_nexus.exceptions import NexusValidationError
from service import AccessError, NexusService
from settings import ADMIN_PATH, MCP_PATH

logger = logging.getLogger("aperture.nexus.portal")

COOKIE = "nexus_admin"
SESSION_SECONDS = 4 * 60 * 60
BRIDGE_URL = "https://github.com/aperture-data/workflows/blob/main/apps/mcp-server/stdio-bridge.py"
SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; form-action 'self'",
}

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _when(value: Optional[str]) -> str:
    """Show a stored UTC ISO timestamp to the minute."""
    return f"{value[:16].replace('T', ' ')} UTC" if value else ""


templates.env.filters["when"] = _when


class Sessions:
    """Stateless admin sessions signed with a key derived from the admin token."""

    def __init__(self, admin_token: str):
        self._admin_token = admin_token
        self._key = hashlib.sha256(b"nexus-portal-session\0" + admin_token.encode()).digest()

    def _sign(self, value: str) -> str:
        return hmac.new(self._key, value.encode(), hashlib.sha256).hexdigest()

    def check_token(self, token: Optional[str]) -> bool:
        return bool(token) and hmac.compare_digest(token.encode(), self._admin_token.encode())

    def issue(self) -> str:
        value = f"{int(time.time()) + SESSION_SECONDS}.{secrets.token_urlsafe(16)}"
        return f"{value}.{self._sign(value)}"

    def valid(self, cookie: Optional[str]) -> bool:
        if not cookie or cookie.count(".") != 2:
            return False
        value, signature = cookie.rsplit(".", 1)
        if not hmac.compare_digest(signature, self._sign(value)):
            return False
        return int(value.split(".", 1)[0]) > time.time()

    def csrf(self, cookie: str) -> str:
        return self._sign("csrf\0" + cookie)


def create_router(nexus: NexusService, admin_token: str) -> APIRouter:
    sessions = Sessions(admin_token)
    router = APIRouter()

    def secure(request: Request) -> bool:
        return request.url.scheme == "https"

    def mcp_url(request: Request) -> str:
        return f"{request.url.scheme}://{request.url.netloc}{MCP_PATH}"

    def render(request: Request, name: str, status_code: int = 200, **context) -> Response:
        cookie = request.cookies.get(COOKIE)
        context.update(
            admin_path=ADMIN_PATH,
            mcp_url=mcp_url(request),
            bridge_url=BRIDGE_URL,
            csrf=sessions.csrf(cookie) if sessions.valid(cookie) else "",
        )
        response = templates.TemplateResponse(request, name, context, status_code=status_code)
        response.headers.update(SECURITY_HEADERS)
        return response

    def signed_in(request: Request) -> bool:
        return sessions.valid(request.cookies.get(COOKIE))

    def csrf_ok(request: Request, csrf: str) -> bool:
        cookie = request.cookies.get(COOKIE)
        return sessions.valid(cookie) and hmac.compare_digest(csrf, sessions.csrf(cookie))

    def sign_in_page(request: Request, error: Optional[str] = None, status_code: int = 401):
        return render(request, "signin.html", status_code=status_code, error=error)

    def start_session(request: Request) -> Response:
        response = RedirectResponse(f"{ADMIN_PATH}/", status_code=303)
        response.set_cookie(COOKIE, sessions.issue(), max_age=SESSION_SECONDS,
                            path=ADMIN_PATH, httponly=True, samesite="lax",
                            secure=secure(request))
        response.headers.update(SECURITY_HEADERS)
        return response

    async def agents_page(request: Request, status_code: int = 200, **context) -> Response:
        agents = await nexus.call(nexus.list_agents)
        return render(request, "agents.html", status_code=status_code, page="agents",
                      agents=agents, **context)

    # ------------------------------------------------------------------
    # Sign in and out
    # ------------------------------------------------------------------

    @router.get("/login")
    async def login_link(request: Request, token: str = ""):
        if sessions.check_token(token):
            return start_session(request)
        return sign_in_page(request, "This sign-in link is not valid. Open the portal "
                            "from the workflow's Connect button.", 403)

    @router.post("/login")
    async def login_form(request: Request, token: str = Form("")):
        if sessions.check_token(token.strip()):
            return start_session(request)
        return sign_in_page(request, "That admin token is not valid.", 403)

    @router.post("/logout")
    async def logout(request: Request, csrf: str = Form("")):
        if not csrf_ok(request, csrf):
            return sign_in_page(request, status_code=403)
        response = RedirectResponse(f"{ADMIN_PATH}/", status_code=303)
        response.delete_cookie(COOKIE, path=ADMIN_PATH)
        return response

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    @router.get("/")
    async def index(request: Request):
        if not signed_in(request):
            return sign_in_page(request)
        return await agents_page(request)

    @router.get("/connect")
    async def connect(request: Request):
        if not signed_in(request):
            return sign_in_page(request)
        return render(request, "connect.html", page="connect")

    # ------------------------------------------------------------------
    # Agent management
    # ------------------------------------------------------------------

    async def change(request: Request, csrf: str, action, *args, success: str,
                     issued_for: Optional[str] = None) -> Response:
        if not csrf_ok(request, csrf):
            return sign_in_page(request, "Your session expired. Sign in again.", 403)
        try:
            token = await nexus.call(action, *args)
        except (AccessError, NexusValidationError) as e:
            return await agents_page(request, 400, error=str(e))
        except Exception:
            logger.exception("Agent change failed")
            return await agents_page(request, 500, error="The change failed. Check the workflow logs.")
        issued = {"agent_id": issued_for, "token": token} if issued_for else None
        return await agents_page(request, message=success, issued=issued)

    @router.post("/agents")
    async def create_agent(request: Request, csrf: str = Form(""), agent_id: str = Form(""),
                           name: str = Form(""), department: str = Form(""),
                           organization: str = Form("")):
        agent_id = agent_id.strip()
        return await change(request, csrf, nexus.create_agent, agent_id, name,
                            department, organization, success=f"Agent {agent_id} added.",
                            issued_for=agent_id)

    @router.post("/agents/rotate")
    async def rotate_agent(request: Request, csrf: str = Form(""), agent_id: str = Form("")):
        return await change(request, csrf, nexus.rotate_agent, agent_id,
                            success=f"New token issued for {agent_id}. The old token no longer works.",
                            issued_for=agent_id)

    @router.post("/agents/delete")
    async def delete_agent(request: Request, csrf: str = Form(""), agent_id: str = Form("")):
        return await change(request, csrf, nexus.delete_agent, agent_id,
                            success=f"Agent {agent_id} removed. Its memories are kept.")

    # ------------------------------------------------------------------
    # Memories
    # ------------------------------------------------------------------

    @router.get("/memories")
    async def memories(request: Request, q: str = "", agent: str = "", organization: str = "",
                       session: str = "", department: str = "", removed: str = ""):
        if not signed_in(request):
            return sign_in_page(request)
        form = {"q": q, "agent": agent, "organization": organization,
                "session": session, "department": department}
        context = {"form": form, "agents": await nexus.call(nexus.list_agents),
                   "message": "Session deleted." if removed == "session" else None}
        try:
            if any(v.strip() for v in (q, organization, session, department)):
                context["results"] = await nexus.call(
                    nexus.search_memories, query=q, agent=agent, organization=organization,
                    session=session, department=department)
            else:
                context["sessions"] = await nexus.call(nexus.list_sessions, agent)
        except (AccessError, NexusValidationError) as e:
            return render(request, "memories.html", 400, page="memories", error=str(e), **context)
        return render(request, "memories.html", page="memories", **context)

    @router.get("/session")
    async def session_page(request: Request, id: str = "", removed: str = ""):
        if not signed_in(request):
            return sign_in_page(request)
        detail = await nexus.call(nexus.session_detail, id) if id else None
        if detail is None:
            return render(request, "notfound.html", 404, page="memories", what="session")
        message = "Context deleted." if removed == "context" else None
        return render(request, "session.html", page="memories", session=detail, message=message)

    @router.get("/context")
    async def context_page(request: Request, id: str = "", removed: str = ""):
        if not signed_in(request):
            return sign_in_page(request)
        detail = await nexus.call(nexus.context_detail, id) if id else None
        if detail is None:
            return render(request, "notfound.html", 404, page="memories", what="context")
        message = "Memory deleted." if removed == "commit" else None
        return render(request, "context.html", page="memories", ctx=detail, message=message)

    @router.post("/memories/delete")
    async def delete_memories(request: Request, csrf: str = Form(""), kind: str = Form(""),
                              id: str = Form(""), context_id: str = Form(""),
                              session_id: str = Form("")):
        if not csrf_ok(request, csrf):
            return sign_in_page(request, "Your session expired. Sign in again.", 403)
        try:
            await nexus.call(nexus.delete_memories, kind, id)
        except AccessError as e:
            return render(request, "notfound.html", 400, page="memories", what=kind, error=str(e))
        if kind == "commit":
            target = f"{ADMIN_PATH}/context?{urlencode({'id': context_id, 'removed': 'commit'})}"
        elif kind == "context":
            target = f"{ADMIN_PATH}/session?{urlencode({'id': session_id, 'removed': 'context'})}"
        else:
            target = f"{ADMIN_PATH}/memories?removed=session"
        return RedirectResponse(target, status_code=303)

    # ------------------------------------------------------------------
    # Usage
    # ------------------------------------------------------------------

    @router.get("/usage")
    async def usage(request: Request):
        if not signed_in(request):
            return sign_in_page(request)
        return render(request, "usage.html", page="usage", usage=await nexus.call(nexus.usage))

    return router
