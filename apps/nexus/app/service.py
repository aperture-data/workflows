"""Nexus operations shared by the MCP tools and the admin portal.

Agents are Nexus principals. Each agent authenticates with its own token, and
every operation is scoped here: an agent reads memories from its own
organization and changes only what it wrote. Nexus itself does not enforce
either rule.

One ApertureDB connection serves all requests. The aperturedb Connector is not
safe to share between threads, so every call runs in a worker thread while
holding a single lock.
"""

import base64
import binascii
import logging
import re
import threading
from typing import Any, Callable, Optional

import anyio
from aperturedb.CommonLibrary import create_connector

from aperture_nexus import Context, Information, Memory, NexusAdmin
from aperture_nexus._embeddings import get_text_embedder
from aperture_nexus._query import literal
from aperture_nexus.auth import Principal
from aperture_nexus.config import load_config
from aperture_nexus.exceptions import (
    NexusError,
    NexusPermissionError,
    NexusValidationError,
)
from aperture_nexus.memory import (
    _CLASS_COMMIT,
    _CLASS_CONTEXT,
    _CLASS_SESSION,
    _CLASS_USER,
    _CONN_LINK,
    _CONTENT_CLASSES,
)

logger = logging.getLogger("aperture.nexus")

TOKEN_PREFIX = "nexus"
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$")
MAX_TEXT_LENGTH = 100_000
MAX_RESULTS = 50
MAX_FIELD_LENGTH = 256
MAX_LISTED = 200
MAX_LINKS = 50
NEWEST_FIRST = ("created_at", "descending")
OLDEST_FIRST = ("created_at", "ascending")
_CLASS_TEXT = _CONTENT_CLASSES["FindBlob"]

# Result properties that describe storage rather than the memory itself.
_INTERNAL_PROPERTIES = {
    "organization", "embedding_model", "document_type", "modality",
    "chunk_index", "chunk_count", "text_preview",
}

# Storage and response fields that must always come from Nexus.
_RESERVED_METADATA = _INTERNAL_PROPERTIES | {
    "agent", "commit_id", "context_id", "created_at", "department", "entry_id",
    "purpose", "score", "session", "session_id", "session_name", "text", "user_id",
}


class AccessError(Exception):
    """The request is not allowed or refers to something the agent cannot see."""


def make_agent_token(agent_id: str, api_key: str) -> str:
    """Combine an agent ID and its Nexus key into one bearer token."""
    encoded = base64.urlsafe_b64encode(agent_id.encode()).decode().rstrip("=")
    return f"{TOKEN_PREFIX}.{encoded}.{api_key}"


def parse_agent_token(token: str) -> Optional[tuple[str, str]]:
    """Return (agent_id, api_key), or None if the token is malformed."""
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != TOKEN_PREFIX or not parts[2]:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        agent_id = base64.urlsafe_b64decode(padded.encode()).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    return agent_id, parts[2]


def _clean(value: Optional[str], name: str) -> Optional[str]:
    """Strip an optional text field and enforce a length limit."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) > MAX_FIELD_LENGTH:
        raise AccessError(f"{name} must be at most {MAX_FIELD_LENGTH} characters")
    return value


def _limit(k: int) -> int:
    return max(1, min(int(k), MAX_RESULTS))


def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _public_properties(properties: dict) -> dict:
    return {
        key: value for key, value in properties.items()
        if not key.startswith("_") and key not in _INTERNAL_PROPERTIES
    }


class NexusService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._db = create_connector()
        self.memory = Memory(db_client=self._db)
        self.admin = NexusAdmin(db_client=self._db)
        self.text_model = load_config(validate_deps=False).models.text_embedding

    async def call(self, fn: Callable, *args, **kwargs):
        """Run a blocking Nexus operation in a worker thread under the lock."""
        def locked():
            with self._lock:
                return fn(*args, **kwargs)
        return await anyio.to_thread.run_sync(locked)

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def prepare(self) -> None:
        """Create indexes and load the embedding model before serving."""
        self.memory.ensure_schema()
        if self.text_model:
            get_text_embedder(self.text_model).embed_text(["warm up"])

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def _query(self, commands: list[dict]) -> list[dict]:
        """Run commands in one transaction and return each command's response body."""
        response, _ = self._db.query(commands, [])
        if not isinstance(response, list) or len(response) != len(commands):
            raise NexusError(f"Unexpected ApertureDB response: {response}")
        bodies = [next(iter(item.values())) for item in response]
        for body in bodies:
            if body.get("status", 0) != 0:
                raise NexusError(f"ApertureDB query failed: {body.get('info', body)}")
        return bodies

    @staticmethod
    def _find_command(object_class: str, constraints: Optional[list] = None, *,
                      command: str = "FindEntity", results: Optional[dict] = None,
                      sort: Optional[tuple] = None, limit: Optional[int] = None,
                      **extra) -> dict:
        body: dict = {"with_class": object_class,
                      "results": results or {"all_properties": True}, **extra}
        if command == "FindBlob":
            body["blobs"] = False
        if constraints:
            body["constraints"] = constraints
        if sort:
            body["sort"] = {"key": sort[0], "order": sort[1]}
        if limit is not None:
            body["limit"] = limit
        return {command: body}

    def _find(self, object_class: str, constraints: Optional[list] = None, **options) -> list[dict]:
        body = self._query([self._find_command(object_class, constraints, **options)])[0]
        return body.get("entities", [])

    def _find_one(self, entity_class: str, prop: str, value: str) -> Optional[dict]:
        found = self._find(entity_class, [f"${prop}", "==", literal(value)])
        return found[0] if found else None

    def _context(self, principal: Principal, context_id: str, *, owned: bool) -> dict:
        record = self._find_one(_CLASS_CONTEXT, "nexus_ctx_id", context_id)
        visible = record is not None and record.get("organization") == principal.organization
        if not visible or (owned and record.get("user_id") != principal.user_id):
            raise AccessError(f"No context {context_id!r} that this agent can {'change' if owned else 'read'}")
        return record

    @staticmethod
    def _organization(principal: Principal) -> str:
        if not principal.organization:
            raise AccessError("This agent has no organization; recreate it in the portal")
        return principal.organization

    # ------------------------------------------------------------------
    # Agent authentication
    # ------------------------------------------------------------------

    def authenticate(self, token: str) -> Optional[Principal]:
        parsed = parse_agent_token(token)
        if parsed is None:
            return None
        try:
            return self.memory.authenticate(*parsed)
        except (NexusPermissionError, NexusValidationError):
            return None

    # ------------------------------------------------------------------
    # Agent operations
    # ------------------------------------------------------------------

    def remember(self, principal: Principal, text: str, session: str,
                 purpose: Optional[str] = None,
                 metadata: Optional[dict] = None) -> dict:
        text = (text or "").strip()
        if not text:
            raise AccessError("text must not be empty")
        if len(text) > MAX_TEXT_LENGTH:
            raise AccessError(f"text must be at most {MAX_TEXT_LENGTH} characters")
        session = _clean(session, "session")
        if not session:
            raise AccessError("session must not be empty")
        for key in metadata or {}:
            if key.startswith("_") or key in _RESERVED_METADATA:
                raise AccessError(f"Metadata key {key!r} is reserved by Nexus")
        ctx = Context(
            principal=principal,
            session_name=session,
            purpose=_clean(purpose, "purpose"),
            organization=self._organization(principal),
            department=principal.department,
        )
        info = Information(context_id=ctx.id)
        info.log(text=text, metadata=metadata or None)
        commit_id = self.memory.process_and_commit(ctx, info)
        return {
            "commit_id": commit_id,
            "context_id": ctx.id,
            "session_id": ctx.session_id,
            "session": session,
        }

    def recall(self, principal: Principal, query: Optional[str] = None,
               session: Optional[str] = None, agent: Optional[str] = None,
               department: Optional[str] = None, k: int = 10) -> list[dict]:
        filters = {"organization": self._organization(principal)}
        for key, value in (("session_name", session), ("user_id", agent),
                           ("department", department)):
            value = _clean(value, key)
            if value:
                filters[key] = value
        query = (query or "").strip() or None
        results = self.memory.search(
            query=query, modality="text", filters=filters, k=_limit(k),
            return_content=False,
        )
        return [{
            "text": r.text,
            "score": round(r.score, 4),
            "agent": r.user_id,
            "session_id": r.session_id,
            "context_id": r.context_id,
            "created_at": _iso(r.created_at),
            **_public_properties(r.metadata),
        } for r in results]

    def find_sessions(self, principal: Principal, query: str,
                      agent: Optional[str] = None,
                      department: Optional[str] = None,
                      k: int = 10) -> list[dict]:
        query = (query or "").strip()
        if not query:
            raise AccessError("query must not be empty")
        filters = {"organization": self._organization(principal)}
        for key, value in (("user_id", agent), ("department", department)):
            value = _clean(value, key)
            if value:
                filters[key] = value
        contexts = self.memory.search_contexts(query, filters=filters, k=_limit(k))
        sessions: dict[str, dict] = {}
        for c in contexts:
            entry = sessions.get(c.session_id)
            if entry is None:
                record = self._find_one(_CLASS_SESSION, "session_id", c.session_id) or {}
                entry = sessions[c.session_id] = {
                    "session_id": c.session_id,
                    "session": record.get("session_name"),
                    "agent": c.user_id,
                    "contexts": [],
                }
            entry["contexts"].append({
                "context_id": c.context_id,
                "purpose": c.purpose,
                "score": round(c.score, 4),
                "created_at": _iso(c.created_at),
            })
        return list(sessions.values())

    def get_context(self, principal: Principal, context_id: str) -> dict:
        record = self._context(principal, context_id, owned=False)
        entries = self.memory.retrieve(context_id)
        return {
            "context_id": context_id,
            "session_id": record.get("session_id"),
            "agent": record.get("user_id"),
            "purpose": record.get("purpose"),
            "department": record.get("department"),
            "created_at": record.get("created_at"),
            "entries": [{
                "modality": e.modality,
                "text": e.text,
                "created_at": _iso(e.created_at),
                **_public_properties(e.properties),
            } for e in entries],
        }

    def link(self, principal: Principal, source_context_id: str,
             target_context_id: str, relationship: str) -> dict:
        relationship = _clean(relationship, "relationship")
        if not relationship:
            raise AccessError("relationship must not be empty")
        self._context(principal, source_context_id, owned=True)
        self._context(principal, target_context_id, owned=False)
        self.memory.connect(source_context_id, target_context_id, relationship)
        return {"source": source_context_id, "target": target_context_id,
                "relationship": relationship}

    def forget(self, principal: Principal, commit_id: Optional[str] = None,
               context_id: Optional[str] = None,
               session: Optional[str] = None) -> dict:
        given = [(k, v) for k, v in (("commit_id", commit_id),
                 ("context_id", context_id), ("session", session)) if v]
        if len(given) != 1:
            raise AccessError("Pass exactly one of commit_id, context_id or session")
        kind, value = given[0]
        if kind == "commit_id":
            record = self._find_one(_CLASS_COMMIT, "commit_id", value)
            if record is None or record.get("user_id") != principal.user_id:
                raise AccessError(f"No commit {value!r} that this agent can change")
            # Commit records store their organization on the parent context.
            self._context(principal, record["context_id"], owned=True)
            self.memory.remove(commit_id=value)
        elif kind == "context_id":
            self._context(principal, value, owned=True)
            self.memory.remove(context_id=value)
        else:
            author = ["$user_id", "==", literal(principal.user_id)]
            organization = ["$organization", "==", literal(self._organization(principal))]
            found = self._find(_CLASS_SESSION, ["all",
                               ["$session_name", "==", literal(value)],
                               author, organization])
            if not found:
                raise AccessError(f"No session {value!r} that this agent can change")
            # Other Nexus clients can share a session across principals. A
            # whole-session deletion also removes every context in it.
            for record in found:
                session_id = ["$session_id", "==", literal(record["session_id"])]
                total, owned = self._query([
                    self._find_command(_CLASS_CONTEXT, scope, results={"count": True})
                    for scope in (session_id, ["all", session_id, author, organization])
                ])
                if total["count"] != owned["count"]:
                    raise AccessError("This session contains contexts owned by another agent")
            for record in found:
                self.memory.remove(session_id=record["session_id"])
        return {"removed": {kind: value}}

    # ------------------------------------------------------------------
    # Admin operations
    # ------------------------------------------------------------------

    def list_agents(self) -> list[dict]:
        agents = [{
            "agent_id": u.get("user_id"),
            "name": u.get("user_name"),
            "department": u.get("department"),
            "organization": u.get("organization"),
        } for u in self._find(_CLASS_USER)]
        return sorted(agents, key=lambda a: a["agent_id"] or "")

    def create_agent(self, agent_id: str, name: Optional[str] = None,
                     department: Optional[str] = None,
                     organization: Optional[str] = None) -> str:
        agent_id = (agent_id or "").strip()
        if not AGENT_ID_RE.match(agent_id):
            raise AccessError(
                "Agent ID must start with a letter or digit and use only "
                "letters, digits, '.', '_', '@' or '-' (at most 128 characters)")
        owner = ["$user_id", "==", literal(agent_id)]
        # Retained memories and named sessions use this ID as their owner.
        counts = self._query([
            self._find_command(cls, owner, results={"count": True}, limit=1)
            for cls in (_CLASS_USER, _CLASS_SESSION, _CLASS_CONTEXT, _CLASS_COMMIT)
        ])
        if counts[0]["count"]:
            raise AccessError(f"Agent {agent_id!r} already exists")
        if any(body["count"] for body in counts[1:]):
            raise AccessError(f"Agent ID {agent_id!r} still owns stored data. Choose a new ID.")
        api_key = self.admin.create_principal(
            user_id=agent_id,
            user_name=_clean(name, "name"),
            department=_clean(department, "department"),
            organization=_clean(organization, "organization"),
        )
        return make_agent_token(agent_id, api_key)

    def rotate_agent(self, agent_id: str) -> str:
        return make_agent_token(agent_id, self.admin.rotate_key(user_id=agent_id))

    def delete_agent(self, agent_id: str) -> None:
        self.admin.delete_principal(user_id=agent_id)

    # ------------------------------------------------------------------
    # Admin: memories. The admin sees every organization.
    # ------------------------------------------------------------------

    def search_memories(self, query: Optional[str] = None, agent: Optional[str] = None,
                        organization: Optional[str] = None, session: Optional[str] = None,
                        department: Optional[str] = None, k: int = 20) -> list[dict]:
        filters = {}
        for key, value in (("user_id", agent), ("organization", organization),
                           ("session_name", session), ("department", department)):
            value = _clean(value, key)
            if value:
                filters[key] = value
        results = self.memory.search(
            query=(query or "").strip() or None, modality="text", filters=filters,
            k=_limit(k), return_content=False)
        return [{
            "text": r.text,
            "score": round(r.score, 4),
            "agent": r.user_id,
            "session": r.metadata.get("session_name"),
            "purpose": r.metadata.get("purpose"),
            "organization": r.metadata.get("organization"),
            "session_id": r.session_id,
            "context_id": r.context_id,
            "created_at": _iso(r.created_at),
        } for r in results]

    def list_sessions(self, agent: Optional[str] = None) -> list[dict]:
        agent = _clean(agent, "agent")
        constraints = ["$user_id", "==", literal(agent)] if agent else None
        return [self._session(s) for s in self._find(
            _CLASS_SESSION, constraints, sort=NEWEST_FIRST, limit=MAX_LISTED)]

    @staticmethod
    def _session(record: dict) -> dict:
        return {
            "session_id": record.get("session_id"),
            "session": record.get("session_name"),
            "agent": record.get("user_id"),
            "organization": record.get("organization"),
            "department": record.get("department"),
            "created_at": record.get("created_at"),
        }

    def session_detail(self, session_id: str) -> Optional[dict]:
        record = self._find_one(_CLASS_SESSION, "session_id", session_id)
        if record is None:
            return None
        in_session = ["$session_id", "==", literal(session_id)]
        contexts, texts = self._query([
            self._find_command(_CLASS_CONTEXT, in_session, sort=OLDEST_FIRST, limit=MAX_LISTED),
            self._find_command(_CLASS_TEXT, in_session, command="FindBlob",
                               sort=OLDEST_FIRST, limit=MAX_LISTED * 4),
        ])
        previews: dict[str, str] = {}
        for blob in texts.get("entities", []):
            text = blob.get("text_preview") or blob.get("text")
            if text and blob.get("context_id") not in previews:
                previews[blob.get("context_id")] = text
        return {**self._session(record), "contexts": [{
            "context_id": c.get("nexus_ctx_id"),
            "purpose": c.get("purpose"),
            "created_at": c.get("created_at"),
            "preview": previews.get(c.get("nexus_ctx_id")),
        } for c in contexts.get("entities", [])]}

    def context_detail(self, context_id: str) -> Optional[dict]:
        record = self._find_one(_CLASS_CONTEXT, "nexus_ctx_id", context_id)
        if record is None:
            return None
        session = self._find_one(_CLASS_SESSION, "session_id", record.get("session_id")) or {}
        commits = self._find(_CLASS_COMMIT, ["$context_id", "==", literal(context_id)],
                             sort=OLDEST_FIRST)
        return {
            "context_id": context_id,
            "agent": record.get("user_id"),
            "purpose": record.get("purpose"),
            "organization": record.get("organization"),
            "department": record.get("department"),
            "created_at": record.get("created_at"),
            "session_id": record.get("session_id"),
            "session": session.get("session_name"),
            "commits": [{"commit_id": c.get("commit_id"), "created_at": c.get("created_at")}
                        for c in commits],
            "entries": [{"text": e.text, "created_at": _iso(e.created_at),
                         "properties": _public_properties(e.properties)}
                        for e in self.memory.retrieve(context_id)],
            "links_out": self._links(context_id, "out"),
            "links_in": self._links(context_id, "in"),
        }

    def _links(self, context_id: str, direction: str) -> list[dict]:
        """Contexts linked from ("out") or to ("in") a context, with relationship types."""
        def context(ref: int, cid: str) -> dict:
            return {"FindEntity": {"with_class": _CLASS_CONTEXT, "_ref": ref,
                                   "constraints": ["$nexus_ctx_id", "==", literal(cid)]}}

        neighbours = self._query([context(1, context_id), self._find_command(
            _CLASS_CONTEXT, limit=MAX_LINKS,
            is_connected_to={"ref": 1, "connection_class": _CONN_LINK, "direction": direction},
        )])[1].get("entities", [])
        links = []
        for neighbour in neighbours:
            other = neighbour.get("nexus_ctx_id")
            src, dst = (context_id, other) if direction == "out" else (other, context_id)
            connections = self._query([context(1, src), context(2, dst), {"FindConnection": {
                "src": 1, "dst": 2, "with_class": _CONN_LINK, "results": {"list": ["type"]}}}])[2]
            links.append({
                "context_id": other,
                "relationship": ", ".join(sorted({c["type"] for c in connections.get(
                    "connections", []) if c.get("type")})),
                "agent": neighbour.get("user_id"),
                "purpose": neighbour.get("purpose"),
            })
        return links

    def delete_memories(self, kind: str, value: str) -> None:
        removers = {"commit": "commit_id", "context": "context_id", "session": "session_id"}
        if kind not in removers or not value:
            raise AccessError("Choose a commit, context or session to delete")
        self.memory.remove(**{removers[kind]: value})

    # ------------------------------------------------------------------
    # Admin: usage
    # ------------------------------------------------------------------

    _COUNTED = (
        ("sessions", "FindEntity", _CLASS_SESSION),
        ("contexts", "FindEntity", _CLASS_CONTEXT),
        ("memories", "FindEntity", _CLASS_COMMIT),
    )

    def usage(self) -> dict:
        """Totals, and per agent: sessions, contexts, memories and last activity.

        A memory is one commit: one ``remember`` call.
        """
        def count(command: str, object_class: str, constraints=None) -> dict:
            return self._find_command(object_class, constraints, command=command,
                                      results={"count": True})

        agents = self.list_agents()
        commands = [count(command, cls) for _, command, cls in self._COUNTED]
        commands.append({"FindConnection": {"with_class": _CONN_LINK, "results": {"count": True}}})
        for agent in agents:
            mine = ["$user_id", "==", literal(agent["agent_id"])]
            commands += [count(command, cls, mine) for _, command, cls in self._COUNTED]
            commands.append(self._find_command(_CLASS_COMMIT, mine, results={"list": ["created_at"]},
                                               sort=NEWEST_FIRST, limit=1))
        bodies = iter(self._query(commands))

        totals = {name: next(bodies).get("count", 0) for name, _, _ in self._COUNTED}
        totals["links"] = next(bodies).get("count", 0)
        totals["agents"] = len(agents)
        rows = []
        for agent in agents:
            row = {**agent, **{name: next(bodies).get("count", 0) for name, _, _ in self._COUNTED}}
            latest = next(bodies).get("entities", [])
            row["last_active"] = latest[0].get("created_at") if latest else None
            rows.append(row)
        rows.sort(key=lambda r: (r["last_active"] or "", r["agent_id"]), reverse=True)
        return {"totals": totals, "agents": rows}
