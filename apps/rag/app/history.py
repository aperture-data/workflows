from typing import List, Dict

# Simple in-memory history manager for conversation history.
# This is a hook for a more sophisticated history manager later.


class HistoryManager:
    """Interface for managing conversation history."""

    def get_history(self, session_id: str) -> List[Dict[str, str]]:
        raise NotImplementedError

    def append_turn(self, session_id: str, user: str, assistant: str) -> None:
        raise NotImplementedError


class InMemoryHistory(HistoryManager):
    def __init__(self):
        self.store = {}

    def get_history(self, session_id: str) -> List[Dict[str, str]]:
        return self.store.get(session_id, [])

    def append_turn(self, session_id: str, user: str, assistant: str) -> None:
        if session_id not in self.store:
            self.store[session_id] = []
        self.store[session_id].append({"user": user, "assistant": assistant})
