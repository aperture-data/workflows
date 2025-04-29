from typing import List, Dict
from langchain.schema import Document


class ContextBuilder:
    def __init__(self, max_tokens: int = 4096):
        self.max_tokens = max_tokens

    def build(self, retrieved_docs: List[Document], query: str, chat_history: List[Dict[str, str]]) -> str:
        """
        retrieved_docs: list of LangChain Document objects
        query: the user's current question
        chat_history: list of {user, assistant} turns
        """

        # Serialize retrieved document contents
        context_text = "\n\n".join(doc.page_content for doc in retrieved_docs)

        # Serialize history
        history_text = ""
        for turn in chat_history:
            history_text += f"User: {turn['user']}\nAssistant: {turn['assistant']}\n"

        # Build full context
        full_context = f"""
=== System Instructions ===
You are a helpful assistant. Format your answer using plain text and simple markdown (bold, lists) when appropriate. Answer the user's question based on the following context. Only answer based on provided information. If no relevant information exists, say you don't know.
=== Retrieved Knowledge ===
{context_text}

=== Conversation History ===
{history_text}

=== Current User Query ===
{query}

=== Assistant Response ===
"""

        # TODO: Apply token limit
        return full_context.strip()
