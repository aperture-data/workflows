from typing import List, Dict


class ContextBuilder:
    def __init__(self, max_tokens: int = 4096):
        self.max_tokens = max_tokens

    separator = "===SUMMARY==="

    def build(self, retrieved_docs: List[Dict], query: str, history: str) -> str:
        """
        retrieved_docs: list of documents
        query: the user's current question
        history: summary of the conversation history
        """

        # Serialize retrieved document contents
        context_text = "\n\n".join(doc.page_content for doc in retrieved_docs)

        # Serialize history
        # Build full context
        full_context = f"""
=== System Instructions ===
You are a helpful assistant. Respond in two parts:

First, answer the user's question using plain text with simple markdown (bold, lists) when appropriate. Only use information from the provided context. If no relevant information exists, say you don't know.

After the answer, output the **exact string** `{self.separator}` on a line **by itself**, with **no extra words, formatting, or preamble**. Then write the updated summary in plain text. Remember to use just the exact string `{self.separator}`. Do not use any other formatting or markdown.

=== Retrieved Knowledge ===
{context_text}

=== Conversation History ===
{history}

=== Current User Query ===
{query}

=== Assistant Response ===
"""

        # TODO: Apply token limit
        return full_context.strip()
