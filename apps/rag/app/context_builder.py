from typing import List, Dict
from langchain.schema import Document


class ContextBuilder:
    def __init__(self, max_tokens: int = 4096):
        self.max_tokens = max_tokens

    separator = "===SUMMARY==="
    def build(self, retrieved_docs: List[Document], query: str, history: str) -> str:
        """
        retrieved_docs: list of LangChain Document objects
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
First, answer the user's question. Format your answer using plain text and simple markdown (bold, lists) when appropriate. Answer the user's question based on the following context. Only answer based on provided information. If no relevant information exists, say you don't know. 
After your answer, output the exact string `{self.separator}` on its own line, with no extra formatting, and then write the updated summary in plain text.

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
