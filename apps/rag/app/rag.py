from typing import List, Dict, Iterator, Tuple, Callable
import logging

logger = logging.getLogger(__name__)
SEPARATOR = "NEW_SUMMARY"
MAX_SEPARATOR_LENGTH = len(SEPARATOR)


class QAChain:
    def __init__(self, retriever, context_builder, llm):
        self.retriever = retriever
        self.context_builder = context_builder
        self.llm = llm

    def run(self, query: str, history: str) -> Tuple[str, str]:
        rewritten_query = self._rewrite_query(
            query, history) if history else query
        logger.debug(f"Rewritten query: {query} -> {rewritten_query}")
        docs = self.retriever.invoke(rewritten_query)
        prompt = self.context_builder.build(docs, query, history)
        answer = self.llm.predict(prompt)
        new_history = ""  # TODO
        return answer, new_history

    async def stream_run(self, query: str, history: str) -> Tuple[Iterator[str], Callable]:
        rewritten_query = await self._rewrite_query(
            query, history)
        docs = self.retriever.invoke(rewritten_query)
        prompt = self.context_builder.build(docs, query, history)

        summary_buffer = ""

        async def _stream_answer():
            buffer = ""
            in_summary = False
            nonlocal summary_buffer
            async for token in self.llm.stream_predict(prompt):
                buffer += token

                if not in_summary:
                    sep_index = buffer.find(SEPARATOR)
                    if sep_index != -1:
                        # logger.debug(
                        #     f"token={token}: Separator found at index {sep_index}")
                        yield buffer[:sep_index]
                        summary_buffer = buffer[sep_index + len(SEPARATOR):]
                        in_summary = True
                    elif len(buffer) > MAX_SEPARATOR_LENGTH:
                        # logger.debug(
                        #     f"token={token}: Flushing buffer {len(buffer)-MAX_SEPARATOR_LENGTH} characters")
                        # Only flush up to the last N characters that might be part of the separator
                        safe_flush = buffer[:-MAX_SEPARATOR_LENGTH]
                        keep_back = buffer[-MAX_SEPARATOR_LENGTH:]
                        yield safe_flush
                        buffer = keep_back
                    # else:
                    #     logger.debug(
                    #         f"token={token}: Show buffer, not yielding yet")
                else:
                    # logger.debug(
                    #     f"token={token}: In summary mode, appending to summary buffer: {summary_buffer}")
                    summary_buffer += token

        def get_summary():
            logger.debug(f"Summary tokens: {summary_buffer}")
            if summary_buffer:
                return summary_buffer
            else:
                logger.debug("No summary tokens found.")
                return history  # old history

        return _stream_answer(), get_summary, rewritten_query

    async def _rewrite_query(self, query: str, history_summary: str = "No history") -> str:
        prompt = f"""
Rewrite the user's question so it can be understood without conversation context.

Conversation summary:
{history_summary}

User question:
{query}

Standalone rewritten query:
        """.strip()
        return await self.llm.predict(prompt)
