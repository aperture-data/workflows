from typing import List, Dict, Iterator


class QAChain:
    def __init__(self, retriever, context_builder, llm):
        self.retriever = retriever
        self.context_builder = context_builder
        self.llm = llm

    def run(self, query: str, chat_history: List[Dict[str, str]]) -> str:
        docs = self.retriever.invoke(query)
        prompt = self.context_builder.build(docs, query, chat_history)
        return self.llm.predict(prompt)

    async def stream_run(self, query: str, chat_history: List[Dict[str, str]]) -> Iterator[str]:
        docs = self.retriever.get_relevant_documents(query)
        prompt = self.context_builder.build(docs, query, chat_history)
        async for token in self.llm.stream_predict(prompt):
            yield token
