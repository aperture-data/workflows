# Crawl to RAG

This is a meta-workflow that merely combines multiple other workflows into one.
This combines the following workflows:
* [crawl-website](../crawl-website/)
* [text-extraction](../text-extraction/)
* [text-embeddings](../text-embeddings/)
* [rag](../rag/)

The permissible parameters are broadly a union of all the parameters available in the individual workflows, except that `WF_INPUT` and `WF_DESCRIPTOR_SET` must not be used. Instead they are tied to `WF_OUTPUT`.

The following parameters are required: `WF_TOKEN`

The following parameters are recommended: `WF_OUTPUT`, `WF_LLM_PROVIDER`, `WF_LLM_API_KEY`