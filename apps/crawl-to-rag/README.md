# Crawl to RAG

This is a meta-workflow that merely combines multiple other workflows into one.
This combines the following workflows:
* [crawl-website](../crawl-website/)
* [text-extraction](../text-extraction/)
* [text-embeddings](../text-embeddings/)
* [rag](../rag/)

The permissible parameters are broadly a union of all the parameters available in the individual workflows, except that `WF_INPUT` and `WF_DESCRIPTOR_SET` must not be used. Instead they are tied to `WF_OUTPUT`.

The following parameters are required: 
* **`WF_TOKEN`**: Sets the authorization token for the `rag` web service

The following parameters are recommended: 
* **`WF_OUTPUT`**: Sets `WF_INPUT` and `WF_OUTPUT` for all sub-workflows. Also determines the name of the descriptor set created. Defaults to a generated UUID.
* **`WF_CLEAN`**: Recommended if reusing the same value for `WF_OUTPUT`
* **`WF_START_URLS`**: Tells the `crawl-website` workflow where to start
* **`WF_ALLOWED_DOMAINS`**: Optionally allows the `crawl-website` workflow to follow links to additional sites
* **`WF_LLM_PROVIDER`**: For performance, it is recommended to use a cloud provider for the `rag` workflow. The default is `huggingface` which is local, but largely intended for cloud-independent testing.
* **`WF_LLM_API_KEY`**: Required by the `rag` workflow when using a cloud LLM provider

See the respective workflow READMEs for more details.