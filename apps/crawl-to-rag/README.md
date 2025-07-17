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
* **`WF_ALLOWED_ORIGINS`**: Required if you'd like to connect the RAG with your frontend. This string is a comma-separated list of URLs without paths or query strings. For example "https://docs.aperturedata.io,https://www.aperturedata.io"


The following parameters are recommended:
* **`WF_OUTPUT`**: Sets `WF_INPUT` and `WF_OUTPUT` for all sub-workflows. Also determines the name of the descriptor set created. Defaults to a generated UUID.
* **`WF_CLEAN`**: Recommended if reusing the same value for `WF_OUTPUT`
* **`WF_START_URLS`**: Tells the `crawl-website` workflow where to start
* **`WF_ALLOWED_DOMAINS`**: Optionally allows the `crawl-website` workflow to follow links to additional sites
* **`WF_LLM_PROVIDER`**: For performance, it is recommended to use a cloud provider for the `rag` workflow. The default is `huggingface` which is local, but largely intended for cloud-independent testing.
* **`WF_LLM_API_KEY`**: Required by the `rag` workflow when using a cloud LLM
* **`AIMON_API_KEY`**: Optional for monitoring response quality with [AIMon](https://aimon.ai). A key would be required for this to work.

The following parameters are required, if configuring for AIMon analytics.
* **`AIMON_API_KEY`**: Optional for monitoring response quality with [AIMon](https://aimon.ai). A key would be required for this to work. If not specified the AIMon integration would be disabled.
* **`AIMON_APP_NAME`**: Optional, should be the same as configured on the AIMon dashboard.
* **`LLM_MODEL_NAME`**: Optional, should be the same as configured on the AIMon dashboard.

See the respective workflow READMEs for more details.