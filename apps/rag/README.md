# RAG (Retrieval Augmented Generation)

This workflow takes embedded text documents in ApertureDB and makes them part of a RAG pipeline, using LLM to build a "chat bot".

## Running in docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS=password \
           -e WF_LOG_LEVEL=INFO \
           -e WF_TOKEN=secretsquirrel \
           -e WF_INPUT=mydescriptorset \
           aperturedata/workflows-rag
```

Parameters:
* **`LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default WARNING.
* **`WF_TOKEN`**: Authorization token to use in API
* **`WF_INPUT`**: Name of descriptorset to use
* **`WF_LLM_PROVIDER`**: The LLM provider to use, e.g. openai, huggingface, together, groq, cohere
* **`WF_LLM_MODEL`**: The LLM model to use, e.g. gpt-3.5-turbo, gpt-4, llama-2-7b-chat; default depends on provider - see table below
* **`WF_LLM_API_KEY`**: API key for LLM provider
* **`WF_MODEL`**: The embedding model to use, of the form "backend model pretrained
* **`WF_PORT`**: The port to use; default 8000. Note that this service is HTTP and expects to be wrapped by an HTTPS proxy with appropriate keys.
* **`WF_N_DOCUMENTS`**: Number of documents to retrieve

The following parameters are required, if configuring for AIMon analytics.
* **`AIMON_API_KEY`**: Optional for monitoring response quality with [AIMon](https://aimon.ai). A key would be required for this to work. If not specified the AIMon integration would be disabled.
* **`AIMON_APP_NAME`**: Optional, should be the same as configured on the AIMon dashboard.
* **`LLM_MODEL_NAME`**: Optional, should be the same as configured on the AIMon dashboard.

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## LLMs

This code supports a number of different LLM providers, and can easily be extended to more. One local "free" provider is included, but this will be slow to use. It should be straightforward to extend [the code](app/llm.py) to other providers.

| Type | Provider | Suggested Model | API key required |
| --- | --- | --- | --- |
| Local | [huggingface](https://huggingface.co/models) | TinyLlama/TinyLlama-1.1B-Chat-v1.0 | No |
| Cloud | [openai](https://platform.openai.com/docs/models) | gpt-3.5-turbo | Yes |
| Cloud | [together](https://www.together.ai/models) | mistralai/Mistral-7B-Instruct-v0.2 | Yes |
| Cloud | [groq](https://console.groq.com/docs/models) | llama3-8b-8192 | Yes |
| Cloud | [cohere](https://docs.cohere.com/v2/docs/models) | command-r-plus | Yes |

## API

The service supports a number of API endpoints. It is implemented using FastAPI, so also supports `/docs`, `/redoc`, and `/openapi.json` for documentation. Brief documentation follows below:

* **`/ask`**: Non-stream query interface.
    * As GET, expects `query` and optionally `history`.
    * As POST, expects a JSON object with `query` and optionally `history`.
* **`/ask/stream`**: Stream query interface. GET only. Expects `query` and optionally `history`.  Returns events:
  * start: Indicates the start of the response
  * rewritten_query: The rewritten query
  * data: The answer tokens as they are generated
  * end: Indicates the end of the response, with duration and number of parts
  * history: The updated conversation history
* **`/login`**: POST only. Expects a JSON object containing a `token` field. Returns a cookie.
* **`/logout`**: POST only. Clears the cookie.
* **`/config`**: GET only. Returns a JSON object reporting aspects of the server configuration. Used for debugging.

With the exception of `/login` and `/logout`, all API methods require authentication using the token specified when the workflow was started. This can be supplied in as and authorization bearer token, or as a cookie called `token`.

# Testing.
This attempts to have a sort of way to evaluate the quality of RAG results from a FindDesctiptor part of responses.

The test.py runs some NLP queries and can possibly evaluate the rag retrievals based on the premise that we advice to our users.

the file responses_openclip ViT-B-32 laion2b_s34b_b79k_TinyLlama-1.1B-Chat-v1.0.json is a capture of one such run.

There is no scoring implemented yet.

The db was prepared with the following docker invocations:

### crawl-website
```
bash ../build.sh  && docker run -it -p 8000:8000 -p 8080:8080 -p 8888:8888 --network garfield_default -e DB_PORT=55551 -e DB_HOST=lenz  -e CLEAN=true  -e DATASET=faces -e RUN_NAME=testing -e WF_OUTPUT=testing  -e WF_LOG_LEVEL=INFO  -v ./input:/app/input aperturedata/workflows-crawl-website:latest
```

### text-extraction
```
bash ../build.sh  && docker run -it -p 8000:8000 -p 8080:8080 -p 8888:8888 --network garfield_default -e DB_PORT=55551 -e DB_HOST=lenz  -e CLEAN=true  -e DATASET=faces -e RUN_NAME=testing -e WF_OUTPUT=testing -e WF_INPUT=testing -e WF_LOG_LEVEL=INFO  -e WF_CLEAN=true -v ./input:/app/input aperturedata/workflows-text-extraction:latest
```

### text-embeddings
```
bash ../build.sh  && docker run -it -p 8000:8000 -p 8080:8080 -p 8888:8888 --network garfield_default -e DB_PORT=55551 -e DB_HOST=lenz  -e CLEAN=true  -e DATASET=faces -e RUN_NAME=testing -e WF_OUTPUT=testing -e WF_INPUT=testing -e WF_LOG_LEVEL=INFO  -e WF_CLEAN=true -v ./input:/app/input aperturedata/workflows-text-embeddings:latest
```