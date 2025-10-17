from fastapi import FastAPI, Request, Response, Header, HTTPException, status, Query, Cookie
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional
from rag import QAChain
from llm import load_llm
from fastapi.staticfiles import StaticFiles
from wf_argparse import ArgumentParser, validate
import logging
import time
import json
import os
from aperturedb.CommonLibrary import create_connector
import asyncio

from embeddings import Embedder
from rag import QAChain
from context_builder import ContextBuilder
from retriever import Retriever
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from status_tools import StatusUpdater


logger = logging.getLogger(__name__)

APP_PATH = "/rag"
SLEEP_TIME = 3  # seconds to wait before checking if the app is ready

ready = False

start_time = time.time()
startup_time = None


async def query_with_aimon(query: str, history: Optional[str] = None):
    answer, new_history, rewritten_query, docs = await qa_chain.run(query, history)
    from aimon import AsyncClient
    # Add your AIMON_API_KEY, get it from app.aimon.ai -> My Account -> Keys -> "Copy API Key"
    aimon_api_key = args.aimon_api_key
    logger.info(f"WF_AIMON_API_KEY: {aimon_api_key is not None}")
    logger.info(f"WF_AIMON_APP_NAME: {args.aimon_app_name}")
    logger.info(f"WF_AIMON_LLM_MODEL_NAME: {args.aimon_llm_model_name}")
    if aimon_api_key and args.aimon_app_name and args.aimon_llm_model_name:
        # The user query and generated text are important
        aimon_payload = {}
        # This is the user query that you sent to the original LLM
        aimon_payload["user_query"] = rewritten_query
        # This is the generated response from the original LLM
        aimon_payload["generated_text"] = answer
        # This is the instructions that you want to evaluate
        aimon_payload["instructions"] = [
            "Ensure that the output is correct and is derived from the provided context."]
        # The context is the documents retrieved by the retriever
        aimon_payload["context"] = [
            docs.page_content for docs in docs] + [history]

        # This configuration invokes AIMon's "instruction_adherence" model
        # that validates if an LLM response has been
        aimon_payload["config"] = {
            "hallucination": {"detector_name": "default"},
            "conciseness": {"detector_name": "default"},
            "completeness": {"detector_name": "default"},
            "toxicity": {"detector_name": "default"},
            "instruction_adherence": {
                "detector_name": "default",
                "explain": "true",  # Generates textual explanation that helps understand AIMon's evaluation
            }
        }

        # This parameter controls whether you want to publish the analysis to the go/aimonui
        aimon_payload["publish"] = True

        # This parameter controls whether you would like to perform this computation asynchronously
        aimon_payload["async_mode"] = False

        # Include application_name and model_name if publishing
        if aimon_payload["publish"]:
            aimon_payload["application_name"] = args.aimon_app_name
            # This is the LLM you used to generate the SQL query from text,
            # AIMon only uses this for metadata in the UI. AIMon does not invoke this LLM.
            aimon_payload["model_name"] = args.aimon_llm_model_name

        data_to_send = [aimon_payload]

        async def call_aimon():
            async with AsyncClient(auth_header=f"Bearer {aimon_api_key}") as aimon:
                resp = await aimon.inference.detect(body=data_to_send)
                return resp

        # Await on
        resp = await call_aimon()
        resp_json = resp[0].instruction_adherence
        print(json.dumps(resp_json, indent='\t'))
    else:
        logger.info(
            "WF_AIMON_API_KEY, WF_AIMON_APP_NAME, WF_AIMON_LLM_MODEL_NAME not set, skipping AIMon analysis")
        resp_json = {}

    return answer, new_history, rewritten_query, docs


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting RAG API lifespan")
    global args
    args = get_args()
    asyncio.create_task(main(args))
    global startup_time
    startup_time = time.time() - start_time
    logger.info(
        f"RAG API is ready to serve requests after {startup_time:.2f}s")
    yield
    logger.info("Shutting down RAG API.")

# Set up the root app to redirect to /rag; useful for local dev
root_app = FastAPI(lifespan=lifespan)


@root_app.get("/")
async def redirect_to_rag():
    """Redirect the root URL to /rag."""
    return Response(status_code=status.HTTP_307_TEMPORARY_REDIRECT, headers={"Location": APP_PATH})


# This is the main app for the RAG API
app = FastAPI(root_path=APP_PATH)

allowed_origins = validate("origin", envar="WF_ALLOWED_ORIGINS", default="http://localhost", sep=",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print(f"Added CORS middleware with origins: {allowed_origins}")

root_app.mount(APP_PATH, app)


@app.get("/ask")
async def ask_get(request: Request,
                  authorization: str = Header(None),
                  token: str = Cookie(default=None),
                  query: str = Query(None, description="The question to ask"),
                  history: Optional[str] = Query(None, description="A summary of the conversation history")):
    """Non-streaming endpoint for asking questions, either GET or POST.

    Must supply token in the authorization bearer header or a cookie.

    Returns a JSON response with the answer, history, and rewritten query.
    """
    return await ask(request, authorization, token, query, history)


@app.post("/ask")
async def ask_post(request: Request,
                   authorization: str = Header(None),
                   token: str = Cookie(default=None),
                   query: str = Query(None, description="The question to ask"),
                   history: Optional[str] = Query(None, description="A summary of the conversation history")):
    """Non-streaming endpoint for asking questions, either GET or POST.

    Must supply token in the authorization bearer header or a cookie.

    Returns a JSON response with the answer, history, and rewritten query.
    """

    body = await request.json()
    query = body.get("query")
    history = body.get("history")
    return await ask(request, authorization, token, query, history)


async def ask(request: Request,
              authorization: str,
              token: str,
              query: str,
              history: Optional[str]):

    verify_token(authorization, token)

    if not_ready := get_not_ready_status():
        logger.info(f"Not ready: {not_ready}")
        return JSONResponse(not_ready, status_code=503)

    if not query:
        raise HTTPException(
            status_code=422, detail="Missing 'query' parameter")

    logger.info(f"Received query: {query}")

    start_time = time.time()
    # answer, new_history, rewritten_query, docs = await qa_chain.run(query, history)
    answer, new_history, rewritten_query, docs = await query_with_aimon(query, history)

    qa_duration = time.time() - start_time
    logger.info(f"Answer: {answer}, duration: {qa_duration:.2f}s")
    json_docs = [doc.to_json() for doc in docs]

    return {"answer": answer,
            "history": new_history,
            "rewritten_query": rewritten_query,
            "duration": qa_duration,
            "documents": json_docs,
            }


@app.get("/ask/stream")
async def stream_ask(query: str = Query(description="The question to ask"),
                     history: Optional[str] = Query(
                         None, description="A summary of the conversation history"),
                     authorization: str = Header(None),
                     token: str = Cookie(default=None)):
    """Streaming endpoint for asking questions.

    Must supply token in the authorization bearer header or a cookie.

    Returns a streaming response with the following events:
    - `start`: Indicates the start of the response
    - `rewritten_query`: The rewritten query
    - `data`: The answer tokens as they are generated
    - `end`: Indicates the end of the response, with duration and number of parts
    - `history`: The updated conversation history
    """

    verify_token(authorization, token)

    if not_ready := get_not_ready_status():
        logger.info(f"Not ready: {not_ready}")
        return JSONResponse(not_ready, status_code=503)

    async def event_generator():
        yield f"event: start\ndata: {json.dumps({})}\n\n"
        results = []
        start_time = time.time()
        answer_stream, history_fn, rewritten_query, docs = await qa_chain.stream_run(query, history)
        yield f"event: rewritten_query\ndata: {json.dumps(rewritten_query)}\n\n"
        json_docs = [doc.to_json() for doc in docs]
        yield f"event: documents\ndata: {json.dumps(json_docs)}\n\n"
        async for token in answer_stream:
            yield f"data: {json.dumps(token)}\n\n"
            # logger.debug(f"data: {token}\n\n")
            results.append(token)
        qa_duration = time.time() - start_time
        answer = "".join(results)
        logger.info(
            f"Answer: {answer}, duration: {qa_duration:.2f}s")
        yield f"event: end\ndata: {json.dumps({'duration': qa_duration, 'parts': len(results)})}\n\n"
        new_history = history_fn()
        yield f"event: history\ndata: {json.dumps(new_history)}\n\n"
        # TODO: With arbitrary messages, we can send, e.g., images

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/login")
async def login(request: Request):
    """
    Login endpoint to set a cookie with the token.
    Expects a JSON body with a "token" field.
    """
    data = await request.json()
    client_token = data.get("token")

    if not client_token:
        return JSONResponse({"error": "Missing token"}, status_code=400)

    if client_token != API_TOKEN:
        return JSONResponse({"error": "Invalid token"}, status_code=401)

    logger.info(f"Login successful")
    response = JSONResponse({"message": "Login successful"})
    response.set_cookie(
        key="token",
        value=client_token,
        httponly=True,
        secure=False,  # True if you're using HTTPS!
        samesite="strict",
    )
    return response


@app.post("/logout")
def logout(response: Response):
    """
    Logout endpoint to clear the cookie.
    """
    logger.info(f"Logout successful")
    response.delete_cookie("token", path="/")
    return {"message": "Logged out"}


@app.get("/config")
async def config(request: Request):
    """
    Endpoint to get the configuration of the API.
    Used for debugging and demos.
    """
    verify_token(request.headers.get("Authorization"),
                 request.cookies.get("token"))

    # If we're not ready, then return that information instead
    if not_ready := get_not_ready_status():
        logger.info(f"Not ready: {not_ready}")
        return JSONResponse(not_ready)

    # calculate number of descriptors in the descriptorset
    count = retriever.count() if retriever else 0

    db_host = validate("hostname", envar="DB_HOST", allow_unset=True)
    
    config = {
        "llm_provider": llm.provider,
        "llm_model": llm.model,
        "input": args.input,
        "n_documents": args.n_documents,
        **({"host": db_host} if db_host else {}),
        # "startup_time": startup_time,  # Useful for debugging, but confusing to user
        "count": count,
        "ready": True,
    }
    logger.info(f"Config: {config}")
    assert config["host"] != "", "DB_HOST is not set"
    return JSONResponse(config)


def verify_token(auth_header: str = Header(None), token_cookie: str = Cookie(None)):
    """
    Verify the token from the Authorization header or cookie.
    """
    token = None

    # Prefer Authorization header if present
    if auth_header:
        scheme, _, auth_token = auth_header.partition(" ")
        if scheme.lower() == "bearer":
            logger.info(f"Token from Authorization header")
            token = auth_token

    # Otherwise fall back to cookie
    if not token and token_cookie:
        token = token_cookie
        if token:
            logger.info(f"Token from cookie")

    # Now check if token matches
    if not token or token != API_TOKEN:
        logger.info(f"No valid token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API token",
        )


def get_retriever(descriptorset_name: str, k: int):
    """Build the retriever for the given descriptorset and model."""
    client = create_connector()
    embedder = Embedder.from_existing_descriptor_set(
        client, descriptorset_name)
    retriever = Retriever(
        embeddings=embedder,
        descriptor_set=descriptorset_name,
        search_type="mmr",  # "similarity" or "mmr"
        k=k,
        fetch_k=k * 4,  # number of results fetched for MMR
        client=create_connector(),
    )
    return retriever


def read_not_ready_file(path="not-ready.txt") -> Optional[str]:
    """Read the not-ready file if it exists."""
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def get_not_ready_status(path="not-ready.txt") -> Optional[dict]:
    """Check if the app is ready to serve requests.
    This allows this workflow to be composed with other workflows
    that may not be ready yet.
    """
    # Lifespan test
    if not ready:
        logger.info("App is not ready yet")
        return {"ready": False, "detail": "App is not ready yet"}

    if detail := read_not_ready_file(path):
        return {"ready": False, "detail": detail}


async def main(args):
    logging.basicConfig(level=args.log_level, force=True)
    logger.info("Starting RAG API")
    logger.info(f"Log level: {args.log_level}")
    logger.info(f"Input ID: {args.input}")

    global API_TOKEN
    API_TOKEN = args.token

    global llm
    llm = load_llm(args.llm_provider, args.llm_model, args.llm_api_key)

    context_builder = ContextBuilder()

    global retriever

    # We can't create the retriever until we're ready
    while read_not_ready_file():
        logger.info("Waiting for not-ready file to be removed...")
        await asyncio.sleep(SLEEP_TIME)

    retriever = get_retriever(args.input, args.n_documents)

    global qa_chain
    qa_chain = QAChain(retriever, context_builder, llm, args.rewrite_query)

    global ready
    ready = True

    updater = StatusUpdater()
    updater.post_update(
        completed=100,
        phase="rag",
        accessible=True,
    )
    logger.info("Complete.")


def get_args(argv=[]):
    obj = ArgumentParser()

    obj.add_argument('--input',
                     help='The descriptorset to use')

    obj.add_argument('--llm_provider',
                     help='The LLM provider to use, e.g. openai, huggingface, together, groq, cohere; default is huggingface',
                     default=None)

    obj.add_argument('--llm_model',
                     help='The LLM model to use, e.g. gpt-3.5-turbo, gpt-4, llama-2-7b-chat; default depends on provider',
                     default=None)

    obj.add_argument('--llm_api_key',
                     help='The LLM API key to use, if required by the provider',
                     default=None)

    obj.add_argument('--log-level',
                     help='Logging level, e.g. INFO, DEBUG',
                     choices=list(logging._nameToLevel.keys()),
                     default='INFO')

    obj.add_argument('--model',
                     #  deprecated=True, # Python 3.13+
                     help='DEPRECATED')

    obj.add_argument('--token',
                     help='The token required to use the API',
                     required=True)

    obj.add_argument('--port',
                     help='The port to use for the API',
                     default=8000,
                     type=int)

    obj.add_argument('--n-documents',
                     help='The number of documents to return from the retriever',
                     default=4,
                     type=int)

    obj.add_argument('--aimon-api-key',
                     help='API key for AIMON')

    obj.add_argument('--aimon-app-name',
                     help='Name of the AIMON app')

    obj.add_argument('--aimon-llm-model-name',
                     help='Name of the LLM model')

    obj.add_argument('--rewrite-query',
                     help='Whether to rewrite the query using the LLM',
                     default=False)

    params = obj.parse_args(argv)
    del params.model  # no longer used; reads from descriptor set

    return params


app.mount("/", StaticFiles(directory="static", html=True), name="static")
