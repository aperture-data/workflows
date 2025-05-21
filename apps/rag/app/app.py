from fastapi import FastAPI, Request, Response, Header, HTTPException, status, Query, Cookie
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
import uuid
from typing import Optional
from rag import QAChain
from llm import load_llm
from fastapi.staticfiles import StaticFiles
from wf_argparse import ArgumentParser
import logging
from uuid import uuid4
from langchain_community.vectorstores import ApertureDB
import time
import json
import os

from llm import LLM
from embeddings import BatchEmbedder
from embeddings import DEFAULT_MODEL as EMBEDDING_MODEL
from rag import QAChain
from context_builder import ContextBuilder

logger = logging.getLogger(__name__)

APP_PATH = "/rag"


# Set up the root app to redirect to /rag; useful for local dev
root_app = FastAPI()


@root_app.get("/")
async def redirect_to_rag():
    """Redirect the root URL to /rag."""
    return Response(status_code=status.HTTP_307_TEMPORARY_REDIRECT, headers={"Location": APP_PATH})

# This is the main app for the RAG API
app = FastAPI(root_path=APP_PATH)


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
    answer, new_history, rewritten_query, docs = await qa_chain.run(query, history)
    qa_duration = time.time() - start_time
    logger.info(f"Answer: {answer}, duration: {qa_duration:.2f}s")

    return {"answer": answer,
            "history": new_history,
            "rewritten_query": rewritten_query,
            "duration": qa_duration,
            "documents": docs,
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
        yield f"event: documents\ndata: {json.dumps(docs)}\n\n"
        async for token in answer_stream:
            yield f"data: {json.dumps(token)}\n\n"
            # logger.debug(f"data: {token}\n\n")
            results.append(token)
        qa_duration = time.time() - start_time
        answer = "".join(results)
        logger.info(
            f"Answer: {answer}, duration: {qa_duration: .2f}s")
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

    config = {
        "llm_provider": llm.provider,
        "llm_model": llm.model,
        "embedding_model": args.model,
        "input": args.input,
        "embedding_model": args.model,
        "n_documents": args.n_documents,
        "host": os.getenv("DB_HOST", ""),
    }
    logger.info(f"Config: {config}")
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


def get_retriever(descriptorset_name: str, model: str, k: int):
    """Build the retriever for the given descriptorset and model."""
    embeddings = BatchEmbedder(model)
    # TODO: Check fingerprint
    # dim = embeddings.dimensions()
    vectorstore = ApertureDB(embeddings=embeddings,
                             descriptor_set=descriptorset_name)

    search_type = "mmr"  # "similarity" or "mmr"
    fetch_k = k*4       # number of results fetched for MMR
    retriever = vectorstore.as_retriever(search_type=search_type,
                                         search_kwargs=dict(k=k, fetch_k=fetch_k))
    return retriever


def get_not_ready_status(path="not-ready.txt") -> Optional[dict]:
    """Check if the app is ready to serve requests.
    This allows this workflow to be composed with other workflows
    that may not be ready yet.
    """
    try:
        with open(path, "r") as f:
            return {"ready": False, "detail": f.read()}
    except FileNotFoundError:
        return None


def main(args):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")

    logging.basicConfig(level=args.log_level, force=True)
    logger.info("Starting text embeddings")
    logger.info(f"Log level: {args.log_level}")
    logger.info(f"Input ID: {args.input}")

    global API_TOKEN
    API_TOKEN = args.token

    global llm
    llm = load_llm(args.llm_provider, args.llm_model, args.llm_api_key)

    retriever = get_retriever(args.input, args.model, args.n_documents)

    context_builder = ContextBuilder()
    global qa_chain
    qa_chain = QAChain(retriever, context_builder, llm)

    logger.info("Complete.")


def get_args(argv=[]):
    obj = ArgumentParser()

    obj.add_argument('--input',
                     help='The descriptorset to use')

    obj.add_argument('--llm_provider',
                     help='The LLM provider to use, e.g. openai, huggingface, together, groq; default is huggingface',
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
                     help='The embedding model to use, of the form "backend model pretrained',
                     default=EMBEDDING_MODEL)

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

    params = obj.parse_args(argv)
    return params


# Unconditional because invoked via uvicorn
root_app.mount(APP_PATH, app)
args = get_args()
main(args)
