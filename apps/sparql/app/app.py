from fastapi import Header, Cookie, HTTPException, Depends
from fastapi import FastAPI, Request, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import logging
from fastapi.responses import Response
from fastapi import status

from aperturedb.CommonLibrary import create_connector
from aperturedb.SPARQL import SPARQL
from wf_argparse import ArgumentParser
import json

logger = logging.getLogger(__name__)

APP_PATH = "/sparql"

# Set up the root app to redirect to the app path; useful for local dev
root_app = FastAPI()


@root_app.get("/")
async def redirect_to_app():
    """Redirect the root URL to the app path."""
    return Response(status_code=status.HTTP_307_TEMPORARY_REDIRECT, headers={"Location": APP_PATH})

# This is the main app for the RAG API
app = FastAPI(root_path=APP_PATH)

templates = Jinja2Templates(directory="templates")

PREFIXES = {
    "t": "http://aperturedb.io/type/",
    "c": "http://aperturedb.io/connection/",
    "p": "http://aperturedb.io/property/",
    "o": "http://aperturedb.io/object/",
    "knn": "http://aperturedb.io/knn/",
}


def execute_sparql(query: str) -> dict:
    client = create_connector()
    sparql = SPARQL(client)
    result = sparql.query(query)
    return json.loads(result.serialize(format="json"))


def verify_token(
    authorization: str = Header(None),
    token: str = Cookie(None)
) -> Cookie:
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:]
    elif access_token:
        token = access_token

    if not token or token != TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return Cookie(token=token)


@app.get("/", response_class=HTMLResponse)
async def sparql_ui(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "prefixes": PREFIXES,
        "endpoint_path": "query"
    })


@app.get("/query")
async def sparql_endpoint(request: Request,
                          query: str = Query(None),
                          token: str = Depends(verify_token)):
    if not query:
        return JSONResponse(status_code=400, content={"error": "No SPARQL query provided"})

    try:
        result = execute_sparql(query)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/query")
async def sparql_endpoint(request: Request, token: str = Depends(verify_token)):
    form = await request.form()
    query = form.get("query")

    if not query:
        return JSONResponse(status_code=400, content={"error": "No SPARQL query provided"})

    try:
        result = execute_sparql(query)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def get_args(argv=[]):
    obj = ArgumentParser()

    obj.add_argument('--token',
                     help='The token required to use the API',
                     required=True)

    obj.add_argument('--port',
                     help='The port to use for the API',
                     default=8001,
                     type=int)

    params = obj.parse_args(argv)
    return params


def main(args):
    global TOKEN
    TOKEN = args.token


# Unconditional because invoked via uvicorn
root_app.mount(APP_PATH, app)
args = get_args()
main(args)
