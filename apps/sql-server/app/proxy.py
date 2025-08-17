# This module acts as a proxy for the SQL Server application.
# It handles ApertureDB queries and manages the connection pool.
# This provides a performance boost by reusing connections and reducing the overhead of establishing new connections.


from fastapi import FastAPI, Form, File, UploadFile, Request
from fastapi.responses import JSONResponse
import json
import base64
from connection_pool import ConnectionPool
from typing import List, Optional
from embeddings import Embedder

app = FastAPI()

pool = ConnectionPool()

# Proxy for ApertureDB queries


@app.post("/aperturedb")
async def query_multipart(
    query: str = Form(...),
    blobs: Optional[List[UploadFile]] = File(None),
):
    in_json = json.loads(query)
    in_blobs = [await b.read() for b in (blobs or [])]
    status, out_json, out_blobs = pool.execute_query(in_json, in_blobs)
    return JSONResponse({
        "status": status,
        "json": out_json,
        "blobs": [base64.b64encode(b).decode("ascii") for b in (out_blobs or [])]
    })


# Proxy for embedding requests

@app.post("/v2/embed/texts")
async def forward_embed_texts(req: Request):
    request = await req.json()
    embedder = Embedder.from_properties(
        {
            "embedding_provider": request["provider"],
            "embedding_model": request["model"],
            "embedding_pretrained": request["corpus"],
        }
    )
    embeddings = embedder.embed_texts(request["texts"])
    return JSONResponse({
        "embeddings": [base64.b64encode(e.tobytes()).decode("ascii") for e in embeddings]
    })


@app.post("/v2/embed/images")
async def forward_embed_images(req: Request):
    request = await req.json()
    embedder = Embedder.from_properties(
        {
            "embedding_provider": request["provider"],
            "embedding_model": request["model"],
            "embedding_pretrained": request["corpus"],
        }
    )
    images = [base64.b64decode(i) for i in request["images"]]
    embeddings = embedder.embed_images(images)
    return JSONResponse({
        "embeddings": [base64.b64encode(e.tobytes()).decode("ascii") for e in embeddings]
    })
