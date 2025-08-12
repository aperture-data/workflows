# This module acts as a proxy for the SQL Server application.
# It handles ApertureDB queries and manages the connection pool.
# This provides a performance boost by reusing connections and reducing the overhead of establishing new connections.


from fastapi import FastAPI, Form, File, UploadFile
from fastapi.responses import JSONResponse
import json
import base64
from connection_pool import ConnectionPool

app = FastAPI()

pool = ConnectionPool()


@app.post("/aperturedb")
async def query_multipart(
    query: str = Form(...),
    blobs: list[UploadFile] | None = File(None),
):
    in_json = json.loads(query)
    in_blobs = [await b.read() for b in (blobs or [])]
    status, out_json, out_blobs = pool.execute_query(in_json, in_blobs)
    return JSONResponse({
        "status": status,
        "json": out_json,
        "blobs": [base64.b64encode(b).decode("ascii") for b in (out_blobs or [])]
    })
