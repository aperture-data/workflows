from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from typing import List, Any, Annotated
import base64
import asyncpg
import os
import logging


app = FastAPI(
    docs_url="/sql/docs",
    openapi_url="/sql/openapi.json",
    title="ApertureDB SQL Server API",
)

bearer_scheme = HTTPBearer(auto_error=False)

LOG_LEVEL = os.getenv("WF_LOG_LEVEL", "WARN").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

AUTH_TOKEN = os.getenv("WF_AUTH_TOKEN")  # checked in app.sh

DB_DATABASE = "aperturedb"
DB_USER = "aperturedb"
DB_PASSWORD = AUTH_TOKEN  # Same key for both
DB_HOST = "localhost"
DB_MAX_CONNECTIONS = 5

DB_POOL = None  # see init_pool()


class SQLQueryRequest(BaseModel):
    query: Annotated[str, "SQL query to execute"]


class ColumnMetadata(BaseModel):
    name: Annotated[str, "column name"]
    type: Annotated[str,
                    "PostgreSQL type name, e.g. 'text', 'bytea', 'timestamp'"]


class SQLQueryResponse(BaseModel):
    columns: Annotated[List[ColumnMetadata], "ordered list of column metadata"]
    rows: Annotated[List[List[Any]],
                    "list of rows, each row is a list of values in the same order as columns"]


@app.on_event("startup")
async def startup():
    await init_pool()


@app.post("/sql/query")
async def sql_query(
    req: Request,
    body: SQLQueryRequest,
    token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> SQLQueryResponse:
    logger.info(f"Executing SQL query: {body.query}")
    check_bearer_auth(token)

    async with DB_POOL.acquire() as conn:
        try:
            stmt = await conn.prepare(body.query)
            rows = await stmt.fetch()

            # Extract column metadata
            columns = [
                ColumnMetadata(
                    name=attr.name,
                    type=attr.type.name
                )
                for attr in stmt.get_attributes()
            ]
            logger.debug(f"Query executed successfully, columns: {columns}")
            # Encode rows with base64 for blobs
            result_rows = []
            for row in rows:
                result_row = []
                for val, attr in zip(row, stmt.get_attributes()):
                    if val is None:
                        result_row.append(None)
                    elif attr.type.name in ("bytea",):  # catch blobs
                        encoded = base64.b64encode(val).decode("ascii")
                        result_row.append(encoded)
                    elif attr.type.name == "timestamptz":
                        result_row.append(val.isoformat())
                    else:
                        result_row.append(val)
                result_rows.append(result_row)

            logger.debug(f"Result rows: {result_rows}")

            result = SQLQueryResponse(
                columns=columns,
                rows=result_rows
            )

            return JSONResponse(
                content=result.dict()
            )
        except asyncpg.PostgresError as e:
            logger.exception(f"Error executing SQL query: {e}")
            return JSONResponse(
                status_code=400,
                content={"error": str(e)}
            )


def check_bearer_auth(token: HTTPAuthorizationCredentials):
    """Check if the provided token matches the expected AUTH_TOKEN."""
    if not token or not token.credentials:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Missing authentication token")
    if token.credentials != AUTH_TOKEN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Invalid authentication token")


async def init_pool():
    """Initialize the database connection pool."""
    logger.info("Initializing database connection pool")
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        host=DB_HOST,
        port=5432,  # Default PostgreSQL port
        min_size=1,
        max_size=DB_MAX_CONNECTIONS,
    )
