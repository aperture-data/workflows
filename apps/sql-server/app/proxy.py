# This module acts as a proxy for the SQL Server application.
# It handles ApertureDB queries and manages the connection pool.
# This provides a performance boost by reusing connections and reducing the overhead of establishing new connections.


from fastapi import FastAPI, Form, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import json
import base64
from connection_pool import ConnectionPool
from typing import List, Optional
from embeddings import Embedder
from pydantic import BaseModel, Field, model_validator
import logging
import os

log_level = os.getenv("WF_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)

app = FastAPI()

pool = ConnectionPool()

# Proxy for ApertureDB queries


@app.post("/aperturedb")
async def query_multipart(
    query: str = Form(...),
    blobs: Optional[List[UploadFile]] = File(None),
):
    try:
        in_json = json.loads(query)
    except Exception as e:
        logger.error(f"Error parsing JSON: {e} {query=}")
        raise
    in_blobs = [await b.read() for b in (blobs or [])]
    status, out_json, out_blobs = pool.execute_query(in_json, in_blobs)
    return JSONResponse({
        "status": status,
        "json": out_json,
        "blobs": [base64.b64encode(b).decode("ascii") for b in (out_blobs or [])]
    })


# Proxy for embedding requests

class EmbedTextInput(BaseModel):
    provider: str = Field(...,
                          description="The provider of the model",
                          example="clip")
    model: str = Field(..., description="The name of the model to use.",
                       example="ViT-B/16")
    corpus: str = Field(...,
                        description="The pre-trained corpus to use for the model",
                        example="openai")
    texts: List[str] = Field(..., description="The texts to embed.",
                             example=["A group of people standing together.", "A cat sitting on a mat."])

    @model_validator(mode="after")
    def check_non_empty(cls, values):
        if not values.texts:
            raise ValueError("Field 'texts' must contain at least one entry.")
        return values


class EmbedTextOutput(BaseModel):
    embeddings: List[str] = Field(
        description="A list of base64-encoded embedding vectors for the input texts.")

    @classmethod
    def from_embeddings(cls, embeddings: List[bytes]) -> "EmbedTextOutput":
        """
        Converts a list of byte arrays to a list of base64-encoded strings.
        """
        return cls(embeddings=[base64.b64encode(e).decode('utf-8') for e in embeddings])


@app.post("/v2/embed/texts")
async def forward_embed_texts(input: EmbedTextInput) -> EmbedTextOutput:
    try:
        embedder = Embedder.from_properties(
            {
                "embeddings_provider": input.provider,
                "embeddings_model": input.model,
                "embeddings_pretrained": input.corpus,
            },
            descriptor_set=None
        )
        embeddings = embedder.embed_texts(input.texts)
        return EmbedTextOutput.from_embeddings(embeddings)
    except Exception as e:
        logger.error(f"Error in embedding request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class EmbedImageInput(BaseModel):
    provider: str = Field(...,
                          description="The provider of the model",
                          example="clip")
    model: str = Field(..., description="The name of the model to use.",
                       example="ViT-B/16")
    corpus: str = Field(...,
                        description="The pre-trained corpus to use for the model",
                        example="openai")
    images: List[str] = Field(...,
                              description="A list of base64-encoded images to embed.")

    @model_validator(mode="after")
    def check_non_empty(cls, values):
        if not values.images:
            raise ValueError("Field 'images' must contain at least one entry.")
        for i, image in enumerate(values.images):
            try:
                base64.b64decode(image)
            except Exception as e:
                raise ValueError(
                    f"Invalid base64 image at index {i}: {str(e)}")
        return values

    def get_images(self) -> List[bytes]:
        """
        Decodes the base64-encoded images to bytes.
        """
        return [base64.b64decode(image) for image in self.images]


class EmbedImageOutput(BaseModel):
    embeddings: List[str] = Field(
        description="A list of base64-encoded embedding vectors for the input images.")

    @classmethod
    def from_embeddings(cls, embeddings: List[bytes]) -> "EmbedImageOutput":
        """
        Converts a list of byte arrays to a list of base64-encoded strings.
        """
        return cls(embeddings=[base64.b64encode(e).decode('utf-8') for e in embeddings])


@app.post("/v2/embed/images")
async def forward_embed_images(input: EmbedImageInput) -> EmbedImageOutput:
    try:
        embedder = Embedder.from_properties(
            {
                "embeddings_provider": input.provider,
                "embeddings_model": input.model,
                "embeddings_pretrained": input.corpus,
            },
            descriptor_set=None
        )
        images = input.get_images()
        assert all(isinstance(i, bytes)
                   for i in images), "All images must be bytes"
        embeddings = embedder.embed_images(images)
        return EmbedImageOutput.from_embeddings(embeddings)
    except Exception as e:
        logger.error(f"Error in embedding request: {e}")
        raise HTTPException(status_code=500, detail=str(e))
