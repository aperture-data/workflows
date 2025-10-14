# This module acts as a proxy for the SQL Server application.
# It handles ApertureDB queries and manages the connection pool.
# This provides a performance boost by reusing connections and reducing the overhead of establishing new connections.


from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import json
import base64
from connection_pool import ConnectionPool
from typing import List, Optional
from embeddings import Embedder
from pydantic import BaseModel, Field, model_validator, ValidationError
import logging
import os
import traceback
import sys
sys.path.insert(0, '/app')
from wf_argparse import validate

log_level = validate("log_level", envar="WF_LOG_LEVEL", default="INFO")
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)

app = FastAPI()

pool = ConnectionPool()


# Custom exception handler for Pydantic validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    error_details = []
    
    for error in errors:
        loc = " -> ".join(str(l) for l in error["loc"])
        error_details.append(
            f"  Location: {loc}\n"
            f"  Error: {error['msg']}\n"
            f"  Type: {error['type']}\n"
            f"  Input: {error.get('input', 'N/A')}"
        )
    
    error_msg = (
        f"Validation Error in request to {request.url.path}\n"
        f"{'='*60}\n" +
        "\n\n".join(error_details) +
        f"\n{'='*60}\n"
        f"Request body: {await request.body()}\n"
        f"Full traceback:\n{traceback.format_exc()}"
    )
    
    logger.error(error_msg)
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": error_msg,
            "errors": errors,
        }
    )


@app.exception_handler(ValidationError)
async def pydantic_validation_exception_handler(request: Request, exc: ValidationError):
    errors = exc.errors()
    error_details = []
    
    for error in errors:
        loc = " -> ".join(str(l) for l in error["loc"])
        error_details.append(
            f"  Location: {loc}\n"
            f"  Error: {error['msg']}\n"
            f"  Type: {error['type']}\n"
            f"  Input: {error.get('input', 'N/A')}"
        )
    
    error_msg = (
        f"Pydantic Validation Error in request to {request.url.path}\n"
        f"{'='*60}\n" +
        "\n\n".join(error_details) +
        f"\n{'='*60}\n"
        f"Full traceback:\n{traceback.format_exc()}"
    )
    
    logger.error(error_msg)
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": error_msg,
            "errors": errors,
        }
    )


# Proxy for ApertureDB queries


@app.post("/aperturedb")
async def query_multipart(
    query: str = Form(...),
    blobs: Optional[List[UploadFile]] = File(None),
):
    try:
        in_json = json.loads(query)
    except Exception as e:
        error_msg = f"Error parsing JSON query: {str(e)}\nQuery: {query[:500]}...\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    try:
        in_blobs = [await b.read() for b in (blobs or [])]
    except Exception as e:
        error_msg = f"Error reading blob data: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    try:
        status, out_json, out_blobs = pool.execute_query(in_json, in_blobs)
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}\nQuery: {json.dumps(in_json, indent=2)[:500]}...\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    
    try:
        return JSONResponse({
            "status": status,
            "json": out_json,
            "blobs": [base64.b64encode(b).decode("ascii") for b in (out_blobs or [])]
        })
    except Exception as e:
        error_msg = f"Error building response: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


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
    def check_non_empty(self):
        if not self.texts:
            error_msg = (
                f"Validation failed for EmbedTextInput:\n"
                f"  Field 'texts' must contain at least one entry.\n"
                f"  Received: {self.texts} (type: {type(self.texts)})\n"
                f"  Provider: {self.provider}\n"
                f"  Model: {self.model}\n"
                f"  Corpus: {self.corpus}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info(f"EmbedTextInput validation passed: {len(self.texts)} texts, provider={self.provider}, model={self.model}")
        return self


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
        error_msg = f"Error in text embedding request: {str(e)}\nProvider: {input.provider}, Model: {input.model}, Corpus: {input.corpus}\nNumber of texts: {len(input.texts)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


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
    def check_non_empty(self):
        if not self.images:
            error_msg = (
                f"Validation failed for EmbedImageInput:\n"
                f"  Field 'images' must contain at least one entry.\n"
                f"  Received: {self.images} (type: {type(self.images)})\n"
                f"  Provider: {self.provider}\n"
                f"  Model: {self.model}\n"
                f"  Corpus: {self.corpus}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        for i, image in enumerate(self.images):
            try:
                base64.b64decode(image)
            except Exception as e:
                error_msg = (
                    f"Validation failed for EmbedImageInput:\n"
                    f"  Invalid base64 image at index {i}\n"
                    f"  Error: {str(e)}\n"
                    f"  Image data length: {len(image) if image else 0}\n"
                    f"  Image preview (first 100 chars): {image[:100] if image else 'None'}...\n"
                    f"  Provider: {self.provider}\n"
                    f"  Model: {self.model}\n"
                    f"  Corpus: {self.corpus}\n"
                    f"  Total images: {len(self.images)}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        logger.info(f"EmbedImageInput validation passed: {len(self.images)} images, provider={self.provider}, model={self.model}")
        return self

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
        error_msg = f"Error in image embedding request: {str(e)}\nProvider: {input.provider}, Model: {input.model}, Corpus: {input.corpus}\nNumber of images: {len(input.images)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
