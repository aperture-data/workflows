import os
import base64
import numpy as np

import uvicorn

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from aperturedb import CommonLibrary


# Security
security = HTTPBearer()
WF_AUTH_TOKEN = os.environ.get("WF_AUTH_TOKEN")


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):

    if not WF_AUTH_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication token not configured on the server.",
        )
    if credentials.scheme != "Bearer" or credentials.credentials != WF_AUTH_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )


# Initialize the FastAPI application
app = FastAPI(
    title="KNN Extras for ApertureDB",
    description="A simple server that implements extras on top of ApertureDB KNN capabilities.",
    version="0.1.0",
    docs_url="/knn/docs",
    redoc_url="/knn/redoc",
    openapi_url="/knn/openapi.json"
)


@app.get(
    "/knn/descriptorsets",
    summary="List available descriptor sets",
    response_description="A list of available descriptor sets.",
    dependencies=[Depends(verify_token)]
)
async def list_descriptorsets():
    """
    Lists the available descriptor sets.
    """

    client = CommonLibrary.create_connector()

    query = [
        {
            "FindDescriptorSet": {
                "results": {
                    "all_properties": True,
                }
            }
        }
    ]

    response, _ = client.query(query)

    if not response or "FindDescriptorSet" not in response[0]:
        return {"status": "error", "error_message": "No DescriptorSet found."}

    descriptorsets = response[0]["FindDescriptorSet"]["entities"]

    return {"descriptorsets": descriptorsets, "status": "success", "error_message": None}


@app.post(
    "/knn/query",
    summary="Get KNN results",
    response_description="The KNN results from the input.",
    dependencies=[Depends(verify_token)]
)
async def knn(params: dict):
    """
    Performs a KNN search based on the provided query parameters.
    This endpoint takes a query vector, a descriptor set, a metric, and the number of neighbors to return.

    Example input:
    {
        "query": [ 0.34, 0.56, ... ],
        "set": "yfcc_vit-b/16",
        "metric": "cs",
        "k_neighbors": 10
    }
    """

    if not params.get("query") or not params.get("set"):
        return {"status": "error", "error_message": "Missing required parameters."}

    query_embedding = params.get("query")

    client = CommonLibrary.create_connector()

    query = [{
        "FindDescriptor": {
            "set": params.get("set", ""),
            "metric": params.get("metric", "").upper(),
            "distances": True,
            "k_neighbors": params.get("k_neighbors", 10),
            "results": {
                "all_properties": True,
            }
        }
    }]

    # query descriptor to float32 and then to a single binary byte array in memory
    query_descriptor_float32 = np.array(query_embedding, dtype=np.float32)
    blob = query_descriptor_float32.tobytes()

    response, _ = client.query(query, [blob])

    if not client.last_query_ok():
        return {"status": "error", "error_message": f"{response}"}

    print("Response:", response)

    if not response or "FindDescriptor" not in response[0]:
        return {"status": "error", "error_message": f"{response.get('info', 'Error with search')}"}

    results = response[0]["FindDescriptor"]["entities"]

    return {
        "nearest": results
    }


@app.post(
    "/knn/re-rank",
    summary="Get KNN re-ranking results",
    response_description="The KNN re-ranking results from the input.",
    dependencies=[Depends(verify_token)]
)
async def re_rank(params: dict):
    """
    Re-ranks the KNN results based on the provided query parameters.

    Example input:
    {
        "query": [ 0.34, 0.56, ... ],
        "set": "yfcc_vit-b/16",
        "metric": "cs",
        "k_neighbors": 10,
        "re-rank": "exact",
        "re-rank-space-factor": 2
    }
    """

    if not params.get("query") or not params.get("set"):
        return {"status": "error", "error_message": "Missing required parameters."}

    query_embedding = params.get("query")

    # Perform the KNN search first and then re-rank the results
    client = CommonLibrary.create_connector()

    query = [{
        "FindDescriptor": {
            "set": params.get("set", ""),
            "metric": params.get("metric", "").upper(),
            "blobs": True,
            "distances": True,
            "k_neighbors": params.get("k_neighbors", 10) * params.get("re-rank-space-factor", 10),
            "results": {
                "all_properties": True,
            }
        }
    }]

    # query descriptor to float32 and then to a single binary byte array in memory
    query_descriptor_float32 = np.array(query_embedding, dtype=np.float32)
    blob = query_descriptor_float32.tobytes()

    response, blobs = client.query(query, [blob])

    if not client.last_query_ok():
        return {"status": "error", "error_message": f"{response}"}

    print("Response:", response)

    # unpack the blobs, there should be re-rank-space-factor * k_neighbors blobs, each is an embeddings float32 array
    if not response or "FindDescriptor" not in response[0]:
        return {"status": "error", "error_message": f"{response.get('info', 'Error with search')}"}

    embeddings = [np.frombuffer(blob, dtype=np.float32) for blob in blobs]

    print(f"Embeddings returned: {len(embeddings)}")

    # now re-rank the results using either exact or mahalanobis distance
    re_rank_method = params.get("re-rank", "exact")

    if re_rank_method == "exact":
        # For exact re-ranking, we can just return the embeddings as they are
        nearest = response[0]["FindDescriptor"]["entities"]

    elif re_rank_method == "mahalanobis":

        # Placeholder, for now, we return the same as exact.
        nearest = response[0]["FindDescriptor"]["entities"]

        # # For mahalanobis re-ranking, we need to compute the covariance matrix and then use it to re-rank
        # # Assuming embeddings are in the shape (n_samples, n_features)
        # search_embeddings = np.array(embeddings)
        # covariance_matrix = np.cov(search_embeddings, rowvar=False)
        # inv_covariance_matrix = np.linalg.inv(covariance_matrix)

        # # Compute the mahalanobis distance for each embedding
        # search_embeddings = np.array(
        #     [np.dot(np.dot(e, inv_covariance_matrix), e) for e in search_embeddings])
    else:
        return {"status": "error", "error_message": f"Unsupported re-rank method: {re_rank_method}"}

    # Return top k_neighbors results
    nearest = response[0]["FindDescriptor"]["entities"][:params.get(
        "k_neighbors", 10)]

    return {
        "mock": True,
        "nearest": nearest,
    }


# This block allows you to run the FastAPI application directly
# when the script is executed.
if __name__ == "__main__":
    # Runs the Uvicorn server.
    # 'main:app' tells Uvicorn to look for an 'app' object in 'main.py'.
    # '--reload' enables auto-reloading when code changes are detected.

    uvicorn.run("app:app", host="0.0.0.0", port=3000,
                reload=True, timeout_keep_alive=1800)
