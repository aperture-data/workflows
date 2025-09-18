from aperturedb.CommonLibrary import execute_query, create_connector
from aperturedb.Connector import Connector
from itertools import product
import os
import numpy as np
import requests
import base64
from typing import Dict, List

np.random.seed(42)  # Seed for reproducibility


def strip_null(d):
    """
    Strip null values from a dictionary.
    """
    return {k: v for k, v in d.items() if v is not None}


def load_constraints_testdata(client):
    """
    Create some test data for the constraint suite.
    """
    boolean_values = [None, False, True]
    number_values = [None, 0, 1, 2]
    string_values = [None, "", "a", "b", "c"]

    query = []
    for b, n, s in product(boolean_values, number_values, string_values):
        query.append({"AddEntity": {"class": "TestRow",
                                    "properties": strip_null({
                                        "b": b,
                                        "n": n,
                                        "s": s
                                    })}})

    ref = 1
    for i in range(5):
        query.append({"AddEntity": {"class": "SourceNode",
                                    "properties": {"source_key": i},
                                    "_ref": ref,
                                    }})
        query.append({"AddEntity": {"class": "DestinationNode",
                                    "properties": {"destination_key": i},
                                    "_ref": ref + 1,
                                    }})
        query.append({"AddConnection": {
            "src": ref,
            "dst": ref + 1,
            "class": "edge",
            "properties": {"edge_key": i},
        }})
        ref += 2

    execute_query(client, query)


TEXTS = [
    "A group of people hiking through a forest trail.",
    "A cat sitting on a windowsill looking outside.",
    "A busy city street during rush hour.",
    "A child blowing bubbles in a park.",
    "A plate of fresh sushi on a wooden table.",
    "A mountain landscape at sunrise.",
    "A cyclist riding along a coastal road.",
    "A dog playing fetch with its owner.",
    "A close-up of colorful autumn leaves.",
    "A family enjoying a picnic by the lake."
]

# Tests will assume 512 dimensions
IMAGE_MODELS = [
    {"provider": "clip", "model": "ViT-B/16",
        "corpus": "openai", "dimensions": 512},
    {"provider": "openclip", "model": "ViT-B-32",
        "corpus": "laion2b_s34b_b79k", "dimensions": 512}
]

# Tests will assume 512 dimensions
TEXT_MODELS = [
    {"provider": "clip", "model": "ViT-B/16",
        "corpus": "openai", "dimensions": 512},
    {"provider": "openclip", "model": "ViT-B-32",
        "corpus": "laion2b_s34b_b79k", "dimensions": 512}
]


def random_images(n=10):
    """
    Generate a list of random images for testing.
    """
    images = []
    for _ in range(n):
        width = np.random.randint(100, 500)
        height = np.random.randint(100, 500)
        format = np.random.choice(['JPEG', 'PNG'])
        image = np.random.randint(0, 256, (height, width, 3), dtype=np.uint8)
        from PIL import Image
        from io import BytesIO
        img_byte_arr = BytesIO()
        Image.fromarray(image).save(img_byte_arr, format=format)
        images.append(img_byte_arr.getvalue())
    return images

def random_blobs(n=10):
    """
    Generate a list of random blobs for testing.
    """
    return [np.random.bytes(np.random.randint(100, 1000)) for _ in range(n)]


def random_embedding(dimensions):
    """
    Generate a random embedding vector.
    """
    return np.random.rand(dimensions).astype(np.float32).tobytes()


def load_text_descriptors_testdata(client):
    """
    Create some test data for the text descriptor suite.
    """
    query = []
    blobs = []

    for i, model in enumerate(TEXT_MODELS):
        set_name = f"TestText_{i}"
        query.append({
            "AddDescriptorSet": {
                "name": set_name,
                "properties": {
                    "description": "Test set for descriptor suite",
                    "embeddings_provider": model["provider"],
                    "embeddings_model": model["model"],
                    "embeddings_pretrained": model["corpus"],
                },
                "dimensions": model["dimensions"],
                "metric": "CS",
            }
        })

        # https://github.com/aperture-data/athena/issues/1738
        status, _, _ = execute_query(client, query, blobs)
        assert status == 0
        query = []
        blobs = []

        for text in TEXTS:
            embedding = random_embedding(model["dimensions"])
            query.append({
                "AddDescriptor": {
                    "set": set_name,
                    "properties": {
                        "text": text,
                    }
                }
            })
            blobs.append(embedding)

    status, _, _ = execute_query(client, query, blobs)
    assert status == 0


def load_images_testdata(client):
    """
    Create some test data for the image descriptor suite.
    """
    query = []
    blobs = []

    images = random_images(10)

    for i, model in enumerate(IMAGE_MODELS):
        set_name = f"TestImage_{i}"
        query.append({
            "AddDescriptorSet": {
                "name": set_name,
                "properties": {
                    "description": "Test set for descriptor suite",
                    "embeddings_provider": model["provider"],
                    "embeddings_model": model["model"],
                    "embeddings_pretrained": model["corpus"],
                },
                "dimensions": model["dimensions"],
                "metric": "CS",
            }
        })

        status, _, _ = execute_query(client, query, blobs)
        assert status == 0
        query = []
        blobs = []

        for image in images:
            embedding = random_embedding(model["dimensions"])
            ref = len(query) + 1
            query.extend([
                {
                    "AddImage": {
                        "_ref": ref,
                    }
                },
                {
                    "AddDescriptor": {
                        "set": set_name,
                        "connect": {"ref": ref},
                    }
                }
            ])
            blobs.append(image)
            blobs.append(embedding)

    status, _, _ = execute_query(client, query, blobs)
    assert status == 0


def load_blobs_testdata(client):
    """
    Create some test data for the blob suite.
    """
    query = []
    blobs = random_blobs(10)

    for blob in blobs:
        query.append({
            "AddBlob": {
            }
        })        

    status, _, _ = execute_query(client, query, blobs)
    assert status == 0


def db_connection():
    """Create a database connection."""
    # Not used in testing, but can be used to seed a different database
    APERTUREDB_KEY = os.getenv("APERTUREDB_KEY")
    if APERTUREDB_KEY:
        return create_connector(key=APERTUREDB_KEY)

    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS)



if __name__ == "__main__":
    client = db_connection()

    print(f"host={client.host}")
    load_constraints_testdata(client)
    load_text_descriptors_testdata(client)
    load_images_testdata(client)
    load_blobs_testdata(client)
    print("Test data loaded successfully.")

    _, results, _ = execute_query(client, [{"GetSchema": {}}])

    print("Loaded test data:", results)
