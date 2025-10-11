from aperturedb.CommonLibrary import execute_query, create_connector
from aperturedb.Connector import Connector
from itertools import product
import os
import numpy as np
from typing import Dict, List

np.random.seed(42)  # Seed for reproducibility


def load_entity_testdata(client):
    """
    Create some test data for entity schema tests.
    """
    query = []
    
    # Create some entities with different classes
    for i in range(10):
        query.append({"AddEntity": {
            "class": "Person",
            "properties": {
                "name": f"Person_{i}",
                "age": 20 + i,
            }
        }})
    
    for i in range(5):
        query.append({"AddEntity": {
            "class": "Company",
            "properties": {
                "name": f"Company_{i}",
                "employees": 100 + i * 50,
            }
        }})
    
    execute_query(client, query)


def load_connection_testdata(client):
    """
    Create some test data for connection schema tests.
    """
    query = []
    
    # Create entities and connections
    for i in range(5):
        query.append({"AddEntity": {
            "class": "Person",
            "_ref": i * 2 + 1,
            "properties": {"name": f"Employee_{i}"}
        }})
        query.append({"AddEntity": {
            "class": "Company",
            "_ref": i * 2 + 2,
            "properties": {"name": f"Employer_{i}"}
        }})
        query.append({"AddConnection": {
            "src": i * 2 + 1,
            "dst": i * 2 + 2,
            "class": "WorksAt",
            "properties": {"since": 2020 + i}
        }})
    
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
TEXT_MODELS = [
    {"provider": "clip", "model": "ViT-B/16",
        "corpus": "openai", "dimensions": 512},
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
        status, response, _ = execute_query(client, query, blobs)
        assert status == 0, f"Failed to create descriptor set: {response}"
        query = []
        blobs = []

        for j, text in enumerate(TEXTS):
            embedding = random_embedding(model["dimensions"])
            query.append({
                "AddDescriptor": {
                    "set": set_name,
                    "properties": {
                        "text": text,
                        "url": f"https://example.com/doc_{j}",
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

    for i, image in enumerate(images):
        ref = len(query) + 1
        query.append({
            "AddImage": {
                "_ref": ref,
                "properties": {
                    "title": f"Image_{i}",
                }
            }
        })
        blobs.append(image)

    status, _, _ = execute_query(client, query, blobs)
    assert status == 0


def str_to_bool(s):
    """Convert a string to a boolean."""
    return s.lower() in ["true", "1", "yes", "y"]


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
    CA_CERT = os.getenv("CA_CERT", None)
    USE_SSL = str_to_bool(os.getenv("USE_SSL", "true"))
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS, ca_cert=CA_CERT, use_ssl=USE_SSL)


if __name__ == "__main__":
    client = db_connection()

    print(f"host={client.host}")
    load_entity_testdata(client)
    load_connection_testdata(client)
    load_text_descriptors_testdata(client)
    load_images_testdata(client)
    print("Test data loaded successfully.")

    _, results, _ = execute_query(client, [{"GetSchema": {}}])

    print("Loaded test data:", results)


