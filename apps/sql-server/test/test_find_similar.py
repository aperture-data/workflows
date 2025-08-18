import os
import json
import pytest
import psycopg2
from psycopg2.extras import Json
from warnings import warn
import numpy as np
from typing import List

SETS = ["TestText_0", "TestImage_0", "TestText_1", "TestImage_1"]

N = 3

TEXTS = ["ceibo", "hornero", "pato"]


def random_images(n: int = 10) -> List[bytes]:
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


IMAGES = random_images(N)


def random_embedding(dimensions):
    """
    Generate a random embedding vector.
    """
    return np.random.rand(dimensions).tolist()


# TODO: Parameterize this dimension
VECTORS = [random_embedding(512) for _ in range(N)]


@pytest.fixture(scope="session")
def sql_connection():
    conn = psycopg2.connect(
        host=os.getenv("SQL_HOST", "sql-server"),
        port=os.getenv("SQL_PORT", "5432"),
        dbname=os.getenv("SQL_NAME", "aperturedb"),
        user=os.getenv("SQL_USER", "aperturedb"),
        password=os.getenv("SQL_PASS", "test"),
    )
    conn.autocommit = True
    yield conn
    conn.close()


N_NEIGHBORS = 4


@pytest.mark.parametrize("set_name", SETS)
@pytest.mark.parametrize("text", TEXTS)
def test_texts(set_name, text, sql_connection):
    """
    Test searching for similar text
    """
    sql = f"""
    SELECT * FROM "{set_name}"
    WHERE _find_similar = FIND_SIMILAR(
        TEXT := %s,
        K := {N_NEIGHBORS});
    """
    with sql_connection.cursor() as cur:
        cur.execute(sql, (text,))
        result = cur.fetchall()
    assert len(
        result) == N_NEIGHBORS, f"Wrong number of results for text: {text}: {len(result)}"


@pytest.mark.parametrize("set_name", SETS)
@pytest.mark.parametrize("image", IMAGES, ids=range(len(IMAGES)))
def test_images(set_name, image, sql_connection):
    """
    Test searching for similar images
    """
    sql = f"""
    SELECT * FROM "{set_name}"
    WHERE _find_similar = FIND_SIMILAR(
        IMAGE := %s,
        K := {N_NEIGHBORS});
    """
    with sql_connection.cursor() as cur:
        cur.execute(sql, (psycopg2.Binary(image),))
        result = cur.fetchall()
    assert len(
        result) == N_NEIGHBORS, f"Wrong number of results for image: {len(result)}"


@pytest.mark.parametrize("set_name", SETS)
@pytest.mark.parametrize("vector", VECTORS, ids=range(len(VECTORS)))
def test_vectors(set_name, vector, sql_connection):
    """
    Test searching for similar vectors
    """
    sql = f"""
    SELECT * FROM "{set_name}"
    WHERE _find_similar = FIND_SIMILAR(
        VECTOR := %s,
        K := {N_NEIGHBORS});
    """
    with sql_connection.cursor() as cur:
        cur.execute(sql, (Json(vector),))
        result = cur.fetchall()
    assert len(
        result) == N_NEIGHBORS, f"Wrong number of results for vector: {len(result)}"
