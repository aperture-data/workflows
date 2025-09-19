import os
import json
import pytest
import psycopg2
from psycopg2.extras import Json
import numpy as np
from typing import List
import base64


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


class TestBlob:
    """Test blob retrieval"""

    def test_blob_retrieval_without_blobs_flag(self, sql_connection):
        """
        Test that blob data is not returned when _blobs flag is not set.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs = FALSE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _blob column should be None
        for row in result:
            assert row[1] is None, f"Expected _blob to be None when _blobs=FALSE, got: {type(row[1])}"

    def test_blob_retrieval_with_blobs_flag(self, sql_connection):
        """
        Test that blob data is returned when _blobs flag is set.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs = TRUE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with actual blob data
        for row in result:
            assert row[1] is not None, f"Expected _blob to contain data when _blobs=TRUE, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _blob to be bytes, got: {type(row[1])}"

    def test_blob_retrieval_with_blobs_is_true(self, sql_connection):
        """
        Test that blob data is returned when _blobs IS TRUE.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs IS TRUE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with actual blob data
        for row in result:
            assert row[1] is not None, f"Expected _blob to contain data when _blobs IS TRUE, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _blob to be bytes, got: {type(row[1])}"

    def test_blob_retrieval_with_not_blobs(self, sql_connection):
        """
        Test that blob data is not returned when NOT _blobs.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE NOT _blobs
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _blob column should be None
        for row in result:
            assert row[1] is None, f"Expected _blob to be None when NOT _blobs, got: {type(row[1])}"

    def test_blob_retrieval_with_blobs_not_equal_true(self, sql_connection):
        """
        Test that blob data is not returned when _blobs <> TRUE.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs <> TRUE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _blob column should be None
        for row in result:
            assert row[1] is None, f"Expected _blob to be None when _blobs <> TRUE, got: {type(row[1])}"

    def test_blob_retrieval_with_blobs_is_false(self, sql_connection):
        """
        Test that blob data is not returned when _blobs IS FALSE.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs IS FALSE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _blob column should be None
        for row in result:
            assert row[1] is None, f"Expected _blob to be None when _blobs IS FALSE, got: {type(row[1])}"

    def test_blob_retrieval_with_blobs_is_not_true(self, sql_connection):
        """
        Test that blob data is not returned when _blobs IS NOT TRUE.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs IS NOT TRUE
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _blob column should be None
        for row in result:
            assert row[1] is None, f"Expected _blob to be None when _blobs IS NOT TRUE, got: {type(row[1])}"


    def test_blob_retrieval_with_properties(self, sql_connection):
        """
        Test that blob retrieval works with property filtering.
        """
        sql = """
        SELECT _uniqueid, _blob FROM system."Blob" 
        WHERE _blobs = TRUE
        AND _uniqueid IS NOT NULL
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with actual blob data
        for row in result:
            assert row[1] is not None, f"Expected _blob to contain data when _blobs=TRUE with properties, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _blob to be bytes, got: {type(row[1])}"


    def test_blob_count_without_blobs(self, sql_connection):
        """
        Test that COUNT works correctly without blob retrieval.
        """
        sql = """
        SELECT COUNT(*) FROM system."Blob" 
        WHERE _blobs = FALSE;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
        
        assert result[0] >= 0, f"Expected count to be non-negative, got: {result[0]}"

    def test_blob_count_with_blobs(self, sql_connection):
        """
        Test that COUNT works correctly with blob retrieval.
        """
        sql = """
        SELECT COUNT(*) FROM system."Blob" 
        WHERE _blobs = TRUE;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
        
        assert result[0] >= 0, f"Expected count to be non-negative, got: {result[0]}"
