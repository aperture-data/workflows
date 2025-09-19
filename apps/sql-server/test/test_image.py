import os
import json
import pytest
import psycopg2
from psycopg2.extras import Json
from PIL import Image
from io import BytesIO


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


def validate_image_format(image_bytes, expected_format):
    """
    Validate that the image bytes are in the expected format.
    """
    if image_bytes is None:
        return False
    
    try:
        # Try to open the image with PIL
        image = Image.open(BytesIO(image_bytes))
        actual_format = image.format.upper()
        expected_format_upper = expected_format.upper()
        
        # Handle JPEG vs JPG
        if expected_format_upper == 'JPG':
            expected_format_upper = 'JPEG'
        
        return actual_format == expected_format_upper
    except Exception:
        return False


# Test parameters for different formats and operations
FORMATS = ['png', 'jpg', 'PNG', 'JPG', 'Png', 'Jpg']
OPERATIONS = [
    None,  # No operations
    'OPERATIONS(RESIZE(width := 100, height := 100))',
    'OPERATIONS(CROP(x := 10, y := 10, width := 50, height := 50))',
    'OPERATIONS(ROTATE(angle := 90))',
    'OPERATIONS(FLIP(code := 1))',
    'OPERATIONS(THRESHOLD(value := 128))',
    'OPERATIONS(THRESHOLD(value := 64), CROP(x := 10, y := 10, width := 50, height := 50), FLIP(code := 1), ROTATE(angle := 90), RESIZE(width := 50))'
]


class TestImage:
    """Test the _as_format functionality for image operations."""

    @pytest.mark.parametrize("format_val", FORMATS)
    def test_as_format_basic(self, sql_connection, format_val):
        """
        Test basic _as_format functionality for different formats.
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _blobs = TRUE
        AND _as_format = '{format_val}'
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with image data
        for row in result:
            assert row[1] is not None, f"Expected _image to contain data when _as_format='{format_val}', got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _image to be bytes, got: {type(row[1])}"
            
            # Validate the image format
            assert validate_image_format(row[1], format_val), f"Expected image to be in {format_val} format, but validation failed"

    @pytest.mark.parametrize("format_val", FORMATS)
    @pytest.mark.parametrize("operation", OPERATIONS)
    def test_as_format_with_operations(self, sql_connection, format_val, operation):
        """
        Test _as_format combined with various operations.
        """
        if operation is None:
            # Test without operations
            sql = f"""
            SELECT _uniqueid, _image FROM system."Image" 
            WHERE _blobs = TRUE
            AND _as_format = '{format_val}'
            LIMIT 5;
            """
        else:
            # Test with operations
            sql = f"""
            SELECT _uniqueid, _image FROM system."Image" 
            WHERE _blobs = TRUE
            AND _as_format = '{format_val}'
            AND _operations = {operation}
            LIMIT 5;
            """
        
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with image data
        for row in result:
            assert row[1] is not None, f"Expected _image to contain data when _as_format='{format_val}' with operation={operation}, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _image to be bytes, got: {type(row[1])}"
            
            # Validate the image format
            assert validate_image_format(row[1], format_val), f"Expected image to be in {format_val} format with operation={operation}, but validation failed"

    @pytest.mark.parametrize("format_val", FORMATS)
    def test_as_format_without_blobs_flag(self, sql_connection, format_val):
        """
        Test that _as_format works even without _blobs flag (should not return blob data).
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _as_format = '{format_val}'
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows but _image column should be None since _blobs is not set
        for row in result:
            assert row[1] is None, f"Expected _image to be None when _as_format='{format_val}' is set but _blobs is not, got: {type(row[1])}"

    @pytest.mark.xfail(reason="Invalid format should fail")
    @pytest.mark.parametrize("invalid_format", ['gif', 'bmp', 'tiff', 'webp', 'invalid', 'jpeg'])
    def test_as_format_invalid_format_fails(self, sql_connection, invalid_format):
        """
        Test that invalid _as_format values fail appropriately.
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _blobs = TRUE
        AND _as_format = '{invalid_format}'
        LIMIT 1;
        """
        
        # This should raise an exception
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            
            assert len(result) == 0, f"Expected no results for invalid format '{invalid_format}', got {len(result)} results"

    @pytest.mark.parametrize("case_format", ['PNG', 'JPG', 'Jpg', 'Png'])
    def test_as_format_case_insensitive(self, sql_connection, case_format):
        """
        Test that _as_format is case insensitive - uppercase formats should work.
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _blobs = TRUE
        AND _as_format = '{case_format}'
        LIMIT 5;
        """
        
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with image data
        for row in result:
            assert row[1] is not None, f"Expected _image to contain data when _as_format='{case_format}', got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _image to be bytes, got: {type(row[1])}"
            
            # Validate the image format (normalize to lowercase for comparison)
            expected_format = case_format.lower()
            assert validate_image_format(row[1], expected_format), f"Expected image to be in {expected_format} format, but validation failed"

    @pytest.mark.parametrize("format_val", FORMATS)
    def test_as_format_with_property_filtering(self, sql_connection, format_val):
        """
        Test that _as_format works with property filtering.
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _blobs = TRUE
        AND _as_format = '{format_val}'
        AND _uniqueid IS NOT NULL
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with image data
        for row in result:
            assert row[1] is not None, f"Expected _image to contain data when _as_format='{format_val}' with property filtering, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _image to be bytes, got: {type(row[1])}"
            
            # Validate the image format
            assert validate_image_format(row[1], format_val), f"Expected image to be in {format_val} format with property filtering, but validation failed"

    @pytest.mark.parametrize("format_val", FORMATS)
    def test_as_format_count_queries(self, sql_connection, format_val):
        """
        Test that COUNT queries work with _as_format.
        """
        sql = f"""
        SELECT COUNT(*) FROM system."Image" 
        WHERE _as_format = '{format_val}';
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
        
        assert result[0] >= 0, f"Expected count to be non-negative for _as_format='{format_val}', got: {result[0]}"

    @pytest.mark.parametrize("format_val", FORMATS)
    def test_as_format_combined_with_multiple_conditions(self, sql_connection, format_val):
        """
        Test that _as_format works with multiple WHERE conditions.
        """
        sql = f"""
        SELECT _uniqueid, _image FROM system."Image" 
        WHERE _blobs = TRUE
        AND _as_format = '{format_val}'
        AND _uniqueid IS NOT NULL
        AND _operations = OPERATIONS(RESIZE(width := 50, height := 50))
        LIMIT 5;
        """
        with sql_connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        
        # Should return rows with image data
        for row in result:
            assert row[1] is not None, f"Expected _image to contain data when _as_format='{format_val}' with multiple conditions, got None"
            assert isinstance(row[1], (bytes, memoryview)), f"Expected _image to be bytes, got: {type(row[1])}"
            
            # Validate the image format
            assert validate_image_format(row[1], format_val), f"Expected image to be in {format_val} format with multiple conditions, but validation failed"
