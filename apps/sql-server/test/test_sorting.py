import os
import pytest
import psycopg2
import json

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

def test_order_by_asc(sql_connection):
    with sql_connection.cursor() as cur:
        # Assuming we have some entities in ApertureDB we can sort
        # E.g. entities from system."Entity"
        cur.execute('EXPLAIN (FORMAT JSON) SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid ASC LIMIT 10')
        explain_plan = cur.fetchone()[0]
        explain_str = json.dumps(explain_plan)
        assert '"sort"' in explain_str
        assert '"order": "ascending"' in explain_str

        cur.execute('SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid ASC LIMIT 10')
        results = cur.fetchall()
        assert len(results) > 0
        ids = [r[0] for r in results]
        assert ids == sorted(ids)

def test_order_by_desc(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute('EXPLAIN (FORMAT JSON) SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid DESC LIMIT 10')
        explain_plan = cur.fetchone()[0]
        explain_str = json.dumps(explain_plan)
        assert '"sort"' in explain_str
        assert '"order": "descending"' in explain_str

        cur.execute('SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid DESC LIMIT 10')
        results = cur.fetchall()
        assert len(results) > 0
        ids = [r[0] for r in results]
        assert ids == sorted(ids, reverse=True)
