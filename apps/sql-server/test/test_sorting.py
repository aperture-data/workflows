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
        
        # Extract Multicorn JSON from the plan
        plan = explain_plan[0]["Plan"]
        # In a limit query, the Foreign Scan might be under "Plans"
        if "Plans" in plan and "Multicorn" in plan["Plans"][0]:
            multicorn_str = plan["Plans"][0]["Multicorn"]
        else:
            multicorn_str = plan.get("Multicorn", "{}")
            
        multicorn_plan = json.loads(multicorn_str)
        aql = multicorn_plan.get("aql", [])
        
        # Check if any operation in AQL has 'sort'
        has_sort = False
        for op in aql:
            op_body = list(op.values())[0]
            if "sort" in op_body:
                has_sort = True
                assert op_body["sort"][0]["order"] == "ascending"
        assert has_sort, "Sort pushdown not found in AQL"

        cur.execute('SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid ASC LIMIT 10')
        results = cur.fetchall()
        assert len(results) > 0
        ids = [r[0] for r in results]
        assert ids == sorted(ids, key=lambda x: [int(p) if p.isdigit() else p for p in x.split('.')])

def test_order_by_desc(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute('EXPLAIN (FORMAT JSON) SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid DESC LIMIT 10')
        explain_plan = cur.fetchone()[0]
        
        plan = explain_plan[0]["Plan"]
        if "Plans" in plan and "Multicorn" in plan["Plans"][0]:
            multicorn_str = plan["Plans"][0]["Multicorn"]
        else:
            multicorn_str = plan.get("Multicorn", "{}")
            
        multicorn_plan = json.loads(multicorn_str)
        aql = multicorn_plan.get("aql", [])
        
        has_sort = False
        for op in aql:
            op_body = list(op.values())[0]
            if "sort" in op_body:
                has_sort = True
                assert op_body["sort"][0]["order"] == "descending"
        assert has_sort, "Sort pushdown not found in AQL"

        cur.execute('SELECT _uniqueid FROM system."Entity" ORDER BY _uniqueid DESC LIMIT 10')
        results = cur.fetchall()
        assert len(results) > 0
        ids = [r[0] for r in results]
        assert ids == sorted(ids, key=lambda x: [int(p) if p.isdigit() else p for p in x.split('.')], reverse=True)
