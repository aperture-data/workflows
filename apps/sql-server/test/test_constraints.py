import os
import json
import pytest
import psycopg2


@pytest.fixture(scope="session")
def sql_connection():
    conn = psycopg2.connect(
        host=os.getenv("SQL_HOST", "sql-server"),
        port=os.getenv("SQL_PORT", "5432"),
        dbname=os.getenv("SQL_NAME", "aperturedb"),
        user=os.getenv("SQL_USER", "aperturedb"),
        password=os.getenv("SQL_PASS", "test"),
    )
    yield conn
    conn.close()


CONSTRAINTS = [
    # boolean
    "b", "b = TRUE", "b <> TRUE", "b IS TRUE", "b IS NOT TRUE",
    "NOT b", "b = FALSE", "b <> FALSE", "b IS FALSE", "b IS NOT FALSE",
    "b IS NULL", "b IS NOT NULL", "b IN (TRUE)", "b NOT IN (TRUE)",
    "b IN (FALSE)", "b NOT IN (FALSE)", 
    "b IN (TRUE, FALSE)", "b NOT IN (TRUE, FALSE)", 
    # PG boolean ordering
    "b < TRUE", "b <= TRUE", "b > TRUE", "b >= TRUE",
    "b < FALSE", "b <= FALSE", "b > FALSE", "b >= FALSE",
    # number
    "n = 1", "n <> 1", "n > 1", "n >= 1", "n < 1", "n <= 1",
    "n IS NULL", "n IS NOT NULL", "n IN (0,1)", "n NOT IN (0,1)",
    # string
    "s = 'b'", "s <> 'b'", "s > 'b'", "s >= 'b'", "s < 'b'", "s <= 'b'",
    "s IS NULL", "s IS NOT NULL", "s IN ('a', 'b')", "s NOT IN ('a', 'b')",
    # combos
    "b = TRUE AND n > 1",
    "b = TRUE OR n > 1",
    "b IS TRUE AND s = 'b'",
    "b IS TRUE OR s = 'b'",
    "b IS NOT TRUE AND n < 1",
    "b IS NOT TRUE OR n < 1",
    "b IS NULL AND s IS NOT NULL",
    "b IS NULL OR s IS NOT NULL",
]


@pytest.mark.parametrize("constraint", CONSTRAINTS, ids=CONSTRAINTS)
def test_constraints(constraint, sql_connection):
    # 1) fully-pushed constraints
    sql1 = f"""
      EXPLAIN (ANALYZE, FORMAT JSON)
      SELECT * FROM "TestRow"
      WHERE {constraint};
    """
    # 2) force post-filter with MATERIALIZED
    sql2 = f"""
      EXPLAIN (ANALYZE, FORMAT JSON)
      WITH q AS MATERIALIZED (SELECT * FROM "TestRow")
      SELECT * FROM q WHERE {constraint};
    """

    with sql_connection.cursor() as cur:
        cur.execute(sql1)
        result1 = cur.fetchone()[0]
        if isinstance(result1, str):
            result1 = json.loads(result1)
        plan1 = result1[0]["Plan"]

        cur.execute(sql2)
        result2 = cur.fetchone()[0]
        if isinstance(result2, str):
            result2 = json.loads(result2)
        plan2 = result2[0]["Plan"]

    print(json.dumps(plan1))
    rows1 = plan_actual_rows(plan1)
    rows2 = plan_actual_rows(plan2)
    removed1 = sum_rows_removed_by_filter(plan1)

    # Debug line that shows up in pytest -q
    print(f"{constraint}: rows1={rows1} rows2={rows2} removed_by_filter={removed1}, constraints={get_constraint(plan1)}")

    # False negatives? (pushdown missed rows)
    assert rows1 == rows2, f"Rowcount mismatch for: {constraint} - {get_constraint(plan1)}"

    # False positives? (server over-returned; PG had to filter)
    assert removed1 == 0, f"Rows removed by Filter for: {constraint} → {removed1} - {get_constraint(plan1)}"


def plan_actual_rows(plan_node: dict) -> int:
    """Actual Rows of the top plan (whole query)."""
    return int(plan_node.get("Actual Rows", 0))


def sum_rows_removed_by_filter(plan_node: dict) -> int:
    """Sum 'Rows Removed by Filter' over the whole tree."""
    removed = int(plan_node.get("Rows Removed by Filter", 0))
    for child in plan_node.get("Plans", []) or []:
        removed += sum_rows_removed_by_filter(child)
    return removed

def aperturedb_query(plan_node: dict) -> list:
    if 'Multicorn' in plan_node:
        s = plan_node['Multicorn'].split(":", 1)[1]
        return json.loads(s)
    return None

def get_constraint(plan_node: dict) -> dict:
    query = aperturedb_query(plan_node)
    if query:
        body = list(query[0].values())[0]
        return body.get('constraints', {})
    return {}