import os
import json
import pytest
import psycopg2
from warnings import warn


class UnsupportedConstraintFullFilterWarning(Warning):
    """Warning for unsupported constraints that are fully pushed down.
    Something unexpected happened, as these constraints should not be supported.
    Maybe support was added, and the test needs to be updated?
    """
    pass


class UnsupportedConstraintPartialFilterWarning(Warning):
    """Warning for unsupported constraints that are partially-filtered.
    Something unexpected happened, as these constraints should not be supported.
    But the support is only partial, so they cannot be marked as fully supported.
    """
    pass


@pytest.fixture(scope="session")
def sql_connection():
    conn = psycopg2.connect(
        host=os.getenv("SQL_HOST", "sql-server"),
        port=os.getenv("SQL_PORT", "5432"),
        dbname=os.getenv("SQL_NAME", "aperturedb"),
        user=os.getenv("SQL_USER", "aperturedb"),
        password=os.getenv("SQL_PASS", "test"),
    )
    # This line makes sure that one crash doesn't leave the connection in a bad state
    conn.autocommit = True
    yield conn
    conn.close()


# We expect these constraints to be fully pushed down by the FDW.
SUPPORTED_CONSTRAINTS = [
    # boolean
    "b", "b = TRUE", "b <> TRUE", "b IS TRUE", "b IS NOT TRUE",
    "NOT b", "b = FALSE", "b <> FALSE", "b IS FALSE", "b IS NOT FALSE",
    "b IS NULL", "b IS NOT NULL", "b IN (TRUE)", "b NOT IN (TRUE)",
    "b IN (FALSE)", "b NOT IN (FALSE)",
    "b IN (TRUE, FALSE)",
    "b = NULL", "b <> NULL",
    # PG boolean ordering
    "b < TRUE", "b <= TRUE", "b > TRUE", "b >= TRUE",
    "b < FALSE", "b <= FALSE", "b > FALSE", "b >= FALSE",
    # number
    "n = 1",  "n > 1", "n >= 1", "n < 1", "n <= 1",
    "n IS NULL", "n IS NOT NULL", "n IN (0,1)",
    # string
    "s = 'b'", "s > 'b'", "s >= 'b'", "s < 'b'", "s <= 'b'",
    "s IS NULL", "s IS NOT NULL", "s IN ('a', 'b')",
    # combos
    "b = TRUE AND n > 1",
    "b IS TRUE AND s = 'b'",
    "b IS NOT TRUE AND n < 1",
    "b IS NULL AND s IS NOT NULL",
]

# Not expected to filter, but should not crash or false-filter
UNSUPPORTED_CONSTRAINTS = [
    # These feel like they ought to be supported,
    # but SQL and AQL have different rules for NULL handling
    "b NOT IN (TRUE, FALSE)",
    "n <> 1",
    "n NOT IN (0,1)",
    "s <> 'b'",
    "s NOT IN ('a', 'b')",

    # Maybe these will be supported in the future, but currently not
    "b = TRUE OR n > 1",
    "b IS TRUE OR s = 'b'",
    "b IS NOT TRUE OR n < 1",
    "b IS NULL OR s IS NOT NULL",

    # We will never be able to support these in AQL
    "s LIKE 'b%'",
    "s ILIKE 'b%'",
    "s SIMILAR TO 'b%'",
    "s ~ 'b'",
]

CONSTRAINTS = [(constraint, True) for constraint in SUPPORTED_CONSTRAINTS] + \
    [(constraint, False) for constraint in UNSUPPORTED_CONSTRAINTS]
IDS = [f"{constraint} ({'supported' if supported else 'unsupported'})" for constraint,
       supported in CONSTRAINTS]


@pytest.mark.parametrize("constraint,supported", CONSTRAINTS, ids=IDS)
def test_constraints(constraint, supported, sql_connection):
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
        multicorn_plan1 = multicorn_plan(plan1)

        cur.execute(sql2)
        result2 = cur.fetchone()[0]
        if isinstance(result2, str):
            result2 = json.loads(result2)
        plan2 = result2[0]["Plan"]

    print(json.dumps(plan1))
    rows1 = plan_actual_rows(plan1)
    rows2 = plan_actual_rows(plan2)
    removed1 = sum_rows_removed_by_filter(plan1)
    removed2 = sum_rows_removed_by_filter(plan2)

    # Debug line that shows up in pytest -q
    print(f"{constraint}: rows1={rows1} rows2={rows2} removed_by_filter={removed1} - {multicorn_plan1}")

    # False negatives? (pushdown missed rows)
    # This is severe, as the user will fail to receive rows they expect.
    assert rows1 == rows2, f"SEVERE: Rowcount mismatch for: {constraint} - {multicorn_plan1}"

    # False positives? (server over-returned; PG had to filter)
    # This is mild, as the user will still receive rows they expect,
    # but at higher cost.
    if supported:  # We expected these to work
        assert removed1 == 0, f"MILD: Rows removed by post-filter for: {constraint} → {removed1} - {multicorn_plan1}"
    elif removed1 == 0:  # Expected not to work, but it did
        warn(
            f"Unsupported constraint {constraint} was fully pushed down, suggesting it is now supported - {multicorn_plan1}",
            UnsupportedConstraintFullFilterWarning
        )
    elif removed1 > 0 and removed1 != removed2:  # expected not to work, but it did partially
        warn(
            f"Unsupported constraint {constraint} was partially pushed down, might be OK - {multicorn_plan1}",
            UnsupportedConstraintPartialFilterWarning
        )
    # Else expected not to work, and it did not, so no warning needed


def plan_actual_rows(plan_node: dict) -> int:
    """Actual Rows of the top plan (whole query)."""
    return int(plan_node.get("Actual Rows", 0))


def sum_rows_removed_by_filter(plan_node: dict) -> int:
    """Sum 'Rows Removed by Filter' over the whole tree."""
    removed = int(plan_node.get("Rows Removed by Filter", 0))
    for child in plan_node.get("Plans", []) or []:
        removed += sum_rows_removed_by_filter(child)
    return removed


def multicorn_plan(plan_node: dict) -> str:
    """Extract the Multicorn plan from the plan node."""
    if "Multicorn" in plan_node:
        return json.dumps(json.loads(plan_node["Multicorn"]))
    return None
