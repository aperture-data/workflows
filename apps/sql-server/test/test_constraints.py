import os
import json
import pytest
import psycopg2
from warnings import warn
from dataclasses import dataclass


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


@pytest.fixture(scope="session")
def constraint_formatter(sql_connection):
    """
    Fixture to format constraints for testing.
    This allows us to use the unique IDs from the TestRow table.
    """
    sql = """SELECT _uniqueid FROM "TestRow" LIMIT 5;"""
    with sql_connection.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()
    # extract unique IDs for the test
    unique_ids = [row[0] for row in result]
    unique_ids_str = '(' + ', '.join(f"'{uid}'" for uid in unique_ids) + ')'
    unique_id_str = f"'{unique_ids[2]}'"

    sql = """SELECT _src, _dst, _uniqueid FROM "edge";"""
    with sql_connection.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()
    src_unique_ids = [row[0] for row in result]
    src_unique_ids_str = '(' + \
        ', '.join(f"'{uid}'" for uid in src_unique_ids) + ')'
    dst_unique_ids = [row[1] for row in result]
    dst_unique_ids_str = '(' + \
        ', '.join(f"'{uid}'" for uid in dst_unique_ids) + ')'
    edge_unique_ids = [row[2] for row in result]
    edge_unique_ids_str = '(' + \
        ', '.join(f"'{uid}'" for uid in edge_unique_ids) + ')'

    return lambda constraint: constraint.format(
        unique_id=unique_id_str,
        unique_ids=unique_ids_str,
        src_unique_ids=src_unique_ids_str,
        dst_unique_ids=dst_unique_ids_str,
        edge_unique_ids=edge_unique_ids_str,
    )


@dataclass
class ConstraintTestCase:
    """
    Represents a test case for constraints.
    """
    constraint: str
    expected_pushdown: bool = True
    expected_empty: bool = False

    def get_no_filtering(self) -> str:
        return self.constraint

    def get_pushdown(self) -> str:
        if self.expected_pushdown:
            return self.constraint
        else:  # expected to fail
            return pytest.param(self.constraint, marks=pytest.mark.xfail(reason="Unsupported constraint should not be fully pushed down"))


TEST_CASES = [
    ConstraintTestCase("b"),
    ConstraintTestCase("b = TRUE"),
    ConstraintTestCase("b <> TRUE"),
    ConstraintTestCase("b IS TRUE"),
    ConstraintTestCase("b IS NOT TRUE"),
    ConstraintTestCase("NOT b"),
    ConstraintTestCase("b = FALSE"),
    ConstraintTestCase("b <> FALSE"),
    ConstraintTestCase("b IS FALSE"),
    ConstraintTestCase("b IS NOT FALSE"),
    ConstraintTestCase("b IS NULL"),
    ConstraintTestCase("b IS NOT NULL"),
    ConstraintTestCase("b IN (TRUE)"),
    ConstraintTestCase("b NOT IN (TRUE)"),
    ConstraintTestCase("b IN (FALSE)"),
    ConstraintTestCase("b NOT IN (FALSE)"),
    ConstraintTestCase("b IN (TRUE, FALSE)"),
    ConstraintTestCase("b = NULL"),
    ConstraintTestCase("b <> NULL"),
    ConstraintTestCase("b < TRUE"),
    ConstraintTestCase("b <= TRUE"),
    ConstraintTestCase("b > TRUE"),
    ConstraintTestCase("b >= TRUE"),
    ConstraintTestCase("b < FALSE"),
    ConstraintTestCase("b <= FALSE"),
    ConstraintTestCase("b > FALSE"),
    ConstraintTestCase("b >= FALSE"),
    ConstraintTestCase("n = 1"),
    ConstraintTestCase("n > 1"),
    ConstraintTestCase("n >= 1"),
    ConstraintTestCase("n < 1"),
    ConstraintTestCase("n <= 1"),
    ConstraintTestCase("n IS NULL"),
    ConstraintTestCase("n IS NOT NULL"),
    ConstraintTestCase("n IN (0,1)"),
    ConstraintTestCase("s = 'b'"),
    ConstraintTestCase("s > 'b'"),
    ConstraintTestCase("s >= 'b'"),
    ConstraintTestCase("s < 'b'"),
    ConstraintTestCase("s <= 'b'"),
    ConstraintTestCase("s IS NULL"),
    ConstraintTestCase("s IS NOT NULL"),
    ConstraintTestCase("s IN ('a', 'b')"),
    ConstraintTestCase("b = TRUE AND n > 1"),
    ConstraintTestCase("b IS TRUE AND s = 'b'"),
    ConstraintTestCase("b IS NOT TRUE AND n < 1"),
    ConstraintTestCase("b IS NULL AND s IS NOT NULL"),
    ConstraintTestCase("_uniqueid = {unique_id}"),
    ConstraintTestCase("_uniqueid <> {unique_id}"),
    ConstraintTestCase("_uniqueid IN {unique_ids}"),
    ConstraintTestCase("_uniqueid NOT IN {unique_ids}"),
    ConstraintTestCase("_uniqueid IS NOT NULL"),
    ConstraintTestCase("b NOT IN (TRUE, FALSE)", expected_pushdown=False),
    ConstraintTestCase("n <> 1", expected_pushdown=False),
    ConstraintTestCase("n NOT IN (0,1)", expected_pushdown=False),
    ConstraintTestCase("s <> 'b'", expected_pushdown=False),
    ConstraintTestCase("s NOT IN ('a', 'b')", expected_pushdown=False),
    ConstraintTestCase("b = TRUE OR n > 1", expected_pushdown=False),
    ConstraintTestCase("b IS TRUE OR s = 'b'", expected_pushdown=False),
    ConstraintTestCase("b IS NOT TRUE OR n < 1", expected_pushdown=False),
    ConstraintTestCase("b IS NULL OR s IS NOT NULL", expected_pushdown=False),
    ConstraintTestCase("_uniqueid IS NULL",
                       expected_pushdown=False, expected_empty=True),
    ConstraintTestCase("s LIKE 'b%'", expected_pushdown=False),
    ConstraintTestCase("s ILIKE 'b%'", expected_pushdown=False),
    ConstraintTestCase("s SIMILAR TO 'b%'", expected_pushdown=False),
    ConstraintTestCase("s ~ 'b'", expected_pushdown=False),
    ConstraintTestCase("_uniqueid LIKE {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid ILIKE {unique_id}", expected_pushdown=False),
    ConstraintTestCase(
        "_uniqueid SIMILAR TO {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid ~ {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid >= {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid <= {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid < {unique_id}", expected_pushdown=False),
    ConstraintTestCase("_uniqueid > {unique_id}", expected_pushdown=False),


]


@pytest.mark.parametrize("constraint", [tc.get_no_filtering() for tc in TEST_CASES])
def test_no_filtering(constraint, sql_connection, constraint_formatter):
    """
    Test that we don't filter out rows that should be returned.
    No constraint should fail this, regardless of whether it is supported or not.
    """
    data = _test_constraints_inner(
        constraint, sql_connection, constraint_formatter)
    assert data["rows1"] == data[
        "rows2"], f"SEVERE: Rowcount mismatch for: {constraint} - {data['multicorn_plan1']}"


@pytest.mark.parametrize("constraint", [tc.get_pushdown() for tc in TEST_CASES])
def test_pushdown(constraint, sql_connection, constraint_formatter):
    """
    Test that we filter out rows that should not be returned.
    This indicates successful pushdown of the constraint.
    """
    data = _test_constraints_inner(
        constraint, sql_connection, constraint_formatter)
    assert data["removed1"] == 0, f"Rows removed by post-filter for: {constraint} → {data['removed1']} - {data['multicorn_plan1']}"


def _test_constraints_inner(constraint, sql_connection, constraint_formatter):
    constraint = constraint_formatter(constraint)
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

    rows1 = plan_actual_rows(plan1)
    rows2 = plan_actual_rows(plan2)
    removed1 = sum_rows_removed_by_filter(plan1)
    removed2 = sum_rows_removed_by_filter(plan2)

    # Debug line that shows up in pytest -q
    print(f"{constraint}: rows1={rows1} rows2={rows2} removed_by_filter={removed1} - {multicorn_plan1}")

    return dict(
        constraint=constraint,
        rows1=rows1,
        rows2=rows2,
        removed1=removed1,
        removed2=removed2,
        multicorn_plan1=multicorn_plan1,
    )


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


@pytest.mark.parametrize("query", [
    "SELECT source_key FROM \"SourceNode\";",
    "SELECT destination_key FROM \"DestinationNode\";",
    "SELECT edge_key FROM \"edge\";",
    "SELECT source_key, edge_key FROM \"SourceNode\" AS A JOIN \"edge\" AS B ON B._src = A._uniqueid;",
    "SELECT source_key, edge_key FROM \"SourceNode\" AS A JOIN \"edge\" AS B ON B._src = A._uniqueid WHERE source_key = edge_key;",
    "SELECT destination_key, edge_key FROM \"DestinationNode\" AS A JOIN \"edge\" AS B ON B._dst = A._uniqueid;",
    "SELECT destination_key, edge_key FROM \"DestinationNode\" AS A JOIN \"edge\" AS B ON B._dst = A._uniqueid WHERE destination_key = edge_key;",
    "SELECT source_key, destination_key FROM \"SourceNode\" AS A JOIN \"edge\" AS B ON A._uniqueid = B._src JOIN \"DestinationNode\" AS C ON B._dst = C._uniqueid;",
    "SELECT source_key, destination_key FROM \"SourceNode\" AS A JOIN \"edge\" AS B ON A._uniqueid = B._src JOIN \"DestinationNode\" AS C ON B._dst = C._uniqueid WHERE source_key = destination_key;",
    "SELECT source_key FROM \"SourceNode\" WHERE _uniqueid in {src_unique_ids};",
    "SELECT destination_key FROM \"DestinationNode\" WHERE _uniqueid in {dst_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _uniqueid in {edge_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _src IN {src_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _dst IN {dst_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _src IN {src_unique_ids} AND _dst IN {dst_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _uniqueid in {edge_unique_ids} AND _src IN {src_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _uniqueid in {edge_unique_ids} AND _dst IN {dst_unique_ids};",
    "SELECT edge_key FROM \"edge\" WHERE _uniqueid in {edge_unique_ids} AND _src IN {src_unique_ids} AND _dst IN {dst_unique_ids};",
])
def test_join_query(query, sql_connection, constraint_formatter):
    """
    Test the execution of join queries.
    """
    query = constraint_formatter(query)
    try:
        with sql_connection.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
    except Exception as e:
        print(
            f"Error executing query: {e} - {query} - {query_aql(query, sql_connection)}")

    assert len(
        result) == 5, f"Expected 5 rows, got {len(result)} for query: {query}, {query_aql(query, sql_connection)}"


def query_aql(query: str, sql_connection) -> str:
    query = f"EXPLAIN (FORMAT JSON) {query};"
    with sql_connection.cursor() as cur:
        cur.execute(query)
        result1 = cur.fetchone()[0]
    if isinstance(result1, str):
        result1 = json.loads(result1)
    plan1 = result1[0]["Plan"]
    multicorn_plan1 = multicorn_plan(plan1)
    aql = json.loads(multicorn_plan1).get("aql", "")
    return aql
