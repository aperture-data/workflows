import pytest
import psycopg2
import os
from dataclasses import dataclass
from typing import Any

# Database configuration
DATABASE_NAME = os.getenv("SQL_NAME", "aperturedb")

# List of schemas to test for read-only access
@dataclass
class Schema:
    schema: str
    table: str
    foreign: bool
    column: str
    value: str = "'test'"
    empty: bool = False

    @property
    def table_name(self):
        return f"\"{self.schema}\".\"{self.table}\""

    @property
    def maybe_foreign(self):
        return "FOREIGN " if self.foreign else ""

    @property 
    def is_writable(self):
        return self.table is not None and not self.foreign

    def __str__(self):
        return self.schema

    def __repr__(self):
        return self.schema

SCHEMAS = [
    Schema(schema="public", table=None, foreign=False, column=None, empty=True),
    Schema(schema="pg_catalog", table="pg_authid", foreign=False, column="rolname"),
    Schema(schema="descriptor", table="TestText_0", foreign=True, column="_uniqueid"),
    Schema(schema="entity", table="TestRow", foreign=True, column="_uniqueid"),
    Schema(schema="connection", table="edge", foreign=True, column="_uniqueid"),
    Schema(schema="system", table="Image", foreign=True, column="_uniqueid"),
]



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


def test_can_run_simple_select(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute("SELECT 42;")
        assert cur.fetchone()[0] == 42


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_create_table_in_schema(sql_connection, schema):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"CREATE TABLE {schema.schema}.\"forbidden\"(id INT);")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_drop_table_in_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"DROP {schema.maybe_foreign} TABLE IF EXISTS {schema.table_name};")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_alter_table_in_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"ALTER {schema.maybe_foreign} TABLE {schema.table_name} ADD COLUMN test_col INT;")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_create_index_in_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"CREATE INDEX IF NOT EXISTS test_idx ON {schema.table_name}({schema.column});")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_create_view_in_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"CREATE VIEW {schema.schema}.my_view AS SELECT {schema.table_name}.{schema.column} FROM {schema.table_name};")


def test_cannot_create_function_in_plpgsql(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute("DROP FUNCTION IF EXISTS harmless();")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("""
                CREATE FUNCTION harmless() RETURNS INTEGER AS $$
                BEGIN
                    RETURN 1;
                END;
                $$ LANGUAGE plpgsql;
            """)


def test_cannot_create_function_in_plpythonu(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute("DROP FUNCTION IF EXISTS evil();")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("""
                CREATE FUNCTION evil() RETURNS TEXT AS $$
                import os
                return os.environ.get("DB_PASS", "NOPE")
                $$ LANGUAGE plpythonu;
            """)


def test_pg_authid_access_blocked(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT * FROM pg_authid;")


def test_create_extension_blocked(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")


def test_no_create_trigger(sql_connection):
    with sql_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS trap CASCADE;")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("""
                CREATE TABLE trap(id INT);
                CREATE FUNCTION trap_fn() RETURNS trigger AS $$
                BEGIN
                    RAISE NOTICE 'Triggered';
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER trg BEFORE INSERT ON trap FOR EACH ROW EXECUTE FUNCTION trap_fn();
            """)


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_insert_into_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    if not schema.is_writable:
        pytest.skip("Skipping test for writable schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"INSERT INTO {schema.table_name} ({schema.column}) VALUES ({schema.value});")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_update_in_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    if not schema.is_writable:
        pytest.skip("Skipping test for writable schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"UPDATE {schema.table_name} SET {schema.column} = {schema.value} WHERE {schema.column} <> {schema.value};")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_delete_from_schema(sql_connection, schema):
    if schema.table is None:
        pytest.skip("Skipping test for public schema")
    if not schema.is_writable:
        pytest.skip("Skipping test for writable schema")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"DELETE FROM {schema.table_name} WHERE {schema.column} = {schema.value};")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_grant_privileges(sql_connection, schema):
    if schema.empty:
        pytest.skip("Skipping test for empty schema; nothing to grant")
    if schema.foreign:
        pytest.skip("Skipping test for foreign schema; empty of real tables")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema.schema} TO PUBLIC;")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_revoke_privileges(sql_connection, schema):
    if schema.empty:
        pytest.skip("Skipping test for empty schema; nothing to revoke")
    if schema.foreign:
        pytest.skip("Skipping test for foreign schema; empty of real tables")
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"REVOKE SELECT ON ALL TABLES IN SCHEMA {schema.schema} FROM PUBLIC;")


def test_cannot_create_schema(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE SCHEMA forbidden_schema;")


@pytest.mark.parametrize("schema", SCHEMAS)
def test_cannot_drop_schema(sql_connection, schema):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"DROP SCHEMA IF EXISTS {schema.schema};")


def test_cannot_create_sequence(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE SEQUENCE forbidden_seq;")


def test_cannot_create_domain(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE DOMAIN forbidden_domain AS INTEGER;")


def test_cannot_create_type(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE TYPE forbidden_type AS (id INTEGER, name TEXT);")


def test_cannot_reindex(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"REINDEX DATABASE {DATABASE_NAME};")


def test_cannot_copy_to_file(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("COPY (SELECT 1) TO '/tmp/forbidden.txt';")


def test_cannot_copy_from_file(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("COPY forbidden_table FROM '/tmp/forbidden.txt';")


def test_cannot_alter_database(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(f"ALTER DATABASE {DATABASE_NAME} SET log_statement = 'all';")


def test_cannot_alter_user(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("ALTER USER postgres PASSWORD 'forbidden';")


def test_cannot_create_user(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("CREATE USER forbidden_user;")


def test_cannot_drop_user(sql_connection):
    with sql_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("DROP USER IF EXISTS postgres;")