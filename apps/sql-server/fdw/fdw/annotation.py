from contextlib import AbstractContextManager
from pathlib import Path
from typing import Optional, Set, Dict
import logging

logger = logging.getLogger(__name__)

DIR = Path('/app/sql/annotations')

def quote_ident(name: str) -> str:
    """Safely double-quote a Postgres identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

def quote(value: str) -> str:
    """Safely single-quote string literals."""
    return "'" + value.replace("'", "''") + "'"


class SchemaAnnotations(AbstractContextManager):
    """Collects SQL annotations for all tables in a schema."""

    def __init__(self, schema: str):
        self.schema = schema
        self.tables: Dict[str, "TableAnnotations"] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.emit()
        return False

    # --- Factory for per-table proxies ---
    def for_table(self, table: str) -> "TableAnnotations":
        """Return a proxy for a specific table within this schema."""
        logger.info(f"Creating annotations for table {table}")
        assert table not in self.tables, f"Table {table} already has annotations; was {self.tables[table]}, now {table}"
        if table not in self.tables:
            self.tables[table] = TableAnnotations(self, table)
        return self.tables[table]

    def emit(self):
        path = DIR / f"{self.schema}.sql"
        logger.info(f"Emitting annotations for schema {self.schema} to {path}")
        assert path.parent.exists(), f"Directory {path.parent} does not exist"
        assert path.parent.is_dir(), f"Directory {path.parent} is not a directory"
        assert not path.exists(), f"File {path} already exists"
        sql = "\n".join(t.emit() for t in self.tables.values()) + "\n"
        path.write_text(sql)


class TableAnnotations:
    """Proxy for a specific table within a schema."""
    _comment: str = None
    _column_comments: dict[str, str] = {}
    _primary_key: str = None
    _foreign_keys: dict[str, tuple[str, str, str]] = {}

    def __init__(self, parent: SchemaAnnotations, table: str):
        self.parent = parent
        self.schema = parent.schema
        self.table = table

    def fq(self) -> str:
        """Fully qualified table name."""
        return f"{quote_ident(self.schema)}.{quote_ident(self.table)}"

    # --- Annotation methods ---
    def comment(self, comment: str):
        """Add comment to the table."""
        logger.info(f"Adding comment to table {self.table}: {comment}")
        assert self._comment is None, f"Schema {self.schema}, table {self.table} already has a comment; was {self._comment}, now {comment}"
        self._comment = comment

    def column_comment(self, column: str, comment: str):
        """Add comment to a column."""
        logger.info(f"Adding comment to column {column} of table {self.table}: {comment}")
        assert column not in self._column_comments, f"Schema {self.schema}, table {self.table}, column {column} already has a comment; was {self._column_comments[column]}, now {comment}"
        self._column_comments[column] = comment

    def primary_key(self, col="_uniqueid"):
        """Add primary key to the table.
        Assume single column primary key.
        """
        logger.info(f"Adding primary key to table {self.table}: {col}")
        assert self._primary_key is None, f"Schema {self.schema}, table {self.table} already has a primary key; was {self._primary_key}, now {col}"
        self._primary_key = col

    def foreign_key(self, column: str, 
        ref_schema: str, ref_table: str, ref_column: str = "_uniqueid"):
        """Add foreign key to the table."""
        logger.info(f"Adding foreign key to column {column} of table {self.table}: {ref_schema}, {ref_table}, {ref_column}")
        assert column not in self._foreign_keys, f"Schema {self.schema}, table {self.table}, column {column} already has a foreign key; was {self._foreign_keys[column]}, now {ref_schema}, {ref_table}, {ref_column}"
        self._foreign_keys[column] = (ref_schema, ref_table, ref_column)

    def emit(self) -> str:
        results = []

        if comment := self.get_comment():
            results.append(f"""COMMENT ON FOREIGN TABLE {self.fq()} IS {quote(comment)};""")

        for column in self.get_columns():
            if comment := self.get_column_comment(column):
                results.append(f"COMMENT ON COLUMN {self.fq()}.{quote_ident(column)} IS {quote(comment)};")

        return "\n".join(results)

    def get_comment(self) -> Optional[str]:
        """Get the comment for the table."""
        comment = self._comment
        pk = self._primary_key
        pk_comment = f"PRIMARY KEY ({quote_ident(pk)})" if pk else None
        return self.combine_comments(comment, pk_comment)

    def get_columns(self) -> Set[str]:
        """Get all columns in the table."""
        return set(self._column_comments.keys()) | set(self._foreign_keys.keys())

    def get_column_comment(self, column: str) -> Optional[str]:
        """Get the comment for a column."""
        comment = self._column_comments.get(column)
        fk = self._foreign_keys.get(column)
        fk_comment = f"REFERENCES {quote_ident(fk[0])}.{quote_ident(fk[1])}({quote_ident(fk[2])})" if fk else None
        return self.combine_comments(comment, fk_comment)


    def combine_comments(self, comment: Optional[str], auto_comment: Optional[str]) -> str:
        """Combine a manual comment and an auto-generated comment."""
        if comment and auto_comment:
            return f"{comment} - {auto_comment}"
        elif comment:
            return comment
        elif auto_comment:
            return auto_comment
        return None