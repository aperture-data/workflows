from contextlib import contextmanager

from aperturedb_workflows.connection_pool import ConnectionPool


class FreshConnectionPool(ConnectionPool):
    """Keep pool capacity while opening each borrowed transport immediately before use."""

    @contextmanager
    def get_connection(self):
        with super().get_connection() as template:
            client = template.clone()
            try:
                yield client
            finally:
                client.close()
