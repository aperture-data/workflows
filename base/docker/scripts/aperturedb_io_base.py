from typing import Optional, Iterator, Tuple
from aperturedb.CommonLibrary import Connector
from connection_pool import ConnectionPool


class AperturedbIOBase:
    """
    Base class for ApertureDB IO operations.
    """

    def __init__(self,
                 input_spec_id: Optional[str] = None,
                 spec_id: Optional[str] = None,
                 run_id: Optional[str] = None,
                 client: Optional[Connector] = None,
                 pool: Optional[ConnectionPool] = None):
        self.input_spec_id = input_spec_id
        self.spec_id = spec_id
        self.run_id = run_id

        if client is None and pool is None:
            pool = ConnectionPool()
        if client is not None and pool is not None:
            raise ValueError(
                "Either client or pool must be provided, not both.")
        self.client = client
        self.pool = pool

    def execute_query(self,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        """Execute a query on ApertureDB and return the results
        """
        if self.client is not None:
            status, results, result_blobs = execute_query(
                client=self.client,
                query=query,
                blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
            )
            return results, result_blobs
        else:
            status, results, result_blobs = self.pool.execute_query(
                query=query,
                blobs=blobs,
                strict_response_validation=strict_response_validation,
                success_statuses=success_statuses
            )
            return results, result_blobs
