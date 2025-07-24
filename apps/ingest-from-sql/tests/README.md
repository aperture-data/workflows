SQL data generation is done through CSV files to keep the format consistent
between ingest workflows.

This necessitates an extra step for blob loading as binary data doesn't exist in CSVs.

default connection is "postgresql+psycopg://root:root@127.0.0.1/aperturedb"
