# SQL server workflow

This workflow makes an ApertureDB instance accessible through a PostgreSQL interface for read-only queries.
A simple HTTP API is also provided.
Every entity class and connection class in ApertureDB is exposed as a SQL table.

Instructions on how to access the server, including a Jupyter notebook tutorial,  are provided at https://docs.aperturedata.io/workflows/sql_server

## Running in docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.cloud.aperturedata.io \
           -e DB_PASS=password \
           -e WF_LOG_LEVEL=INFO \
           -e WF_AUTH_TOKEN=secretsquirrel \
           aperturedata/workflows-sql-server
```

Parameters: 
* **`WF_LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default WARNING.
* **`WF_AUTH_TOKEN`**: Password for the `aperturedb` user in Postgres, and bearer authentication token for REST API

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Implementation details

This workflow is implemented using 
a full [PostgreSQL](https://www.postgresql.org/) server,
its [Foreign Data Wrapper](https://www.postgresql.org/docs/current/fdwhandler.html) feature,
and the [Multicorn2](https://github.com/pgsql-io/multicorn2) Foreign Data Wrapper that supports implementation in Python.
All of these are automatically provisioned for you in the workflow image.

## Schemata and tables

This workflow provides four different types of table, added to four different schemata.

* **`system`**: Table for every system object type used in the database, e.g. `Descriptor`, `DescriptorSet`, `Image`, `BoundingBox`. In particular, there are tables for `Entity` and `Connection`, which will have any properties that are consistently-typed across all classes.
* **`entity`**: Table for every user-defined entity class, e.g. `CrawlDocument`.
* **`connection`**: Table for every user-defined connection class, e.g. `crawlDocumentHasBlob`. Note that in addition to the usual `_uniqueid`, connections also have columns for `_src` and `_dst`.
* **`descriptor`**: Table for every descriptor set.

> **Note**: Tables are published at startup, based on the ApertureDB schema at the time. If classes or properties are added later, they will not be available through this interface until the workflow is restarted. 

> **Tip**: The examples in this documentation use schema-qualified table names, e.g. `system."Image"`, but it is usually possible to omit the schema (e.g. `"Image"`), except when that would make the table name ambiguous.
This is done using the `search_path` feature of SQL.

> **Tip**: Because SQL table names are case-insensitive (strictly speaking, they are by default normalized to lower-case) and may not contain certain special characters, you will almost certainly need to double-quote the table names generated from ApertureDB.

> **Tip**: To see how your SQL query is converted into an ApertureDB query, use the `EXPLAIN` keyword in front of your `SELECT` query. The output will include the queries that will be sent to ApertureDB, one for each table. ([Batching](https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/batch) is used internally, and only the first query in each batch sequence is shown in `EXPLAIN`.)
> ```sql
> EXPLAIN SELECT * FROM entity."Person" WHERE age > 30;
>```

## Graph queries

ApertureDB is able to store property graphs, where nodes are connected by directed, labelled edges and both nodes and edges can have properties.
Nodes are represented by rows in tables from the `entity`, `system`, or `descriptor` schemata.
Edges are represented by rows in tables from the `connection` schema.
You can also use the `system."Connection"` table for unlabelled (but still directed) edges.
You can think of the connection tables as being like "join tables" or "[associative entities](https://en.wikipedia.org/wiki/Associative_entity).

Every table has a `_uniqueid` column, representing the system-generated ApertureDB property of the same name, acting as a primary key, and which is also used as a foreign key in the `_src` and `_dst` columns of connections tables.

> **Warning**: Like SQL row identifiers, the values seen in `_uniqueid`, `_src`, and `_dst` can be used within a session, but should not be stored long-term, as they are not guaranteed to survive migration and upgrade.

For example, if you have created entity classes `Person` and `Address` and a connection class `personAddress`, then you could write a SQL query like:

```sql
SELECT A.*, C.* 
    FROM entity."Person" AS A
    INNER JOIN connection."personAddress" AS B 
        ON A._uniqueid = B._src
    INNER JOIN entity."Address" AS C
        ON B._dst = C._uniqueid
    WHERE C.state = "CA"
    ORDER BY A.last_transaction_date DESC;
```

## Object retrieval 

Certain ApertureDB system object types have associated binary data, known as blobs.
It is possible to retrieve these blobs in SQL, and even perform certain transformations on them, using the ApertureDB [operations](https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/operations) feature.

Objects retrieval can be expensive because the blobs can be arbitrarily large.
For this reason, ApertureDB only returns blobs when they are explicitly requested.
A special boolean column `_blobs` is provided to enable object retrieval.
This is to avoid the problem that `SELECT *` would otherwise implicitly request blob downloads.

Selected object types have special columns that will contain the blob data:

| Table | Blob column |
| --- | --- |
| `system."Blob"` | `_blob` |
| `system."Image"` | `_image` |
| `descriptor.*` | `_vector` |

> **Note**: It is important to distinguish the control flag `_blobs` from the `_blob` data field.

For example, to fetch blob data, you might use the following SQL:

```sql
SELECT _blob FROM system."Blob"
    WHERE foo = "bar"
    AND _blobs
```

The `_blob` column will contain `BYTEA` data.

> **Tip**: When using the `_blobs` field in a WHERE clause, it can be a plain variable as above, or can be explicitly tested with `_blobs = TRUE` or `_blobs IS TRUE`. To avoid downloading blobs, the `_blobs` clause can be omitted, or negated in the usual way with `NOT _blobs`, `_blobs <> TRUE`, `_blobs = FALSE`, `_blobs IS FALSE`, or `_blobs IS NOT TRUE`.

### Image operations

Certain tables also have special control columns that manipulate the returned blob when used as WHERE clauses.

The `system."Image"` table provides:
* **`_as_format`**: This controls the content type of the returned image, and can be either "png" or "jpg".
* **`_operations`**:  This applies an [operations](https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/operations) pipeline. It should always be a call to the provided `OPERATIONS` function, which in turn should be passed invocations of functions from the list: `THRESHOLD`, `RESIZE`, `CROP`, `ROTATE`, `FLIP`. See the ApertureDB documentation for the parameters of these functions.

For example:
```sql
SELECT * FROM system."Image" 
    WHERE _blobs
    AND _operations = OPERATIONS(
        THRESHOLD(64), 
        CROP(x:=10, y:=10, width:=200, height:=200),
        FLIP(+1),
        ROTATE(angle:=90),
        RESIZE(width:=50))
    AND _as_format = 'jpg'
LIMIT 5;
```

## Similarity search

ApertureDB also acts as a vector store, allowing you to store a set of vectors generated, and then search for the stored vectors that are closest to some query vector. These vectors are typically generated using some machine-learning "embedding" model, and usually represent images or text segments.

The SQL interface builds on this capability by allowing you to provide a query text or image blob and automatically invoke the appropriate embedding model.

On tables in the `descriptor` schema, there is a special column `_find_similar` that can be used in a WHERE clause to invoke a vector search. This should be constrained to be equal the result of the provided `FIND_SIMILAR` function, which takes the following parameters:
* **`text`**: A text string to embed and search for
* **`image`**: A `BYTEA` binary blob to embed and search for
* **`vector`**: An explicit JSONB vector of numbers to search for.
* **`k`**: (Default 10) The number of results to retrieve. See `k_neighbors` in [FindDescriptor](https://docs.aperturedata.io/query_language/Reference/descriptor_commands/desc_commands/FindDescriptor)
* **`knn_first`**: (Default TRUE) Whether to perform the vector search before or after applying constraints. See `knn_first` in [FindDescriptor](https://docs.aperturedata.io/query_language/Reference/descriptor_commands/desc_commands/FindDescriptor)

Exactly one of `text`, `image` and `vector` should be specified.

For example:
```sql
SELECT * FROM descriptor."crawl-to-rag"
    WHERE _find_similar = FIND_SIMILAR(
        text := "find entity",
        k := 10)
    LIMIT 10;
```

> **Note**: Not all `descriptor` tables support similar search. This is because the corresponding `DescriptorSet` has not been annotated in a way that permits the wrapper to determine the appropriate embedding model. In such cases, the table will not have a `_find_similar` column.