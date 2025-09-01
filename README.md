# Workflows

ApertureDB Workflows make it easy to perform certain common tasks, like ingesting datasets and adding embeddings or bounding boxes.
The most convenient way to execute a workflow against an ApertureDB Cloud instance is via [the ApertureDB Cloud interface](https://cloud.aperturedata.io/).
See [the documentation](https://docs.aperturedata.io/workflows) for more information.

## About this repository

This repository contains the code used to implement various ApertureDB workflows and build them into Docker images.
This will allow you to do various things that you can't do via the UI:
* Execute workflows against non-Cloud ApertureDB instances.
* Access various advanced options.
* Find out details of what the code is doing in the database.
* Copy and adapt the underlying code.

## List of workflows

* [Dataset Ingestion](apps/dataset-ingestion/): Load various public datasets.
* [Embeddings Extraction](apps/embeddings-extraction/): Automatically generate embeddings for images.
* [Face Detection](apps/face-detection/): Find faces within images and generate bounding boxes. Optionally generate embeddings for the extracted faces.
* [Object Detection](apps/object-detection/): Detect objects within images and add labelled bounding boxes.
* [Jupyter Lab](apps/jupyterlab/): Run a Jupyter Notebook server that has access to your ApertureDB instance.
* [Crawl to RAG](apps/crawl-to-rag/): Retrieve data from a website and runs a chatbot which can use RAG to intelligently reference data from the website.
* [Crawl Website](apps/crawl-website/): Retrieve data from a website and store it in ApertureDB.
* [Ingest Croissant](apps/ingest-croissant/): Ingests data using a Crossiant file.
* [Ingest From Bucket](apps/ingest-from-bucket/): Ingests data from a GCP or S3 bucket.
* [Ingest From SQL](apps/ingest-from-sql/): Ingests data from a PostgreSQL server.
* [Label Studio](apps/ingest-croissant/): Hosts a Label Studio instance with automatic configuration for ApertureDB.
* [MCP Server](apps/ingest-croissant/): Hosts a MCP server which exposes ApertureDB as a resource for LLMs.
* [RAG](apps/ingest-croissant/): Host a Chatbot using pre-embedded text documents in ApertureDB.
* [SQL Server](apps/sql-server/): Host a SQL server which retreives information from ApertureDB.
* [Text Embeddings](apps/text-embedding/): Creates text embeddings from text segments.
* [Text Extraction](apps/text-extraction/): Creates text segments from raw documents.

## Docker

This is the general pattern for running a workflow.
Every workflow requires the host, user name, and password for your ApertureDB instance.
Workflows typically have additional parameters.

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=<HOSTNAME> \
           -e DB_USER=<USERNAME> \
           -e DB_PASS="<PASSWORD>" \
           aperturedata/workflows-<APP-DIR>
```

### Common parameters

Certain parameters are supported by all workflow images:
* **`RUN_NAME`**: A name for this run. May be used in internal logging or reporting.
* **`APERTUREDB_KEY`**: API key for access to an ApertureDB instance. This replaces all of the options below, which are still supported for back-wards compatibility.
* **`DB_HOST`**: Hostname of the ApertureDB instance
* **`DB_PORT`**: Port number for the ApertureDB instance. Defaults to `55555`.
* **`DB_USER`**: ApertureDB user name. Defaults to `admin`.
* **`DB_PASS`**: ApertureDB password.
* **`USE_SSL`**: Use SSL to protect the connection. Defaults to `true`. We recommend that you do not change this.
* **`USE_REST`**: Use the REST API instead of the TCP connection. Defaults to `false`. We recommend that you do not change this.

### Building Docker images

There is a `build.sh` script in the `/apps` directory that can be used to build any of the workflow images. Either invoke it with a workflow name as a parameter:
```
apps/build.sh rag
```
or invoke it from the workflow directory:
```
cd apps/rag
../build.sh
```
