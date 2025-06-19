# Ingesting datasets by their croissant URLs

This workflow can ingest a Dataset which is published with a croissant URL.

## Database details

This section will explain how the workflow interacts with the database, including any properties used.

## Running in docker

```
docker run \
           -e RUN_NAME=ingestion \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS="password" \
           aperturedata/workflows-ingest-croissant
```

Parameters:
* **`WF_CROISSANT_URL`**: Croissant URL of the published dataset.

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Cleaning up

This section describes how to reverse the effect of a workflow.
Often this is a simple query in the ApertureDB Query Language.