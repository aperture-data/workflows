# Example App

This application is an example that serves as a reference on how
to add new apps.

## Database details

This section will explain how the workflow interacts with the database, including any properties used.

## Running in docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS="password" \
           aperturedata/workflows-example
```

Parameters: 
* **`EXAMPLE`**: Brief description including default.

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Cleaning up

This section describes how to reverse the effect of a workflow.
Often this is a simple query in the ApertureDB Query Language.