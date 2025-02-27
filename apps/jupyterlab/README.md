# JupyterLab App

This workflow runs a Jupyter Lab server that can be used to access your ApertureDB instance.
It is prepopulated with certain tutorial notebooks.

## Running in Docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS="password" \
           aperturedata/workflows-jupyterlab
```
