# Running a Label Studio Integration

This workflow runs a label studio instance with integration into ApertureDB

## Workflow details

`label-studio` makes images from the database available to the Label Studio application and stores user created annotations back in the database.

Label Studio is available on port 8000 of the docker container.


## Running in docker

```
docker run \
           -e RUN_NAME=label-studio-app \
           -e DB_HOST=aperturedb.gcp.cloud.aperturedata.dev \
           -e DB_PASS="adb-password" \
           aperturedata/workflows-label-studio
```

Parameters:
None

See [Common Parameters](../../README.md#common-parameters) for common parameters.

