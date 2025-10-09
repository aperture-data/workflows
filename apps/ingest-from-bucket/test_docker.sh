#!/bin/bash
ARGS=$(getopt -o p:b: -l provider:,bucket: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1
fi

vars=()

PROVIDER=''
BUCKET=''
eval set -- "$ARGS"
while [ : ]; do
  case "$1" in
      -p | --provider)
        PROVIDER=$2
        shift 2
        ;;
      -b | --bucket)
        BUCKET=$2
        shift 2
        ;;
    *)
        break;;
  esac
done

if [ -z "$PROVIDER" ]; then
    echo "Need Provider"
    exit 1;
fi
if [ -z "$BUCKET" ]; then
    echo "Need Bucket"
    exit 1;
fi

if [[ "$PROVIDER" == "s3" ]]; then
    echo "* Provider = S3"
    # ensure we don't upload to a random bucket - only upload to key in environment.
    source aws.keys
    export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-"INVALID"}
    export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-"INVALID"}
    BUCKET=wf-test-priv-1
    vars+=(-e WF_AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID")
    vars+=(-e WF_AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY")
elif [[ "$PROVIDER" == "gs" ]]; then
    echo "* Provider = GS"
    vars+=(-e WF_GCP_SERVICE_ACCOUNT_KEY="$(cat gcp.key)")
else
    echo "Bad Provider (s3|gs)"
    exit 1
fi

vars+=(--add-host host.docker.internal:host-gateway )
vars+=(-e DB_HOST="host.docker.internal")
vars+=(-e WF_CLOUD_PROVIDER="$PROVIDER")
vars+=(-e WF_BUCKET="$BUCKET")
vars+=(-e WF_INGEST_IMAGES="true")

docker run --rm -it "${vars[@]}" aperturedata/workflows-ingest-from-bucket
