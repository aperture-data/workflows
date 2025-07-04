#!/bin/bash


ARGS=$(getopt -o p:b: -l provider:,bucket: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1
fi

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
    source ../aws.keys
    export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-"INVALID"}
    export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-"INVALID"}
    set -e
    aws s3 ls s3://${BUCKET} 
    aws s3  cp  ${PWD}/generated/  s3://${BUCKET}/videos/  --recursive --exclude="*" --include="*.mp4"
elif [[ "$PROVIDER" == "gs" ]]; then
    export CLOUDSDK_CONFIG="google-auth"
    source ../gs.key
    set -e
    gcloud auth activate-service-account --key-file="$TOKEN_FILE"
    echo "* Provider = GS"
    gcloud storage  ls gs://${BUCKET}
    gcloud storage cp generated/*.mp4 gs://${BUCKET}/videos
    rm "$TOKEN_FILE"
    rm -rf google-auth
else
    echo "Bad Provider: Choices = [s3,gs]"
    exit 1
fi


