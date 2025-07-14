#!/bin/bash

# Remove the set -e to allow the script to continue
# even if there are errors because we want to upload
# the results and logs to S3 even if the app fails.
# set -e

LOGFILE=/tmp/app.log
source config-db.part

python3 check_data.py --images ${IMAGE_COUNT} --videos ${VIDEO_COUNT} --pdfs ${PDF_COUNT}
ret_val="$?"

exit "${ret_val}"
