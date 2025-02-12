#!/bin/bash

# Remove the set -e to allow the script to continue
# even if there are errors because we want to upload
# the results and logs to S3 even if the app fails.
# set -e

cd app

OUTPUT="output"
LOGFILE="${OUTPUT}/log.log"
APPLOG="${OUTPUT}/app.log"
S3LOGFILE="upload_s3.log"


if [ -z "${AWS_ACCESS_KEY_ID}" ]; then
    AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_LOGS_AWS_CREDENTIALS})
fi
if [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
    AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_LOGS_AWS_CREDENTIALS})
fi

if [ -z "${KEEP_PREV_OUTPUT}" ]; then
    KEEP_PREV_OUTPUT=false
fi

if [ -z "${USERLOG_MSG}" ]; then
    USERLOG_MSG=true
fi

if [ -z "${POST_TO_SLACK}" ]; then
    POST_TO_SLACK=false
fi

if [ -z "${SLACK_CHANNEL}" ]; then
    SLACK_CHANNEL="cronjobs"
fi

if [ -z "${SLACK_CHANNEL_FAILED}" ]; then
    SLACK_CHANNEL_FAILED="cronjobs"
fi

if [ -z "${SLACK_BOT_TOKEN}" ]; then
    SLACK_BOT_TOKEN=""
    POST_TO_SLACK=false
fi

if [ -z "${PUSH_TO_S3}" ]; then
    PUSH_TO_S3=true
    if [ -z "${AWS_ACCESS_KEY_ID}" ]; then
        PUSH_TO_S3=false
    fi
    if [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
        PUSH_TO_S3=false
    fi
fi

if [ "$KEEP_PREV_OUTPUT" = true ]; then
    # Sometimes it is useful to keep results from previous app runs.
    # This is useful when you want to compare results from different runs.
    echo "Keeping old output." >> $LOGFILE
    date >> $LOGFILE
else
    rm -rf ${OUTPUT}/*
    date > $LOGFILE
fi

if [ -z "$RUN_NAME" ]
    then
    RUN_NAME="unspecified_app_run"
    echo "WARNING: RUN_NAME not specified" >> $LOGFILE
fi

if [ -z "$APP_NAME" ]
    then
    APP_NAME="unspecified_app"
    echo "WARNING: APP_NAME not specified" >> $LOGFILE
fi

DEFAULT_DB_PORT=55555

DB_HOST=${DB_HOST:="localhost"}
DB_USER=${DB_USER:="admin"}
DB_PASS=${DB_PASS:="admin"}
USE_SSL=${USE_SSL:=true}
USE_REST=${USE_REST:=false}

if [[ $USE_REST == true ]]; then
    DEFAULT_DB_PORT=80
    if [[ $USE_SSL == true ]]; then
        DEFAULT_DB_PORT=443
    fi
fi

DB_PORT=${DB_PORT:=$DEFAULT_DB_PORT}

params=()
if [[ $USE_SSL == false ]]; then
    params+=(--no-use-ssl)
fi

if [[ $USE_REST == true ]]; then
    params+=(--use-rest)
fi

adb config create default \
    --host=$DB_HOST \
    --port=$DB_PORT \
    --username=$DB_USER \
    --password=$DB_PASS \
    "${params[@]}" \
    --no-interactive

echo "Verifying connectivity to ${DB_HOST}..."
adb utils execute status >> $LOGFILE
echo "Done."

adb utils execute summary >> db_summary_before.log

if [ "$USERLOG_MSG" = true ]; then
    python3 userlog.py --info "Starting App ${APP_NAME} - Run: ${RUN_NAME}."
fi

GRAFANA_START_TIME=$(($(date '+%s') * 1000))

# Move all log files to output folder

start=`date +%s`
echo "Starting App: ${APP_NAME} - Run: ${RUN_NAME}..."
echo "Starting App: ${APP_NAME} - Run: ${RUN_NAME}..." >> $LOGFILE

bash app.sh |& tee -a $APPLOG
ret_val="${PIPESTATUS[0]}"

echo "App Done." >> $LOGFILE

echo "App Done."
end=`date +%s`
runtime=$((end-start))

GRAFANA_END_TIME=$(($(date '+%s') * 1000))

adb utils execute summary >> db_summary_after.log
diff db_summary_before.log db_summary_after.log >> db_summary_diff.log

if [ "$USERLOG_MSG" = true ]; then
    python3 userlog.py --info "Done App ${APP_NAME} - Run: ${RUN_NAME}."
fi

date >> $LOGFILE

# Move all log files to output folder
if compgen -G "*.log" > /dev/null; then
    mv *.log ${OUTPUT}/
fi

DATE=$(date '+%Y-%m-%d') # Get current date
SECONDS=$(date '+%s')    # seconds since epoch

if [ "$PUSH_TO_S3" = true ]; then

    aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
    aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
    aws configure set default.region us-west-2

    ENVIRONMENT=${ENVIRONMENT:="develop"}

    FILE_STASH_DOMAIN=${FILE_STASH_DOMAIN:="aperturedata.dev"}
    LOGS_BUCKET=${LOGS_BUCKET:="aperturedata-${ENVIRONMENT}-iris-workflows-logs"}

    # Hacky, I don't like depending on the environment, but not sure how to do it better.
    if [ "${ENVIRONMENT}" == "main" ]; then
        FILE_STASH_DOMAIN="aperturedata.io"
    fi

    SUFFIX=$DATE/${APP_NAME}/${SECONDS}_${RUN_NAME}/

    BUCKET=s3://$WF_LOGS_AWS_BUCKET/$SUFFIX

    echo "Uploading results to S3..." >> $LOGFILE
    aws s3 sync --quiet ${OUTPUT}/ $BUCKET > $S3LOGFILE
    echo "Done uploading results to S3." >> $LOGFILE
    date >> $LOGFILE
    echo "All done. Bye." >> $LOGFILE

    URL=https://workflows-logs.${FILE_STASH_DOMAIN}/files/$WF_LOGS_AWS_BUCKET/$SUFFIX

    GRAFANA_URL="https://${DB_HOST}/grafana/d/mPHHiqbnk/aperturedb-connectivity-status?from=${GRAFANA_START_TIME}&to=${GRAFANA_END_TIME}&var-job_filter=job%7C%3D%7Caperturedb&var-pod_ip=All&var-node_filter=pod_ip%7C%3D~%7C$pod_ip&orgId=1&refresh=5s"

    echo "Results available at: $URL"
    echo "Dashboard: $GRAFANA_URL"

    echo "Results available at: $URL" >> $LOGFILE
    echo "Dashboard: $GRAFANA_URL" >> $LOGFILE

    RESULTS="<$URL|Results>"
    GRAFANA="<$GRAFANA_URL|Dashboards>"

    # Need to upload log.log again :)
    aws s3 cp $LOGFILE ${BUCKET}log.log >> $S3LOGFILE
else
    echo "Not uploading results to S3."
    echo "=================== APP LOG ==================="
    cat $APPLOG
    echo "================= END APP LOG ================="
fi

if [ "${ret_val}" -ne 0 ]; then
    echo "Error with app: $APP_NAME. Exiting with error code: ${ret_val}"
fi

exit "${ret_val}"
