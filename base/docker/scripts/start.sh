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
WORKFLOW_VERSION=$(cat /app/workflow_version)

mkdir -p ${OUTPUT}

ENVIRONMENT=${ENVIRONMENT:-"develop"}

if [ -z "${KEEP_PREV_OUTPUT}" ]; then
    KEEP_PREV_OUTPUT=false
fi

if [ "$KEEP_PREV_OUTPUT" == true ]; then
    # Sometimes it is useful to keep results from previous app runs.
    # This is useful when you want to compare results from different runs.
    echo "Keeping old output." >> $LOGFILE
    date >> $LOGFILE
else
    rm -rf ${OUTPUT}/*
    date > $LOGFILE
fi

echo "Workflow Version: ${WORKFLOW_VERSION}"

if [ -z "$RUN_NAME" ]; then
    RUN_NAME="unspecified_app_run"
    echo "WARNING: RUN_NAME not specified" >> $LOGFILE
fi

if [ -z "$APP_NAME" ]; then
    APP_NAME="unspecified_app"
    echo "WARNING: APP_NAME not specified" >> $LOGFILE
fi

echo "Starting Status Server: ..."
# Start fastapi-based status server
# This is used to provide a status endpoint for the workflow.
STATUS_SERVER_HOSTNAME=$(hostname -f)
STATUS_SERVER_PORT=8080
PROMETHEUS_PORT=8001
HOSTNAME=${STATUS_SERVER_HOSTNAME} PORT=${STATUS_SERVER_PORT} PROMETHEUS_PORT=${PROMETHEUS_PORT} python3 status_server.py &
# Wait for the status server to start
while [ -z "$(lsof -i:8080)" ]; do
    echo "Waiting for Status Server to start on port 8080..."
    sleep 1
done
echo "Starting Status Server is up."

# Initialize ADB_USER and ADB_PASS
ADB_USER=${DB_USER:-"admin"}
ADB_PASS=${DB_PASS:-"admin"}

#Initialize ADB_USE_SSL and ADB_USE_REST
ADB_USE_SSL="${USE_SSL:-true}"
ADB_USE_REST="${USE_REST:-false}"

#Initialize ADB_PORT
if [ -n "${DB_PORT}" ]; then
    ADB_PORT="${DB_PORT}"
elif [ "${ADB_USE_REST}" == true ]; then
    if [ "${ADB_USE_SSL}" == true ]; then
        ADB_PORT=443
    else
        ADB_PORT=80
    fi
else
    ADB_PORT=55555
fi

# Initialize ADB_HOST and ADB_VERIFY_HOSTNAME
if [ -n "${DB_HOST_PRIVATE}" ]; then
    ADB_HOST="${DB_HOST_PRIVATE}"
    ADB_VERIFY_HOSTNAME=false
elif [ -n "${DB_HOST_PUBLIC}" ]; then
    ADB_HOST="${DB_HOST_PUBLIC}"
    ADB_VERIFY_HOSTNAME="${VERIFY_HOSTNAME:-true}"
elif [ -z "${DB_HOST}" ]; then
    ADB_HOST="localhost"
    ADB_VERIFY_HOSTNAME=false
elif [ "${DB_HOST}" == "localhost" ] || [ "${DB_HOST}" == "127.0.0.1" ] || [ "${DB_HOST}" == "::1" ]; then
    ADB_HOST="${DB_HOST}"
    ADB_VERIFY_HOSTNAME=false
else
    ADB_HOST="${DB_HOST}"
    ADB_VERIFY_HOSTNAME="${VERIFY_HOSTNAME:-true}"
fi

params=()
if [ "${ADB_USE_SSL}" == false ]; then
    params+=(--no-use-ssl)
elif [ "${ADB_VERIFY_HOSTNAME}" == false ]; then
    params+=(--no-verify-hostname)
fi

if [ "${ADB_USE_REST}" == true ]; then
    params+=(--use-rest)
fi

if [ -n "${CA_CERT:-}" ]; then
    params+=(--ca-cert $CA_CERT)
fi

STATUS_SCRIPT=/app/status_tools.py

adb config create default \
    --host=$ADB_HOST \
    --port=$ADB_PORT \
    --username=$ADB_USER \
    --password=$ADB_PASS \
    "${params[@]}" \
    --no-interactive

echo "Verifying connectivity to ${ADB_HOST}..." | tee -a $LOGFILE
adb utils execute status 2>&1 | tee -a $LOGFILE
ret_val="${PIPESTATUS[0]}"

if [ "${ret_val}" -ne 0 ]; then
    python $STATUS_SCRIPT --completed 0 --error-message "Could not connect to database" --error-code "workflow_error"
else
    echo "Done."

    adb utils execute summary >> db_summary_before.log

    if [ "$USERLOG_MSG" == true ]; then
        python3 userlog.py --info "Starting App ${APP_NAME} - Run: ${RUN_NAME}."
    fi

    GRAFANA_START_TIME=$(($(date '+%s') * 1000))



    # Move all log files to output folder

    start=`date +%s`
    echo "Starting App: ${APP_NAME} - Run: ${RUN_NAME}..." | tee -a $LOGFILE
    export PYTHONPATH=.

    bash app.sh |& tee -a $APPLOG
    ret_val="${PIPESTATUS[0]}"

    if [ "${ret_val}" -ne 0 ]; then
        status_response=$(curl -s http://${HOSTNAME}:${STATUS_SERVER_PORT}/status)
        curl_exit_code=$?
        if [ $curl_exit_code -ne 0 ] || [ -z "$status_response" ]; then
            error_message="Failed to fetch status from http://${HOSTNAME}:${STATUS_SERVER_PORT}/status (curl exit code: $curl_exit_code)"
        else
            error_message=$(echo "$status_response" | jq -r '.error_message' 2>/dev/null)
            jq_exit_code=$?
            if [ $jq_exit_code -ne 0 ] || [ -z "$error_message" ] || [ "$error_message" == "null" ]; then
                error_message="Failed to parse error_message from status response"
            fi
        fi
        python $STATUS_SCRIPT --completed 0 --error-message "${error_message}. Failed with exit code: ${ret_val}" --error-code "workflow_error"
    fi

    echo "App Done." | tee -a $LOGFILE

    end=`date +%s`
    runtime=$((end-start))

    GRAFANA_END_TIME=$(($(date '+%s') * 1000))

    GRAFANA_URL="https://${DB_HOST_PUBLIC}/grafana/d/mPHHiqbnk/aperturedb-connectivity-status?from=${GRAFANA_START_TIME}&to=${GRAFANA_END_TIME}&refresh=5s"

    adb utils execute summary >> db_summary_after.log
    diff db_summary_before.log db_summary_after.log >> db_summary_diff.log

    if [ "$USERLOG_MSG" == true ]; then
        python3 userlog.py --info "Done App ${APP_NAME} - Run: ${RUN_NAME}."
    fi

    date >> $LOGFILE

    # Move all log files to output folder
    if compgen -G "*.log" > /dev/null; then
        mv *.log ${OUTPUT}/
    fi

    DATE=$(date '+%Y-%m-%d') # Get current date
    SECONDS=$(date '+%s')    # seconds since epoch

    # UPLOAD RESULTS TO S3

    if [ "$PUSH_TO_S3" == true ]; then

        if ([ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ]) && [ -n "${WF_LOGS_AWS_CREDENTIALS}" ]; then
            AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_LOGS_AWS_CREDENTIALS})
            AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_LOGS_AWS_CREDENTIALS})
        fi
        aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
        aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
        aws configure set default.region us-west-2

        declare -A domains=([develop]=aperturedata.dev [main]=aperturedata.io);

        FILE_STASH_DOMAIN=${FILE_STASH_DOMAIN:-${domains[$ENVIRONMENT]}}
        LOGS_BUCKET=${WF_LOGS_AWS_BUCKET:-"aperturedata-${ENVIRONMENT}-iris-workflows-logs"}

        SUFFIX=$DATE/${APP_NAME}/${SECONDS}_${RUN_NAME}/

        BUCKET=s3://$LOGS_BUCKET/$SUFFIX

        echo "Uploading results to S3..." >> $LOGFILE
        aws s3 sync --quiet ${OUTPUT}/ $BUCKET > $S3LOGFILE
        echo "Done uploading results to S3." >> $LOGFILE
        date >> $LOGFILE
        echo "All done. Bye." >> $LOGFILE

        URL=https://workflows-logs.${FILE_STASH_DOMAIN}/files/$LOGS_BUCKET/$SUFFIX

        echo "Results available at: $URL"
        echo "Dashboard: $GRAFANA_URL"

        echo "Results available at: $URL" >> $LOGFILE
        echo "Dashboard: $GRAFANA_URL" >> $LOGFILE

        # Need to upload log.log again :)
        aws s3 cp $LOGFILE ${BUCKET}log.log >> $S3LOGFILE
    else
        echo "Not uploading results to S3."
        echo "=================== APP LOG ==================="
        cat $APPLOG
        echo "================= END APP LOG ================="
    fi

    # POST TO SLACK

    if [ "$POST_TO_SLACK" == true ]; then

        SLACK_CHANNEL=${SLACK_CHANNEL:-"cronjobs"}
        SLACK_CHANNEL_FAILED=${SLACK_CHANNEL_FAILED:-"alerts-${ENVIRONMENT}"}

        RESULTS="<$URL|Results>" # URL may not be available if PUSH_TO_S3 is false
        GRAFANA="<$GRAFANA_URL|Dashboards>"

        if [ "${ret_val}" -ne 0 ]; then
            python3 slack-alert.py \
                -msg ":fire: App \`${APP_NAME}\` failed(\"${ret_val}\"). $RESULTS. $GRAFANA." \
                -channel "${SLACK_CHANNEL_FAILED}"
        else
            python3 slack-alert.py \
                -msg "App \`${APP_NAME}\` done. Took ${runtime}s. $RESULTS. $GRAFANA." \
                -channel "${SLACK_CHANNEL}"
        fi
    fi

    if [ "${ret_val}" -ne 0 ]; then
        echo "Error with app: $APP_NAME. Exiting with error code: ${ret_val}"
    fi




fi

SLEEP_REPORT_TIME=6

echo "Sleeping for $SLEEP_REPORT_TIME seconds to allow statuses to be reported..."
sleep $SLEEP_REPORT_TIME
exit "${ret_val}"
