#!/bin/bash

set -o errexit -o nounset -o pipefail

cd app

# Global variables
OUTPUT="output"
LOGFILE="${OUTPUT}/log.log"
APPLOG="${OUTPUT}/app.log"
S3LOGFILE="upload_s3.log"
WORKFLOW_VERSION=$(cat /app/workflow_version)
ENVIRONMENT=${ENVIRONMENT:-"develop"}
KEEP_PREV_OUTPUT=${KEEP_PREV_OUTPUT:-false}
STATUS_SERVER_HOSTNAME=$(hostname -f)
STATUS_SERVER_PORT=8080
PROMETHEUS_PORT=8001
STATUS_SCRIPT=/app/status_tools.py

# Exit code tracking
ret_val=0
start=0
end=0
runtime=0
GRAFANA_START_TIME=0
GRAFANA_END_TIME=0
GRAFANA_URL=""
URL=""

# Setup logging and output directory
setup_logging() {
    mkdir -p ${OUTPUT}

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
}

# Start the status server
start_status_server() {
    echo "Starting Status Server: ..."
    # Start fastapi-based status server
    # This is used to provide a status endpoint for the workflow.
    HOSTNAME=${STATUS_SERVER_HOSTNAME} PORT=${STATUS_SERVER_PORT} PROMETHEUS_PORT=${PROMETHEUS_PORT} python3 status_server.py &
    
    # Wait for the status server to start
    while [ -z "$(lsof -i:8080)" ]; do
        echo "Waiting for Status Server to start on port 8080..."
        sleep 1
    done
    echo "Starting Status Server is up."
}

# Setup database configuration
setup_database() {
    local DB_HOST=${DB_HOST:-"localhost"}
    local DB_HOST_PUBLIC=${DB_HOST_PUBLIC:-${DB_HOST}}
    local DB_HOST_PRIVATE_TCP=${DB_HOST_PRIVATE_TCP:-${DB_HOST}}
    local DB_HOST_PRIVATE_HTTP=${DB_HOST_PRIVATE_HTTP:-${DB_HOST}}
    local DB_USER=${DB_USER:-"admin"}
    local DB_PASS=${DB_PASS:-"admin"}
    local USE_SSL=${USE_SSL:-true}
    local USE_REST=${USE_REST:-false}
    local DEFAULT_DB_PORT

    if [ "$USE_REST" == true ]; then
        DB_HOST=${DB_HOST_PRIVATE_HTTP}
        if [ "$USE_SSL" == true ]; then
            DEFAULT_DB_PORT=443
        else
            DEFAULT_DB_PORT=80
        fi
    else
        DB_HOST=${DB_HOST_PRIVATE_TCP}
        DEFAULT_DB_PORT=55555
    fi

    local DB_PORT=${DB_PORT:-$DEFAULT_DB_PORT}

    local params=()
    if [ "$USE_SSL" == false ]; then
        params+=(--no-use-ssl)
    fi

    if [ "$USE_REST" == true ]; then
        params+=(--use-rest)
    fi

    if [ -n "${CA_CERT:-}" ]; then
        params+=(--ca-cert $CA_CERT)
    fi

    if [ "$VERIFY_HOSTNAME" == false ]; then
        params+=(--no-verify-hostname)
    fi

    adb config create default \
        --host=$DB_HOST \
        --port=$DB_PORT \
        --username=$DB_USER \
        --password=$DB_PASS \
        "${params[@]}" \
        --no-interactive

    echo "Verifying connectivity to ${DB_HOST}..." | tee -a $LOGFILE
    
    # Temporarily disable errexit to capture failure and report to status server
    set +o errexit
    adb utils execute status 2>&1 | tee -a $LOGFILE
    local db_ret_val="${PIPESTATUS[0]}"
    set -o errexit
    
    if [ "${db_ret_val}" -ne 0 ]; then
        python $STATUS_SCRIPT --completed 0 --error-message "Could not connect to database" --error-code "workflow_error"
        return 1
    fi

    echo "Done."
    adb utils execute summary >> db_summary_before.log
}

# Run the main application
run_app() {
    if [ "$USERLOG_MSG" == true ]; then
        python3 userlog.py --info "Starting App ${APP_NAME} - Run: ${RUN_NAME}."
    fi

    GRAFANA_START_TIME=$(($(date '+%s') * 1000))

    start=`date +%s`
    echo "Starting App: ${APP_NAME} - Run: ${RUN_NAME}..." | tee -a $LOGFILE
    export PYTHONPATH=.

    # Temporarily disable errexit to capture app failure and report to status server
    set +o errexit
    bash app.sh |& tee -a $APPLOG
    ret_val="${PIPESTATUS[0]}"
    set -o errexit

    if [ "${ret_val}" -ne 0 ]; then
        # Try to get error message from status server, but don't fail if we can't
        local error_message
        error_message=$(curl -s http://${HOSTNAME}:${STATUS_SERVER_PORT}/status | jq -r '.error_message // empty' 2>/dev/null || echo "")
        
        if [ -z "$error_message" ]; then
            error_message="App failed"
        fi
        
        python $STATUS_SCRIPT --completed 0 --error-message "${error_message}. Failed with exit code: ${ret_val}" --error-code "workflow_error"
    fi

    echo "App Done." | tee -a $LOGFILE

    end=`date +%s`
    runtime=$((end-start))

    GRAFANA_END_TIME=$(($(date '+%s') * 1000))

    local DB_HOST_PUBLIC=${DB_HOST_PUBLIC:-${DB_HOST:-"localhost"}}
    GRAFANA_URL="https://${DB_HOST_PUBLIC}/grafana/d/mPHHiqbnk/aperturedb-connectivity-status?from=${GRAFANA_START_TIME}&to=${GRAFANA_END_TIME}&refresh=5s"

    adb utils execute summary >> db_summary_after.log
    diff db_summary_before.log db_summary_after.log >> db_summary_diff.log || true

    if [ "$USERLOG_MSG" == true ]; then
        python3 userlog.py --info "Done App ${APP_NAME} - Run: ${RUN_NAME}."
    fi

    date >> $LOGFILE

    # Move all log files to output folder
    if compgen -G "*.log" > /dev/null; then
        mv *.log ${OUTPUT}/
    fi
}

# Upload results to S3
upload_to_s3() {
    if [ "$PUSH_TO_S3" != true ]; then
        echo "Not uploading results to S3."
        echo "=================== APP LOG ==================="
        cat $APPLOG
        echo "================= END APP LOG ================="
        return 0
    fi

    if ([ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ]) && [ -n "${WF_LOGS_AWS_CREDENTIALS}" ]; then
        AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_LOGS_AWS_CREDENTIALS})
        AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_LOGS_AWS_CREDENTIALS})
    fi
    
    aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
    aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
    aws configure set default.region us-west-2

    declare -A domains=([develop]=aperturedata.dev [main]=aperturedata.io)

    local FILE_STASH_DOMAIN=${FILE_STASH_DOMAIN:-${domains[$ENVIRONMENT]}}
    local LOGS_BUCKET=${WF_LOGS_AWS_BUCKET:-"aperturedata-${ENVIRONMENT}-iris-workflows-logs"}
    local DATE=$(date '+%Y-%m-%d')
    local SECONDS=$(date '+%s')
    local SUFFIX=$DATE/${APP_NAME}/${SECONDS}_${RUN_NAME}/
    local BUCKET=s3://$LOGS_BUCKET/$SUFFIX

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
}

# Post notification to Slack
post_to_slack() {
    if [ "$POST_TO_SLACK" != true ]; then
        return 0
    fi

    local SLACK_CHANNEL=${SLACK_CHANNEL:-"cronjobs"}
    local SLACK_CHANNEL_FAILED=${SLACK_CHANNEL_FAILED:-"alerts-${ENVIRONMENT}"}
    local RESULTS="<$URL|Results>" # URL may not be available if PUSH_TO_S3 is false
    local GRAFANA="<$GRAFANA_URL|Dashboards>"

    if [ "${ret_val}" -ne 0 ]; then
        python3 slack-alert.py \
            -msg ":fire: App \`${APP_NAME}\` failed(\"${ret_val}\"). $RESULTS. $GRAFANA." \
            -channel "${SLACK_CHANNEL_FAILED}"
    else
        python3 slack-alert.py \
            -msg "App \`${APP_NAME}\` done. Took ${runtime}s. $RESULTS. $GRAFANA." \
            -channel "${SLACK_CHANNEL}"
    fi
}

# Cleanup function to be called on exit
cleanup() {
    local exit_code=$?
    
    # Use ret_val if it was set by app failure, otherwise use trap's exit code
    if [ ${ret_val} -ne 0 ]; then
        exit_code=${ret_val}
    fi

    # Upload results and post to Slack even on failure (disable errexit temporarily)
    set +o errexit
    upload_to_s3
    post_to_slack
    set -o errexit

    if [ "${exit_code}" -ne 0 ]; then
        echo "Error with app: $APP_NAME. Exiting with error code: ${exit_code}"
    fi

    local SLEEP_REPORT_TIME=6
    echo "Sleeping for $SLEEP_REPORT_TIME seconds to allow statuses to be reported..."
    sleep $SLEEP_REPORT_TIME
    
    exit "${exit_code}"
}

# Set up trap to ensure cleanup happens on exit
trap cleanup EXIT

# Main execution flow
main() {
    setup_logging
    start_status_server
    setup_database
    run_app
}

# Run main function
main
