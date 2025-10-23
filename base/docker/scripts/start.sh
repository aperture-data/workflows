#!/bin/bash

set -o errexit -o nounset -o pipefail

cd app

# Validate and sanitize external inputs
ENVIRONMENT=$(/app/wf_argparse.py --type environment --envar ENVIRONMENT --default develop)
KEEP_PREV_OUTPUT=$(/app/wf_argparse.py --type bool --envar KEEP_PREV_OUTPUT --default false)
RUN_NAME=$(/app/wf_argparse.py --type shell_safe --envar RUN_NAME --default "unspecified_app_run")
APP_NAME=$(/app/wf_argparse.py --type shell_safe --envar APP_NAME --default "unspecified_app")
USERLOG_MSG=$(/app/wf_argparse.py --type bool --envar USERLOG_MSG --default true)
PUSH_TO_S3=$(/app/wf_argparse.py --type bool --envar PUSH_TO_S3 --default false)
POST_TO_SLACK=$(/app/wf_argparse.py --type bool --envar POST_TO_SLACK --default false)

# Global variables
OUTPUT="output"
LOGFILE="${OUTPUT}/log.log"
APPLOG="${OUTPUT}/app.log"
S3LOGFILE="upload_s3.log"
WORKFLOW_VERSION=$(cat /app/workflow_version)
STATUS_SERVER_HOSTNAME=$(hostname -f)
STATUS_SERVER_PORT=8080
PROMETHEUS_PORT=8001
STATUS_SCRIPT=/app/status_tools.py

# Timing and URL variables (set during execution)
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

    if [ "${KEEP_PREV_OUTPUT}" == true ]; then
        # Sometimes it is useful to keep results from previous app runs.
        # This is useful when you want to compare results from different runs.
        echo "Keeping old output." >> $LOGFILE
        date >> $LOGFILE
    else
        rm -rf ${OUTPUT}/*
        date > $LOGFILE
    fi

    echo "Workflow Version: ${WORKFLOW_VERSION}"
    echo "App Name: ${APP_NAME}" >> $LOGFILE
    echo "Run Name: ${RUN_NAME}" >> $LOGFILE
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
    # Validate and sanitize database configuration inputs
    local ADB_USER=$(/app/wf_argparse.py --type shell_safe --envar DB_USER --default admin)
    local ADB_PASS=$(/app/wf_argparse.py --type string --envar DB_PASS --default admin --hidden)
    local ADB_USE_SSL=$(/app/wf_argparse.py --type bool --envar USE_SSL --default true)
    local ADB_USE_REST=$(/app/wf_argparse.py --type bool --envar USE_REST --default false)
    local VERIFY_HOSTNAME_DEFAULT=$(/app/wf_argparse.py --type bool --envar VERIFY_HOSTNAME --default true)
    local CA_CERT=$(/app/wf_argparse.py --type file_path --envar CA_CERT --allow-unset)

    # Initialize ADB_PORT
    local ADB_PORT
    local DB_PORT_VAL=""
    if [ -n "${DB_PORT:-}" ]; then
        DB_PORT_VAL=$(/app/wf_argparse.py --type port --envar DB_PORT --allow-unset)
        ADB_PORT="${DB_PORT_VAL}"
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
    local ADB_HOST
    local ADB_VERIFY_HOSTNAME
    if [ -n "${DB_HOST_PRIVATE:-}" ]; then
        ADB_HOST=$(/app/wf_argparse.py --type hostname --envar DB_HOST_PRIVATE)
        ADB_VERIFY_HOSTNAME=false
    elif [ -n "${DB_HOST_PUBLIC:-}" ]; then
        ADB_HOST=$(/app/wf_argparse.py --type hostname --envar DB_HOST_PUBLIC)
        ADB_VERIFY_HOSTNAME="${VERIFY_HOSTNAME_DEFAULT}"
    elif [ -z "${DB_HOST:-}" ]; then
        ADB_HOST="localhost"
        ADB_VERIFY_HOSTNAME=false
    else
        local DB_HOST_VAL=$(/app/wf_argparse.py --type hostname --envar DB_HOST )
        if [ "${DB_HOST_VAL}" == "localhost" ] || [ "${DB_HOST_VAL}" == "127.0.0.1" ] || [ "${DB_HOST_VAL}" == "::1" ]; then
            ADB_HOST="${DB_HOST_VAL}"
            ADB_VERIFY_HOSTNAME=false
        else
            ADB_HOST="${DB_HOST_VAL}"
            ADB_VERIFY_HOSTNAME="${VERIFY_HOSTNAME_DEFAULT}"
        fi
    fi

    local params=()
    if [ "${ADB_USE_SSL}" == false ]; then
        params+=(--no-use-ssl)
    elif [ "${ADB_VERIFY_HOSTNAME}" == false ]; then
        params+=(--no-verify-hostname)
    fi

    if [ "${ADB_USE_REST}" == true ]; then
        params+=(--use-rest)
    fi

    if [[ -n "${CA_CERT:-}" ]]; then
        echo "Using CA certificate: $CA_CERT"
        params+=(--ca-cert "$CA_CERT")
    fi

    adb config create default \
        --host=$ADB_HOST \
        --port=$ADB_PORT \
        --username=$ADB_USER \
        --password=$ADB_PASS \
        "${params[@]}" \
        --no-interactive

    echo "Verifying connectivity to ${ADB_HOST}..." | tee -a $LOGFILE
    adb utils execute status 2>&1 | tee -a $LOGFILE

    echo "Done."
    adb utils execute summary >> db_summary_before.log
}

app_error() {
    local app_exit_code=$?
    # Try to get error message from status server, but don't fail if we can't
    local error_message
    error_message=$(curl -s http://${STATUS_SERVER_HOSTNAME}:${STATUS_SERVER_PORT}/status | jq -r '.error_message // empty' 2>/dev/null || echo "")

    if [ -z "$error_message" ]; then
        error_message="App failed"
    fi

    python $STATUS_SCRIPT --completed 0 --error-message "${error_message}. Failed with exit code: ${app_exit_code}" --error-code "workflow_error"
    exit "${app_exit_code}"
}

# Run the main application
run_app() {
    trap app_error ERR

    if [ "${USERLOG_MSG}" == true ]; then
        python3 userlog.py --info "Starting App ${APP_NAME} - Run: ${RUN_NAME}."
    fi

    GRAFANA_START_TIME=$(($(date '+%s') * 1000))

    start=`date +%s`
    echo "Starting App: ${APP_NAME} - Run: ${RUN_NAME}..." | tee -a $LOGFILE
    export PYTHONPATH=.

    bash app.sh |& tee -a $APPLOG

    echo "App Done." | tee -a $LOGFILE

    end=`date +%s`
    runtime=$((end-start))

    GRAFANA_END_TIME=$(($(date '+%s') * 1000))

    # Get DB_HOST_PUBLIC for Grafana URL
    local DB_HOST_PUBLIC
    if [ -n "${DB_HOST_PUBLIC:-}" ]; then
        DB_HOST_PUBLIC=$(/app/wf_argparse.py --type hostname --envar DB_HOST_PUBLIC --default "localhost")
    elif [ -n "${DB_HOST:-}" ]; then
        DB_HOST_PUBLIC=$(/app/wf_argparse.py --type hostname --envar DB_HOST --default "localhost")
    else
        DB_HOST_PUBLIC="localhost"
    fi
    GRAFANA_URL="https://${DB_HOST_PUBLIC}/grafana/d/mPHHiqbnk/aperturedb-connectivity-status?from=${GRAFANA_START_TIME}&to=${GRAFANA_END_TIME}&refresh=5s"

    adb utils execute summary >> db_summary_after.log
    diff db_summary_before.log db_summary_after.log >> db_summary_diff.log || true

    if [ "${USERLOG_MSG}" == true ]; then
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
    if [ "${PUSH_TO_S3}" != true ]; then
        echo "Not uploading results to S3."
        echo "=================== APP LOG ==================="
        cat $APPLOG
        echo "================= END APP LOG ================="
        return 0
    fi

    # Validate and sanitize AWS credentials
    local AWS_ACCESS_KEY_ID_VAL=""
    local AWS_SECRET_ACCESS_KEY_VAL=""

    if ([ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]) && [ -n "${WF_LOGS_AWS_CREDENTIALS:-}" ]; then
        echo "Using WF_LOGS_AWS_CREDENTIALS"
        local WF_LOGS_AWS_CREDENTIALS_VAL=$(/app/wf_argparse.py --type json --envar WF_LOGS_AWS_CREDENTIALS --allow-unset --hidden)

        # Extract values from JSON
        local EXTRACTED_ACCESS_KEY=$(jq -r .access_key <<< ${WF_LOGS_AWS_CREDENTIALS_VAL})
        local EXTRACTED_SECRET_KEY=$(jq -r .secret_key <<< ${WF_LOGS_AWS_CREDENTIALS_VAL})

        # Validate extracted values
        AWS_ACCESS_KEY_ID_VAL=$(/app/wf_argparse.py --type aws_access_key_id --value "${EXTRACTED_ACCESS_KEY}" --allow-unset --hidden)
        AWS_SECRET_ACCESS_KEY_VAL=$(/app/wf_argparse.py --type aws_secret_access_key --value "${EXTRACTED_SECRET_KEY}" --allow-unset --hidden)
    else
        AWS_ACCESS_KEY_ID_VAL=$(/app/wf_argparse.py --type aws_access_key_id --envar AWS_ACCESS_KEY_ID --allow-unset --hidden)
        AWS_SECRET_ACCESS_KEY_VAL=$(/app/wf_argparse.py --type aws_secret_access_key --envar AWS_SECRET_ACCESS_KEY --allow-unset --hidden)
    fi

    if [ -z "${AWS_ACCESS_KEY_ID_VAL}" ] || [ -z "${AWS_SECRET_ACCESS_KEY_VAL}" ]; then
        echo "No AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY, skipping S3 upload"
        return 0
    fi

    echo "Configuring AWS credentials to upload logs to S3"
    aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID_VAL}"
    aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY_VAL}"
    aws configure set default.region us-west-2

    declare -A domains=([develop]=aperturedata.dev [main]=aperturedata.io)

    local FILE_STASH_DOMAIN=$(/app/wf_argparse.py --type hostname --envar FILE_STASH_DOMAIN --default "${domains[$ENVIRONMENT]}")
    local LOGS_BUCKET=$(/app/wf_argparse.py --type aws_bucket_name --envar WF_LOGS_AWS_BUCKET --default "aperturedata-${ENVIRONMENT}-iris-workflows-logs")
    local DATE=$(date '+%Y-%m-%d')
    local TIMESTAMP=$(date '+%s')
    local SUFFIX=$DATE/${APP_NAME}/${TIMESTAMP}_${RUN_NAME}/
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
    local exit_code=$1

    if [ "${POST_TO_SLACK}" != true ]; then
        return 0
    fi

    # Validate and sanitize Slack configuration
    local SLACK_CHANNEL=$(/app/wf_argparse.py --type slack_channel --envar SLACK_CHANNEL --default "cronjobs")
    local SLACK_CHANNEL_FAILED=$(/app/wf_argparse.py --type slack_channel --envar SLACK_CHANNEL_FAILED --default "alerts-${ENVIRONMENT}")
    local RESULTS="<$URL|Results>" # URL may not be available if PUSH_TO_S3 is false
    local GRAFANA="<$GRAFANA_URL|Dashboards>"

    if [ "${exit_code}" -ne 0 ]; then
        python3 slack-alert.py \
            -msg ":fire: App \`${APP_NAME}\` failed(\"${exit_code}\"). $RESULTS. $GRAFANA." \
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

    # Upload results and post to Slack even on failure (disable errexit temporarily)
    set +o errexit
    upload_to_s3
    post_to_slack "${exit_code}"
    set -o errexit

    if [ "${exit_code}" -ne 0 ]; then
        echo "Error with app: $APP_NAME. Exiting with error code: ${exit_code}"
    fi

    local SLEEP_REPORT_TIME=6
    echo "Sleeping for $SLEEP_REPORT_TIME seconds to allow statuses to be reported..."
    sleep $SLEEP_REPORT_TIME

    exit "${exit_code}"
}

# Make sure we log error details
trap 'echo "Error on line ${BASH_LINENO[0]}: ${BASH_COMMAND}" >&2' ERR
# Set up trap to ensure cleanup happens on exit
trap cleanup EXIT

# Main execution flow
main() {
    setup_logging
    start_status_server
    if [ -z "${APERTUREDB_KEY:-}" ] && ( [ -z "${DB_PASS:-}" ] || [ -z "${DB_USER:-}" ] ); then
        echo "No ApertureDB key or DB password or DB user provided. Running app.sh directly."
        bash app.sh |& tee -a $APPLOG
    else
        setup_database
        run_app
    fi

}

# Run main function
main
