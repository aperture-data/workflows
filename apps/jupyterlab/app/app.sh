#!/bin/bash
set -e


# Make DB_HOST_PUBLIC default to DB_HOST or localhost if not set
DB_HOST_PUBLIC="${DB_HOST_PUBLIC:-${DB_HOST:-localhost}}"

# Provide default directories for notebooks and app
NOTEBOOK_DIR="${NOTEBOOK_DIR:-/notebooks}"
APP_DIR="${APP_DIR:-/app}"

initialize_jupyter() {
    echo "Initializing Jupyter server..."

    mkdir -p "${NOTEBOOK_DIR}"
    for FILE in /aperturedata/notebooks/*.ipynb; do
        NAME=$(basename "$FILE")
        cp "$FILE" "${NOTEBOOK_DIR}/"
        sed -i "s/<DB_HOST>/${DB_HOST_PUBLIC}/g" "${NOTEBOOK_DIR}/${NAME}"
    done

    mkdir -p "${APP_DIR}"
    touch "${APP_DIR}/initialized"

    echo "Jupyter server initialized."
}

if [ ! -f "${APP_DIR}/initialized" ]; then
    initialize_jupyter
fi

configure_jupyter() {
    echo "Configuring Jupyter server..."

    #Install adb completion
    echo "adb --install-completion" | bash

    touch /opt/.jupyter_configured

    echo "Jupyter server configured."
}

if [ ! -f /opt/.jupyter_configured ]; then
    configure_jupyter
fi

bash /start-jupyter.sh &
python3 /app/status.py --phases running --phase running --completed 100 --accessible
