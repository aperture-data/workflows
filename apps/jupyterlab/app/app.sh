#!/bin/bash
set -e

initialize_jupyter() {
    echo "Initializing Jupyter server..."

    mkdir -p "${NOTEBOOK_DIR}"
    cp /aperturedata/hello.ipynb "${NOTEBOOK_DIR}/hello.ipynb"
    sed -i "s/<DB_HOST>/${DB_HOST_PUBLIC}/g" "${NOTEBOOK_DIR}/hello.ipynb"

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
