#!/bin/bash
set -e

initialize_jupyter() {
    echo "Initializing Jupyter server..."
    cp /aperturedata/hello.ipynb ${NOTEBOOK_DIR}/hello.ipynb

    echo "Jupyter server configured."
    touch "${APP_DIR}/initialized"
}

if [ ! -f "${APP_DIR}/initialized" ]; then
    initialize_jupyter
fi

configure_jupyter() {
    echo "Configuring Jupyter server..."
    #Install adb completion
    echo "adb --install-completion" | bash

    echo "Jupyter server configured."
    touch /opt/.jupyter_configured
}

if [ ! -f /opt/.jupyter_configured ]; then
    configure_jupyter
fi
