#!/bin/bash
set -e

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
