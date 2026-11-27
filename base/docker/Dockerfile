FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip \
    wget \
    curl \
    jq \
    lsof \
    python3-venv \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Use virtual environment for pip install
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
RUN tar -xf google-cloud-cli-linux-x86_64.tar.gz
RUN ./google-cloud-sdk/install.sh
ENV PATH=$PATH:/google-cloud-sdk/bin

# git is required for CLIP
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# The sdk_wheel context holds the SDK and aperturedb-workflows wheels. Keep
# them for the separate test images built against this base.
COPY --from=sdk_wheel /*.whl /opt/aperturedb-sdk/
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir /opt/aperturedb-sdk/*.whl

# The aperturedb-workflows sitecustomize replaces Ubuntu's.
RUN rm /usr/lib/python3.10/sitecustomize.py

# Applications copy their code, including app.sh, into /app.
RUN mkdir /app

ARG WORKFLOW_VERSION="UNDEFINED"
ENV WORKFLOW_VERSION=${WORKFLOW_VERSION}

CMD ["wf-start"]
