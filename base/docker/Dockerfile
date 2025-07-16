FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip \
    wget \
    curl \
    jq \
    python3-pip \
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
    
COPY requirements.txt /
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /requirements.txt

COPY requirements_cpu.txt /
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu -r requirements_cpu.txt

RUN pip3 install --upgrade aperturedb

RUN mkdir -p app/output

COPY scripts/app.sh app/
COPY scripts/*.py app/

COPY scripts/start.sh /

# Get the image version from the build context
ARG VERSION
ENV APP_VERSION=$VERSION

COPY scripts/start.sh /

CMD ["/start.sh"]
