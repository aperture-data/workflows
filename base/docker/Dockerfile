FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip wget curl jq lsof\
    python3-venv &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Use virtual environment for pip install
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
RUN tar -xf google-cloud-cli-linux-x86_64.tar.gz
RUN ./google-cloud-sdk/install.sh
ENV PATH=$PATH:/google-cloud-sdk/bin

COPY requirements.txt /
RUN pip install --upgrade -r /requirements.txt

RUN mkdir -p app/output

COPY scripts/app.sh app/

COPY scripts/userlog.py app/
COPY scripts/slack-alert.py app/
COPY scripts/batcher.py app/
COPY scripts/wf_argparse.py app/
COPY scripts/connection_pool.py app/
COPY scripts/status_server.py app/
COPY scripts/status.py app/
COPY scripts/log_processor.py app/
COPY scripts/text_extraction app/text_extraction/

COPY scripts/start.sh /

CMD ["/start.sh"]
