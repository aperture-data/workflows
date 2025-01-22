FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip wget curl\
    python3-pip \
    python3-opencv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip && \
    pip3 install dbeval slack_sdk

RUN pip3 install --upgrade aperturedb

RUN pip3 install "numpy < 2"

RUN mkdir -p app/output

COPY scripts/app.sh app/

COPY scripts/userlog.py app/
COPY scripts/slack-alert.py app/

COPY scripts/start.sh /

CMD ["/start.sh"]
