ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION} AS workflow_base

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip wget curl jq \
    python3-pip &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN --mount=type=bind,from=workflow_base,source=/opt/aperturedb-sdk,target=/sdk \
    pip3 install --upgrade pip && \
    pip3 install /sdk/aperturedb-*-py3-none-any.whl



COPY check_data.py /
COPY config-db.part /
COPY start.sh /

CMD ["/start.sh"]
