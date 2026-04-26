FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
    unzip wget curl jq \
    python3-pip &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN pip3 install --upgrade pip && \
    pip3 install aperturedb==0.4.58



COPY check_data.py /
COPY config-db.part /
COPY start.sh /

CMD ["/start.sh"]
