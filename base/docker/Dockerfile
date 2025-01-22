FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    awscli \
<<<<<<< HEAD
    unzip wget curl \
    python3-pip &&\
=======
    unzip wget curl\
    python3-pip \
    python3-opencv && \
>>>>>>> 9552f0c (automatic commit message)
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip && \
    pip3 install dbeval slack_sdk

RUN pip3 install --upgrade aperturedb

<<<<<<< HEAD
=======
RUN pip3 install "numpy < 2"

>>>>>>> 9552f0c (automatic commit message)
RUN mkdir -p app/output

COPY scripts/app.sh app/

COPY scripts/userlog.py app/
COPY scripts/slack-alert.py app/

COPY scripts/start.sh /

CMD ["/start.sh"]
