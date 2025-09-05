
# We are merging two base images together
# We take the option of starting with label-studio since it is more complex
# and laying our workflow data on top

ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION} as wf
FROM aperturedata/label-studio

USER 0
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


ENV APP_NAME=workflows-label-studio


# Use virtual environment for pip install
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --from=wf /opt/venv /opt/venv

COPY --from=wf /google-cloud-sdk /google-cloud-sdk
ENV PATH=$PATH:/google-cloud-sdk/bin

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=wf  /app/ /app
COPY --from=wf  /start.sh /
RUN chown -R 1001 /app
USER 1001
COPY app/extensions.py /label-studio/label_studio/io_storages/aperturedb/
COPY uwsgi.ini.part /app/
RUN cat /app/uwsgi.ini.part >> /label-studio/deploy/uwsgi.ini

COPY app /app/
WORKDIR /
ENTRYPOINT ["/start.sh"]
