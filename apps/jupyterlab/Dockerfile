# Pull base image.
ARG VERSION=latest
# The CPU notebook at dev; CI passes the digest in the app's input hash.
ARG NOTEBOOK_CPU_IMAGE=aperturedata/aperturedb-notebook-cpu:dev
FROM aperturedata/workflows-base:${VERSION} AS common
FROM ${NOTEBOOK_CPU_IMAGE}

RUN mkdir -p /app

RUN apt-get -y install lsof

# Install the SDK and aperturedb-workflows wheels from the workflows base.
COPY requirements.txt /
RUN --mount=type=bind,from=common,source=/opt/aperturedb-sdk,target=/sdk \
    pip uninstall -y aperturedb && \
    pip install --no-cache-dir /sdk/*.whl -r /requirements.txt

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension

COPY app/app.sh /app

RUN mkdir -p /notebooks
RUN mkdir -p /aperturedata/notebooks
RUN mv /start.sh /start-jupyter.sh

COPY app/notebooks/*.ipynb /aperturedata/notebooks/
ARG WORKFLOW_VERSION="UNDEFINED"
ENV WORKFLOW_VERSION=${WORKFLOW_VERSION}
# This runs everything, and app.sh in the app directory
CMD ["wf-start"]
