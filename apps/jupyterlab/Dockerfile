# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION} as common
FROM aperturedata/aperturedb-notebook:cpu

RUN mkdir -p /app
# Original app directory from base It's app.sh will be overridden by the one in this directory
COPY --from=common /app /app

RUN apt-get -y install lsof

COPY --from=common requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension

# This will override the app.sh from the base image
COPY app/app.sh /app

RUN mkdir -p /notebooks
RUN mkdir -p /aperturedata/notebooks
RUN mv /start.sh /start-jupyter.sh

COPY app/notebooks/*.ipynb /aperturedata/notebooks/
# This runs everything, and app.sh in the app directory
COPY --from=common /start.sh /
