# Pull base image.
FROM aperturedata/workflows-base as common
FROM aperturedata/aperturedb-notebook:cpu

RUN apt-get -y install lsof
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension
COPY app/app.sh /aperturedata
RUN mkdir -p /notebooks
RUN mkdir -p /aperturedata/notebooks
COPY app/notebooks/*.ipynb /aperturedata/notebooks/

COPY --from=common /app/status_server.py /aperturedata
COPY --from=common /app/status.py /aperturedata

CMD bash -c '/aperturedata/app.sh && /start.sh'
