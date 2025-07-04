# Pull base image.
FROM aperturedata/aperturedb-notebook:cpu

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension
COPY app/app.sh /aperturedata
RUN mkdir -p /notebooks
RUN mkdir -p /aperturedata/notebooks
COPY app/notebooks/*.ipynb /aperturedata/notebooks/
CMD bash -c '/aperturedata/app.sh && /start.sh'
