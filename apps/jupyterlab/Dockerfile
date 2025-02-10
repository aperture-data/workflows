# Pull base image.
FROM aperturedata/aperturedb-notebook

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension
RUN mkdir -p /notebooks
COPY app/hello.ipynb /notebooks/
