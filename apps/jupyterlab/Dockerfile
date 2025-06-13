# Pull base image.
FROM aperturedata/aperturedb-notebook:cpu

COPY app/shell.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/application-extension
COPY app/manager.jupyterlab-settings /root/.jupyter/lab/user-settings/\@jupyterlab/completer-extension
COPY app/app.sh /aperturedata
RUN mkdir -p /notebooks
COPY app/hello.ipynb /aperturedata
COPY app/clip.ipynb  /aperturedata
COPY app/rag.ipynb  /aperturedata
CMD bash -c '/aperturedata/app.sh && /start.sh'
