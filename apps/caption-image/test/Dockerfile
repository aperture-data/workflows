ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION} AS workflow_base

FROM python:3.10-slim
WORKDIR /app

RUN pip install --upgrade pip
COPY requirements.txt .
RUN --mount=type=bind,from=workflow_base,source=/opt/aperturedb-sdk,target=/sdk \
    pip install /sdk/aperturedb-*-py3-none-any.whl -r requirements.txt

COPY *.py /app/
