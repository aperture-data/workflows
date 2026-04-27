FROM python:3.10-slim
WORKDIR /app
RUN pip install --upgrade pip
COPY requirements.txt .
# Bust cache when a new version of aperturedb is published to PyPI
ADD "https://pypi.org/pypi/aperturedb/json" /tmp/aperturedb_version.json
RUN pip install -r requirements.txt
COPY *.py /app/

CMD ["python", "app.py"]