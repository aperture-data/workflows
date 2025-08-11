FROM python:3.10-slim
WORKDIR /app
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py /app/

CMD ["python", "app.py"]