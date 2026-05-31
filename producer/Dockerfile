FROM python:3.10-slim

WORKDIR /app
COPY producer.py .
RUN pip install --no-cache-dir kafka-python websocket-client

CMD ["python", "producer.py"]
