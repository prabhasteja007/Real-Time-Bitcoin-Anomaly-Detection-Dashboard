# Java is required by PySpark
FROM python:3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the Spark Kafka connector JAR so it's baked into the image
# (avoids a slow first-run Maven download at container startup)
COPY preload_jars.py .
RUN python preload_jars.py

COPY . .

CMD ["python", "consumer.py"]
