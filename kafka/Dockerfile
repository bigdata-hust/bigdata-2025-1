FROM python:3.10-slim

WORKDIR /app

COPY kafka/producer.py .
COPY kafka/requirements.txt .
COPY data ./data

RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER=kafka:9092
ENV DATA_DIR=/app/data

CMD ["python", "producer.py"]
