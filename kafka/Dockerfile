FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .

ENV KAFKA_BROKER=kafka:9092

CMD ["python", "producer.py"]
