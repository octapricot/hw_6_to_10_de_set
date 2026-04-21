FROM --platform=linux/arm64 python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.6.1

COPY consumer.py .

CMD ["python", "consumer.py"]