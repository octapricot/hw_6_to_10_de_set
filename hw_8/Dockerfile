FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python-ng==2.2.3

COPY producer.py .

CMD ["python", "producer.py"]