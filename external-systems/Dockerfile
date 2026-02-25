FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pymongo[srv] dnspython paho-mqtt confluent-kafka
COPY external-systems-generator.py .
CMD ["python", "-u", "external-systems-generator.py"]
