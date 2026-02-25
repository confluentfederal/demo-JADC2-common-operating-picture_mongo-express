FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    pymongo==4.6.0 \
    confluent-kafka==2.3.0 \
    sentence-transformers==2.2.2

COPY app.py .

EXPOSE 5001

CMD ["python", "-u", "app.py"]
