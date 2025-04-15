# Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jdk curl && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN pip install JayDeBeApi JPype1 python-dotenv

COPY dremio-jdbc-driver.jar .
COPY test_dremio_jdbc.py .
COPY .env .

CMD ["python", "test_dremio_jdbc.py"]