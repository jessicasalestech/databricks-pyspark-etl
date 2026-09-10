# Optional containerised local run/CI for the ETL tests.
# Build:  docker build -t medallion-etl .
# Run:    docker run --rm -v "$(pwd)":/workspace -w /workspace medallion-etl pytest -v
FROM eclipse-temurin:17-jre

# Python build deps come from the Ubuntu base's apt
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY etl ./etl
COPY tests ./tests
COPY data ./data

ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONUNBUFFERED=1

CMD ["pytest", "-v"]