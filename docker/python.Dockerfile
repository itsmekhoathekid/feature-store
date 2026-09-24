FROM python:3.11.11-slim-bookworm@sha256:081075da77b2b55c23c088251026fb69a7b2bf92471e491ff5fd75c192fd38e5

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY requirements.lock ./
RUN pip install --no-cache-dir --upgrade pip==25.2 \
    && pip install --no-cache-dir -r requirements.lock
COPY docker/feast_serde.py /usr/local/lib/python3.11/site-packages/feast/infra/common/serde.py

COPY pyproject.toml ./
COPY python ./python
RUN pip install --no-cache-dir --no-deps .
COPY feast ./feast
COPY docker ./docker
COPY scripts ./scripts
ENV PYTHONUNBUFFERED=1 \
    FEAST_REPO_PATH=/workspace/feast \
    DATA_DIR=/data
