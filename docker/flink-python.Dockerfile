FROM flink:2.2.1-scala_2.12-java17@sha256:13b1328bbb86263b41e740f06790f49c8512a26a5c0eba36a8f91b1983d33a6c

USER root
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl python3 python3-dev python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY flink-job/requirements.lock /tmp/flink-requirements.lock
RUN python3 -m venv /opt/pyflink \
    && /opt/pyflink/bin/pip install --no-cache-dir --upgrade pip==25.2 \
    && /opt/pyflink/bin/pip install --no-cache-dir -r /tmp/flink-requirements.lock

ARG KAFKA_CONNECTOR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
ARG KAFKA_CONNECTOR_SHA256=5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616
RUN curl -fsSL "$KAFKA_CONNECTOR_URL" -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
    && echo "$KAFKA_CONNECTOR_SHA256  /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar" \
       | sha256sum -c -

COPY flink-job /opt/flink/usrlib
ENV PATH=/opt/pyflink/bin:$PATH \
    PYTHONPATH=/opt/flink/usrlib \
    PYTHONUNBUFFERED=1

USER flink
