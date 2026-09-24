SHELL := /bin/sh
COMPOSE := docker compose

.PHONY: build-python up-sfv demo-sfv up-flink build-flink submit-flink demo-flink seed verify verify-flink-topics test test-java test-python test-restart check-readme down

build-python:
	$(COMPOSE) build feast

up-sfv: build-python
	$(COMPOSE) --profile sfv up -d kafka kafka-init redis raw-mirror feast

demo-sfv: up-sfv seed
	$(COMPOSE) run --rm sfv-runner

up-flink: build-flink build-python
	$(COMPOSE) --profile flink up -d kafka kafka-init redis feast flink-jobmanager flink-taskmanager feast-pusher

build-flink:
	$(COMPOSE) run --rm flink-build

submit-flink: build-flink
	$(COMPOSE) cp flink-job/target/flink-customer-features.jar flink-jobmanager:/tmp/flink-customer-features.jar
	$(COMPOSE) exec flink-jobmanager flink run -d /tmp/flink-customer-features.jar

demo-flink: up-flink submit-flink seed
	$(COMPOSE) run --rm verify python -m feature_store_demo.integration_check
	$(COMPOSE) run --rm verify python -m feature_store_demo.verify --pipeline flink --wait-seconds 90

seed:
	$(COMPOSE) run --rm seed

verify:
	$(COMPOSE) run --rm verify python -m feature_store_demo.verify --pipeline all --wait-seconds 30

verify-flink-topics:
	$(COMPOSE) run --rm verify python -m feature_store_demo.integration_check

test: test-java test-python

test-java:
	$(COMPOSE) run --rm flink-build mvn -q test

test-python: build-python
	$(COMPOSE) run --rm python-test

test-restart: build-flink
	sh scripts/restart_from_savepoint.sh
	$(COMPOSE) run --rm seed
	$(COMPOSE) run --rm verify python -m feature_store_demo.verify --pipeline flink --wait-seconds 90

check-readme:
	python3 scripts/check_readme_references.py

down:
	$(COMPOSE) --profile sfv --profile flink down --remove-orphans
