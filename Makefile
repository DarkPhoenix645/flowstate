.PHONY: help setup ingest amplify hotspots up down \
	stream-produce stream-consume hive-ddl mr-package test lint

PYTHON ?= uv run python
ROWS ?= 1000000
TOPIC ?= rides.raw
RATE ?= 500
DATASET ?= all
SPARK_MASTER ?= spark://spark:7077
SPARK_SUBMIT = docker compose exec -e PYTHONPATH=/opt/src spark \
	spark-submit --master $(SPARK_MASTER)

help:
	@echo "FlowState targets:"
	@echo "  make setup              uv lock/sync + config check"
	@echo "  make ingest             download/stage datasets (DATASET=all)"
	@echo "  make amplify            synthetic scale (ROWS=1000000)"
	@echo "  make up / make down     docker compose stack"
	@echo "  make hotspots           Spark demand job (in compose)"
	@echo "  make stream-produce     Kafka replay (TOPIC, RATE)"
	@echo "  make stream-consume     Structured Streaming (in compose)"
	@echo "  make hive-ddl           apply warehouse DDL via beeline"
	@echo "  make mr-package         Maven package MapReduce jar (Docker)"
	@echo "  make test / make lint"

setup:
	uv lock
	uv sync --all-groups
	$(PYTHON) -m flowstate.config --check

ingest:
	$(PYTHON) -m flowstate.ingest --dataset $(DATASET)

amplify:
	$(PYTHON) -m flowstate.ingest.amplify --rows $(ROWS)

up:
	docker compose up -d

down:
	docker compose down

hotspots:
	$(SPARK_SUBMIT) /opt/src/flowstate/batch/spark_jobs/demand_hotspots.py

stream-produce:
	$(PYTHON) -m flowstate.streaming.producer \
		--topic $(TOPIC) --events-per-sec $(RATE)

stream-consume:
	$(SPARK_SUBMIT) /opt/src/flowstate/streaming/rolling_metrics.py

hive-ddl:
	docker compose exec hiveserver2 beeline \
		-u jdbc:hive2://localhost:10000 \
		-f /opt/src/flowstate/warehouse/ddl/rides.sql

mr-package:
	docker run --rm \
		-v "$(CURDIR)/src/flowstate/batch/mapreduce:/src" \
		-w /src \
		maven:3.9-eclipse-temurin-8 \
		mvn -q -DskipTests package

test:
	uv run pytest -q

lint:
	uv run ruff check .
