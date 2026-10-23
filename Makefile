.PHONY: help setup ingest normalize stage lake amplify evaluate hotspots \
	spark-hdfs up down down-v \
	ensure-kafka-cluster-id \
	stream-produce stream-consume hive-ddl mr-package test lint \

PYTHON ?= uv run python
ROWS ?= 1000000
SEED ?= 42
TOPIC ?= flowstate.ridehail.activity.v1
RATE ?= 500
DATASET ?= all
SPARK_MASTER ?= spark://spark:7077
COMPOSE = docker compose --env-file infra/kafka/cluster.env
SPARK_SUBMIT = $(COMPOSE) exec -e PYTHONPATH=/opt/src \
	-e HADOOP_CONF_DIR=/etc/hadoop spark \
	/opt/spark/bin/spark-submit --master $(SPARK_MASTER)

help:
	@echo "FlowState targets:"
	@echo "  make setup              uv lock/sync + config check"
	@echo "  make ingest             DATASET=all|ridehail|metro|bus|aviation|opensky|events|manifest|validate"
	@echo "  make normalize          analysis tables from files already on disk"
	@echo "  make stage              parquet from cold extracts. aviation is DGCA only, not OpenSky"
	@echo "  make lake               put staged ride-hail parquet on HDFS"
	@echo "  make amplify            synthetic scale (ROWS=1000000 SEED=42)"
	@echo "  make evaluate           fit + record the held-out resampler evaluation"
	@echo "  make up                 mint Kafka CLUSTER_ID if needed, start stack"
	@echo "  make down               stop stack (keeps volumes + CLUSTER_ID)"
	@echo "  make down-v             stop stack, DELETE volumes, remint CLUSTER_ID"
	@echo "  make hotspots           Spark demand job (in compose)"
	@echo "  make spark-hdfs         Spark list+write HDFS probe (replication 1)"
	@echo "  make stream-produce     Kafka publish (TOPIC, RATE)"
	@echo "  make stream-consume     Structured Streaming (in compose)"
	@echo "  make hive-ddl           apply warehouse DDL via beeline"
	@echo "  make mr-package         Maven package MapReduce jar (Docker)"
	@echo "  make test / make lint"

setup:
	uv lock
	uv sync --all-groups
	$(PYTHON) -m flowstate.config --check

ingest:
	PYTHONUNBUFFERED=1 $(PYTHON) -m flowstate.ingest --dataset $(DATASET)

normalize:
	PYTHONUNBUFFERED=1 $(PYTHON) -m flowstate.normalize

stage:
	PYTHONUNBUFFERED=1 $(PYTHON) -m flowstate.stage --dataset $(DATASET)

lake:
	PYTHONUNBUFFERED=1 $(PYTHON) -m flowstate.lake

amplify:
	$(PYTHON) -m flowstate.ingest.amplify --rows $(ROWS) --seed $(SEED)

evaluate:
	$(PYTHON) -m flowstate.resample.evaluate

ensure-kafka-cluster-id:
	$(PYTHON) infra/kafka/ensure_cluster_id.py

up: ensure-kafka-cluster-id
	$(COMPOSE) up -d

down: ensure-kafka-cluster-id
	$(COMPOSE) down

# Destructive: drops namenode/datanode/kafka/hive volumes AND remints CLUSTER_ID.
# Use when Kafka fails to start after an id/format mismatch, or you want a clean lake.
down-v: ensure-kafka-cluster-id
	@echo "WARNING: removing Compose volumes (HDFS, Kafka, Hive data) and reminting CLUSTER_ID"
	$(COMPOSE) down -v
	$(PYTHON) infra/kafka/ensure_cluster_id.py --force

hotspots:
	$(SPARK_SUBMIT) /opt/src/flowstate/batch/spark_jobs/demand_hotspots.py

spark-hdfs:
	$(SPARK_SUBMIT) /opt/src/flowstate/batch/spark_jobs/hdfs_probe.py

stream-produce:
	$(PYTHON) -m flowstate.streaming.producer \
		--topic $(TOPIC) --events-per-sec $(RATE)

stream-consume:
	$(SPARK_SUBMIT) /opt/src/flowstate/streaming/rolling_metrics.py

hive-ddl:
	$(COMPOSE) exec hiveserver2 beeline \
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

