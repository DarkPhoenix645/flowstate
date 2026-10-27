.PHONY: help setup ingest normalize stage lake amplify evaluate hotspots \
	spark-hdfs up down down-v \
	ensure-kafka-cluster-id \
	stream-produce stream-serve stream-ctl kafka-topics stream-consume \
	config-seed api hive-ddl \
	mr-package yarn-probe test lint \

PYTHON ?= uv run python
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
	@echo "  make amplify            corpus, lake put, rollup_conservation (ROWS, SEED)"
	@echo "  make evaluate           fit + record the held-out resampler evaluation"
	@echo "  make up                 mint Kafka CLUSTER_ID if needed, start stack"
	@echo "  make down               stop stack (keeps volumes + CLUSTER_ID)"
	@echo "  make down-v             stop stack, DELETE volumes, remint CLUSTER_ID"
	@echo "  make hotspots           Spark demand job (in compose)"
	@echo "  make spark-hdfs         Spark list+write HDFS probe (replication 1)"
	@echo "  make kafka-topics       create flowstate.*.v1 topics, 1 partition"
	@echo "  make config-seed        load the first model-config versions into Postgres"
	@echo "  make api                demo and run API on 127.0.0.1:8090"
	@echo "  make stream-serve       long-running producer: waits for control messages"
	@echo "  make stream-ctl         ARGS='start --rows 20000' | pause | seek T | command JSON"
	@echo "  make stream-produce     Kafka producer (RATE, SEED, PRODUCE_ROWS or REQUEST=file.json)"
	@echo "  make stream-consume     Structured Streaming (in compose)"
	@echo "  make hive-ddl           apply warehouse DDL via beeline"
	@echo "  make mr-package         Maven package MapReduce jar (Docker)"
	@echo "  make yarn-probe         Hadoop examples pi on YARN (SUCCEEDED)"
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
	$(PYTHON) -m flowstate.ingest.amplify $(if $(ROWS),--rows $(ROWS)) $(if $(SEED),--seed $(SEED))
	$(PYTHON) -m flowstate.lake.amplify
	$(SPARK_SUBMIT) /opt/src/flowstate/batch/spark_jobs/conservation_check.py
	$(COMPOSE) exec -T hiveserver2 beeline \
		-u jdbc:hive2://localhost:10000 \
		-f /opt/src/flowstate/warehouse/ddl/rollup_conservation.sql

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

kafka-topics:
	$(PYTHON) -m flowstate.streaming.topics

# The long-running producer (ADR-0017). Control it with `make stream-ctl`.
config-seed:
	$(PYTHON) -m flowstate.config.seed

api:
	$(PYTHON) -m flowstate.api

stream-serve:
	$(PYTHON) -m flowstate.streaming.service

stream-ctl:
	$(PYTHON) -m flowstate.streaming.ctl $(ARGS)

# REQUEST is a replay request JSON (ADR-0017).
# RATE, SEED, and PRODUCE_ROWS are passed only when set.
# Otherwise the producer uses StartRequest.
stream-produce:
ifdef REQUEST
	$(PYTHON) -m flowstate.streaming.producer --request $(REQUEST)
else
	$(PYTHON) -m flowstate.streaming.producer --mode replay $(if $(RATE),--events-per-sec $(RATE)) $(if $(SEED),--seed $(SEED)) $(if $(PRODUCE_ROWS),--rows $(PRODUCE_ROWS))
endif

stream-consume:
	$(SPARK_SUBMIT) /opt/src/flowstate/streaming/rolling_metrics.py

hive-ddl:
	$(COMPOSE) exec -T hiveserver2 beeline \
		-u jdbc:hive2://localhost:10000 \
		-f /opt/src/flowstate/warehouse/ddl/rides.sql
	$(PYTHON) -m flowstate.warehouse.load
	$(COMPOSE) exec -T hiveserver2 beeline \
		-u jdbc:hive2://localhost:10000 \
		-e "MSCK REPAIR TABLE rides;"

mr-package:
	docker run --rm \
		-v "$(CURDIR)/src/flowstate/batch/mapreduce:/src" \
		-w /src \
		maven:3.9-eclipse-temurin-8 \
		mvn -q -DskipTests package

# Spike proof for #9. Not DemandCount (#24).
yarn-probe:
	$(COMPOSE) exec -T resourcemanager bash -lc 'set -eu; \
	  yarn=/opt/hadoop-3.2.1/bin/yarn; \
	  jar=/opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar; \
	  "$$yarn" jar "$$jar" pi 1 1; \
	  list=$$("$$yarn" application -list -appStates FINISHED); \
	  printf "%s\n" "$$list"; \
	  printf "%s\n" "$$list" | grep QuasiMonteCarlo | grep -q SUCCEEDED'

test:
	uv run pytest -q

lint:
	uv run ruff check .

