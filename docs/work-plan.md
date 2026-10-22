# Work plan

This document is the go-forward after the 2026-10-18 HDFS review. It does not replace [ADR-0002](adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md), [ADR-0010](adr/0010-bengaluru-calibrated-resampler.md), or the [resampling plan](resampling-plan.md). Hold-out scores: [resampler-evaluation.md](resampler-evaluation.md). Read those for the locked product. Read this for what to build next and where files live.

Issue order still lives in `.scratch/branch-board.md`. Ticket text lives on GitHub (`gh issue view <n>`). This file records the layout and the product split that those tickets must follow. Do not keep a second copy of an issue body on disk.

## Two consumers, one resampler

The [resampler](adr/0010-bengaluru-calibrated-resampler.md) is one process. It feeds the data lake (`make amplify`) and Kafka (the producer).

The **demo** is the speed layer, the map, and Grafana. A user sees live events, scenario forks, and historical panels. The **report** is Spark versus MapReduce over the same amplified directory. **Findings** are Spark batch jobs over multimodal output. They are not DemandCount on three Namma Yatri files.

MapReduce on `namma_yatri` is the bootstrap for ticket #16. It is not the product end state.

When the tickets below are done, each surface reads a different store:

| Surface                                      | Ticket       | Reads                                                                          | Role               |
| -------------------------------------------- | ------------ | ------------------------------------------------------------------------------ | ------------------ |
| Map `/map`                                   | #48–#51      | Event envelope from Kafka through the demo API                                 | Demo               |
| Live Grafana                                 | #22, #28–#30 | Stream-sink from Structured Streaming                                          | Demo               |
| Historical Grafana                           | #23, #27     | Hive warehouse tables                                                          | Demo and findings  |
| Spark `cross_modal` and the other batch jobs | #18–#21      | `/lake/rides_amplified/` plus the source clocks where the join is the finding  | Findings           |
| MapReduce DemandCount                        | #24          | `/lake/rides_amplified/` at bench time. `#16` files stay as the observed clock | Report slide       |
| Amplify 1M / 10M / 100M                      | #25, #31     | Resampler output to `/lake/rides_amplified/`                                   | Shared scale input |

The map does not read HDFS. Grafana live reads the stream-sink. After #22 that sink is on HDFS. The lake is the shared history for the batch layer, the warehouse, and the archived stream.

## Where data lives

| Store                      | Path                                                    | What it holds                                                                 |
| -------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Host raw                   | `data/raw/`                                             | Download cache. 3.5 GB today. Rebuild costs OpenSky credits and GitHub auth   |
| Host staged                | `data/staged/`                                          | Source tables. 57 MB. Rebuild from raw with `make stage`                      |
| Observed clock on the lake | `hdfs://namenode:9000/lake/namma_yatri/`                | Three ride-hail parquet files from `make lake` (#16)                          |
| Product corpus on the lake | `hdfs://namenode:9000/lake/rides_amplified/`            | Multimodal ride events from the resampler. All four modes                     |
| Warehouse                  | `hdfs://namenode:9000/user/hive/warehouse/`             | Metro, BMTC stops, DGCA, and later `rides`. Already loaded for three of those |
| Kafka                      | `flowstate.*.v1` topics                                 | Live envelope for the speed layer and the map                                 |
| Stream archive             | `hdfs://namenode:9000/stream-sink/` and `/checkpoints/` | After #22. Lambda master dataset and fault-tolerant checkpoints               |

Do not copy metro, BMTC, or DGCA under `/lake/<dataset>/`. Those tables already live as Hive external tables. `/lake/` holds what MapReduce and Spark scan directly: the observed clock and the amplified corpus.

Host `data/lake/` is a copy-in mount for `hdfs dfs -put`. It is not the lake. `make lake` already removes the ride-hail copy-in after the put. `data/lake/hive-load/` stays until #23 loads warehouse files from the host client or from `-put` straight to the table LOCATION.

## What stays on the host

Keep `data/raw/` and `data/staged/` on disk.

- `make down-v` deletes HDFS volumes on purpose. Raw on HDFS would force a re-download.
- `make ingest` and `make stage` must work with no cluster. ADR-0003 CI does not boot Compose.
- Excel and GTFS zip parsers want seekable local files. HDFS is a poor fit for that grain.

The glossary already names host `data/` as a staging aid. Moving the whole tree onto HDFS adds no lake capability. It makes ingest and tests cluster-dependent.

## Work in order

Do the rows in this order. A later row can start only when its blockers in the table are done. Amend ADR-0003 in the same branch as the Spark conf change. Do not open a new ADR for the host-versus-lake split. This file is that record.

### 1. #16 on `feat/008` — code is ready

`make lake` puts the three staged ride-hail files on `hdfs://namenode:9000/lake/namma_yatri/`. Host copies stay. No WebHDFS client. The put path is `docker exec` `/opt/hadoop-3.2.1/bin/hdfs`. Bytes matched the host files on 2026-10-18.

Compose binds namenode 9870/9000 and Kafka 9092/9094 to `127.0.0.1`. HDFS permissions are off. Recreate namenode and kafka after the merge so the binds take effect.

Ticket bodies on GitHub match this file as of 2026-10-22. Do not keep local copies of those bodies.

Done when: the pull request is open and merged. Tests already pass on this branch.

### 2. Calibrated resampler — `feat/009`, #34

Build the three-layer resampler. One module serves `make amplify` and the producer. Emit all four modes with provenance. Amplify writes a thin copy of the alignment window (ADR-0013), not a handful of full source days. Live wall-clock emit stays #39.

Write amplify output as many parquet part files. Target about 128 MB per part. That size is one HDFS block (live default `blockSize` is 134217728). MapReduce and Spark split work by block. Record `--seed`. Never overwrite a source file.

Done when: the #34 acceptance list passes, and the output contract above is in the amplify writer.

### 3. Spark reads the lake — `feat/010-spark-hdfs`, #7

The old spike asked if Spark can see host files at `/opt/data`. That question is obsolete. Batch jobs read `hdfs://namenode:9000/lake/...`.

Mount the existing Hive Hadoop conf into the Spark master and the Spark worker. Set `HADOOP_CONF_DIR`. That ships `dfs.replication=1`. Without it, a Spark write uses replication 3 and fails on one datanode. This is proven: the Spark container has no Hadoop conf today.

Keep the `./data` mount until #22 moves the stream-sink off the host. #18–#21 now say the jobs read `hdfs://` after #7.

Done when: Spark can `ls` an `hdfs://` path, a write succeeds at replication 1, and ADR-0003 no longer requires `/opt/data` as the batch input.

### 4. Warehouse `rides` — `feat/016`, #23

Write the real partitioned `rides` DDL. Load from the lake. Keep `docker exec hdfs dfs -put` as the default loader.

Add an `fsspec` WebHDFS client only if the load must skip the copy-in. If you add it, follow the client rules in the agent briefing. Publish `127.0.0.1:9864:9864` on the datanode in that same change.

Done when: Beeline can `SELECT` from `rides`, and `make hive-ddl` is no longer a stub.

### 5. Amplify onto the lake — #25 then #31

Write `/lake/rides_amplified/` beside `namma_yatri`. Run 1M on the Compose cluster. Run 10M and 100M on a small GCP Dataproc cluster (about 3–4 workers). Quote rows and `--seed` every time.

Measured size for a canonical ride-event parquet (Snappy, 1M synthetic rows): about 57 bytes per row. That is about 57 MB at 1M, 0.57 GB at 10M, 5.7 GB / ~43 blocks at 100M. The in-memory working set at 100M is tens of GB. That is why the 100M row belongs on GCP. The single-node Compose stack cannot show a real shuffle.

The map does not need 100M events. Draw a live or replayed envelope at human time.

Done when: `/lake/rides_amplified/` exists at 1M, and the 10M / 100M rows are either on disk or recorded as a GCP run in `benchmarks/results/timings.csv`.

### 6. Producer and speed layer — #39, then #22 / #17 / #29 / #30

Emit the `flowstate.*.v1` topics from the same model bundle. Default demo: a flight-arrival command changes a configured synthetic airport-access ride-hail response. Mark it `assumption_driven` unless a holdout validated that coefficient.

After Spark has Hadoop conf, put checkpoints at `hdfs://namenode:9000/checkpoints/<job>`. Put the stream-sink on HDFS as the archive of the live run.

Done when: a scenario command shows on the stream, consumers filter by `scenario_id` and provenance, and checkpoints recover after a worker restart.

### 7. Findings jobs, benchmark, Grafana, demo UI

Spark jobs (#18–#21) scan `/lake/rides_amplified/` and join source clocks where the finding is the join. MapReduce (#24) mirrors the hotspots aggregation on that same directory. #26 and #31 write timing rows. #12 / #27 / #28 wire Grafana. #41–#51 is the demo UI. Grafana stays the City Dashboard. The map draws envelope events. It does not replace Grafana.

## Honest answers

A cross-modal question is only honest at the coarsest grain among its legs ([data-sources.md](data-sources.md)). That rule applies to every user-facing answer, not only the report.

| Query class                                                                | Grain we have                                                                                                      | Honest v1 answer                                                                                                 |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| Day-level auto ↔ metro coupling                                            | City-day clocks, copula on shared dates                                                                            | Yes. This is the report cross-modal quantity. Association, not cause                                             |
| Hour shape, station load, ward load                                        | RTI 48 days. City-hour is ~1% of volume. Ward-hour is a spatial prior                                              | Yes, with provenance tags. Missing is an absence, not a zero                                                     |
| Live busy / surge / cancel in this window                                  | Kafka envelope plus 1-minute and 5-minute jobs                                                                     | Yes. That is the speed layer                                                                                     |
| Scenario "if a flight lands / a line stops"                                | Assumption-driven unless the coefficient survives holdout                                                          | Yes as simulation. Default demo is this. Not a causal finding                                                    |
| Person itinerary, recommended mode, true origin-destination, observed fare | No trip-level bookings. Metro v1 is station-hour. Bus boardings are synthetic. Ride-hail is ward, not a coordinate | Not a validated product answer in v1. You can draw places and simulate load. You cannot claim a recommended mode |

Write the live-query catalog in the demo tickets. Do not treat one sketch question as the product spec. A routing-class question later needs a new ticket and an ADR. It would be an assumption-driven router over the synthetic field, with the same provenance rules as bus boardings.

## Optional later

These are not in the current branch board. Do them only if the report or the demo needs them.

- An opt-in `docker-compose.scale.yml` with two extra datanodes, distinct host ports for 9864, and `dfs.replication=2`. Use it to show `fsck` block locations and re-replication. Keep the default stack at one datanode.
- One billion rows (~57 GB). ADR-0006 caps `N` at 1e8. Change that only with a new ADR if 100M on GCP finishes too fast to be a useful contrast.
- Host-side WebHDFS as the default loader. Keep `docker exec` until #23 proves a need.

---

## Agent briefing

Skip this section unless you implement a row above.

### Live pointers

Live branch and ticket: `.scratch/phone-home.md`. Work order: `.scratch/branch-board.md`. Ticket text: `gh issue view <n>`.

### Do not touch

- `src/flowstate/normalize/` and the #37 tables
- `data/raw/`, `data/geocode/`. Do not run `make ingest`. OpenSky spends credits. Ride-hail needs `gh auth login`
- `feat/007-opensky-backfill`. #38 is closed
- BMTC boarding invention. That is #40
- `data/geocode/` and `data/normalized/` stay untracked

### Layout rules

- Host raw and staged stay on disk. Lake = HDFS.
- `/lake/` = `namma_yatri` (observed clock) + `rides_amplified` (product corpus). Warehouse = Hive LOCATIONs already in `src/flowstate/warehouse/ddl/`.
- Batch jobs read `hdfs://namenode:9000/lake/rides_amplified/` after #7. The map reads the Kafka envelope. Grafana live reads the stream-sink (on HDFS after #22).
- One resampler module. Amplify and the producer import it. No second generator.
- Provenance on every event. Do not label synthetic as observed. Do not add synthetic bus boardings to observed metro or ride-hail totals.

### HDFS client (only if #23 adds it)

Keep `docker exec` `/opt/hadoop-3.2.1/bin/hdfs` as default. `hdfs` is not on PATH in a login shell.

If you add `fsspec` (2026.9.0 source checked):

- `port=9870`. Default is 50070 and is closed.
- `user="root"`.
- `data_proxy` must be a **callable**. Dict form matches a literal hostname. The namenode Location uses the datanode **container id** (`e8317e4ed0e9` on the 2026-10-18 probe). That id changes on recreate.
- Rewrite only host:port to `127.0.0.1:9864`. Leave `namenoderpcaddress=namenode:9000`.
- Publish `127.0.0.1:9864:9864` on the datanode. Host PUT to unpublished 9864 is connection refused (curl exit 7). PUT from a reachable peer returns 201.
- `autocommit=False` then rename. `WebHDFile.commit()` is `fs.mv`.
- Delete the destination before `mv`. `RENAME` onto an existing file returns `{"boolean":false}` and changes nothing. fsspec `mv` discards that body. Proven live: A stayed 32 bytes, B stayed 10 bytes.
- Mode `"w"` sends `overwrite=true` on CREATE. Plain `open(..., "wb")` does overwrite. The silent-fail trap is the temp-then-rename path only.
- Writes are CREATE-empty then buffered APPEND chunks. For large part files, prefer one streamed PUT or `hdfs dfs -put dir/`.
- `tests/test_lake.py` currently forbids `-rm` in commands. Delete-before-rename changes that assertion on purpose.

Do not set `HDFS_CONF_dfs_client_use_datanode_hostname`. `MULTIHOMED_NETWORK=1` is in the bde2020 image. The entrypoint appends both `use.datanode.hostname=true` flags **after** `HDFS_CONF_*` processing. An env var loses. Proven live in `/etc/hadoop/hdfs-site.xml`. Do not set `dfs.datanode.hostname=localhost`. That breaks `make lake` inside the namenode container.

### Live probe facts (2026-10-18)

- `MULTIHOMED_NETWORK=1`
- WebHDFS `noredirect` CREATE Location: `http://<container-id>:9864/...&namenoderpcaddress=namenode:9000`
- `fsck /lake/namma_yatri`: HEALTHY, replication 1, one block per file, sizes 127006 / 1024853 / 47241015. Match host `stat`
- Spark `HADOOP_CONF_DIR` unset. No `/opt/hadoop` in the Spark image
- Compose publishes namenode 9870/9000 and Kafka 9092/9094 on `127.0.0.1` after #16

### Files to read first

`CONTEXT.md`, this file, `docs/adr/0002`, `0003`, `0006`, `0007`, `0008`, `0010`, `docs/resampling-plan.md`, `docs/data-sources.md`, `docs/dev-modes.md`, `docs/agents/spikes.md`, `docs/agents/staged-tables.md`, `src/flowstate/lake/ridehail.py`, `infra/hadoop/hadoop.env`, `infra/hive/hdfs-site.xml`, `docker-compose.yml`. Ticket body: `gh issue view <n>` (updated 2026-10-22).

Research chat exports in `.scratch/validate/` are background. This file wins if they disagree.
