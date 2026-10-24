# City Dashboard split

Decision: [ADR-0009](../adr/0009-grafana-city-dashboard.md). This note is the #12 sketch. It names which Grafana dashboard reads which source. Which questions the panels answer is #67. Wiring is #27 and #28.

The product is Grafana `grafana/grafana:13.2.0` on host port 3000. Login is `admin` / `admin`. Dashboard JSON lives under `dashboards/grafana/`. Do not add Superset. `dashboards/explore` is a local sandbox. It is not the City Dashboard.

The map is the demo UI (#41-#51). It reads the Kafka envelope. It is not a Grafana panel.

Compose mounts `./dashboards/grafana` at `/var/lib/grafana/dashboards`. Grafana does not load that directory until a provisioning file points at it. #27 adds that file.

## Two dashboards

Use two dashboards. Do not mix the sources in one dashboard.

| Dashboard | File | Reads | Does not read | Ticket |
| --- | --- | --- | --- | --- |
| Historical | `dashboards/grafana/historical.json` | Hive warehouse | Stream-sink | #27 |
| Live | `dashboards/grafana/live.json` | `hdfs://namenode:9000/stream-sink/active_rides/` | Hive `rides` | #28 |

## Historical

The query target is HiveServer2 JDBC on port 10000, database `default`. The warehouse is `hdfs://namenode:9000/user/hive/warehouse`. Checked 2026-10-24: the running `grafana/grafana:13.2.0` image has no Hive plugin. #27 installs the datasource that talks to that JDBC port. Do not add Trino, DuckDB, or Superset for this path.

The first chart is Hive `rides`, after #23 loads that table. `rides.sql` is still a comment. Do not query it before the load.

These tables have a real `CREATE TABLE` today. Extra charts on them are optional for #27:

| Table | Grain |
| --- | --- |
| `metro_daily` | One row per tracker day |
| `metro_station_hour` | Station-hour. A missing cell stays missing. |
| `bmtc_stops` | Stop list. This is not ridership. |
| `dgca_city_month` | Month. There is no DGCA Kafka topic. |

`metro_ridership.sql` and `aviation.sql` are comments. Do not query them until they are real tables.

Do not add synthetic bus boardings to an observed metro total or an observed ride-hail total.

## Live

The chart is `activity_count` by `ward_id` from the active-rides sink, after #22. Columns are `ward_id`, `provenance_status`, `scenario_id`, `event_type`, `activity_count`, `window_start`, and `window_end`. Keep `provenance_status` and `scenario_id` as filters.

A longer bucket is the sum of the 1-minute rows. Cut hour and day in IST in the panel query. Spark already wrote UTC windows.

Do not read host `data/stream-sink`. How Grafana opens the HDFS path is #28, including sink partitioning.

Cancel-rate and surge sinks are not on this dashboard. Those jobs are #29 and #30. #67 may drop them.
