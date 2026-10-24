# City Dashboard JSON

Grafana `grafana/grafana:13.2.0`. The historical vs live split is [docs/agents/city-dashboard.md](../../docs/agents/city-dashboard.md).

| File | Dashboard | Source |
| --- | --- | --- |
| `historical.json` | Historical (#27) | Hive warehouse |
| `live.json` | Live (#28) | `hdfs://namenode:9000/stream-sink/active_rides/` |

Do not put the demo map here. Do not put the `dashboards/explore` sandbox here. Do not add Superset.
