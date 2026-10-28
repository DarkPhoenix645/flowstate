# City Dashboard JSON

Grafana `grafana/grafana:13.2.0`. The historical vs live split is [docs/agents/city-dashboard.md](../../docs/agents/city-dashboard.md).

| Folder | File | Dashboard | Source |
| --- | --- | --- | --- |
| `City/` | `historical.json` | Historical (#27) | Hive rollups |
| `City/` | `live.json` | Live (#28) | stream-sink external tables |
| `Questions/` | `analytics-*.json` | One catalog question (#91) | That question's Hive result table |

Do not put the demo map here. Do not put the `dashboards/explore` sandbox here. Do not add Superset.
