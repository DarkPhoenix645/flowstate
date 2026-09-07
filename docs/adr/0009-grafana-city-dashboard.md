# ADR-0009: Grafana for City Dashboard

## Status

Accepted

## Context

ADR-0002 listed Apache Superset (with Grafana as an option) for the City Dashboard. The speed layer writes a live stream-sink. Apache Superset does not support streamed data sources for visualizations.

The questionnaire locked Grafana only.

## Decision

Use Grafana for batch views over Hive and live views over the stream-sink. Do not run Apache Superset.

- Compose service: `grafana`, image `grafana/grafana:13.2.0`.
- Dashboard JSON lives under `dashboards/grafana/`.
- Keep `dashboards/explore` as a local Plotly/Streamlit sandbox. It is not the City Dashboard.

Hive datasource wiring and live panels stay later tickets (T11 / T26 / T27). This ADR locks the product, not those panels.

This supersedes the dashboard technology rows in ADR-0002 and the cluster list in ADR-0003.

## Consequences

- Grafana listens on host port 3000. Default login is `admin` / `admin`.
- Do not add a Superset service or export path.
- Changing the Grafana image tag is a Compose pin change (ADR-0003).

## Sources

`docker-compose.yml`, `docs/architecture.md`, `docs/dev-modes.md`
