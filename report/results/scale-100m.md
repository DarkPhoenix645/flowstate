# Scale corpus, seed 42

Window `2026-02-17` through `2026-10-28`. 254 whole days. Each ride-hail day is that day's Bengaluru city-day total (bookings, completions, and cancels). Days after `2026-04-15` repeat the same weekday from inside the alignment window and are marked outside the training window. The row count is the closest whole-day sum to 100 million. No day was thinned.

| Item | Value |
| --- | --- |
| Rows | 100237848 |
| Ride-hail | 100010969 |
| Metro | 205133 |
| Aviation | 21746 |
| Bus | omitted. The GTFS feed is one service day, not a calendar |
| Seed | 42 |
| Bundle | resampler-1.0.0 |
| Bundle hash | `ccd21a7d4e4affd172c3473fc465831ff400bae599dc0e397d67c6fcaa0a6b7b` |
| Provenance | synthetic 100224767, observed_proxy 13081 |
| Aviation coverage | source days on or before 2026-04-15 |

Grafana runs on this corpus:

| Question | Run |
| --- | --- |
| Ridership trends | `6ff4cad4-1ab9-4199-9b17-9f5ce3bb3ed5` |
| Ride-hail and metro, and flights and ride-hail | `0a347a0c-56e6-4f8e-ba89-94a31611017f` |

Open Dashboards, then Questions. The Run menu lists these first.
