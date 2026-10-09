# Amplify: resampling ride events for scale

This is the method note for [ADR-0006](adr/0006-amplify-for-scale-benchmarks.md). It does not change that decision. It explains, from scratch, what gets resampled, what does not, and why each choice is the one it is.

**Status.** `src/flowstate/ingest/amplify.py` still prints `TODO`. Nothing in this file has been implemented or checked with `make test`. Loaders are stubs too, so there is no seed parquet yet. Treat the procedure below as the contract to implement, not as a description of running code.

**What this is not.** It is not a second schema, not a generative model, and not a license to synthesize metro, bus, or aviation. Kafka still carries the canonical ride event in [ADR-0005](adr/0005-canonical-ride-schema.md). The speed layer still replays staged rows ([ADR-0008](adr/0008-speed-layer-streaming-shape.md)). The timing comparison is still Spark against MapReduce at 1M / 10M / 100M ([ADR-0007](adr/0007-mapreduce-spark-benchmark-pair.md)).

---

## 1. The problem, without the pipeline jargon

Public mobility extracts are small. A ride-hailing CSV of order 10^5 rows is enough to see that mornings are busier than 3 a.m., and that some bookings are cancelled. It is not enough to show that a Spark job and a MapReduce job behave differently as the input grows. Those curves need inputs at 1 million, 10 million, and 100 million rows, on the same aggregations (zone by hour, cancellation counts).

Nobody is going to scrape 100 million real Indian trips for a coursework pipeline. The missing piece is a way to make a larger table that is still *the same statistical object* as the seed, so a timing difference is about the engine, not about a different mix of hours or cancel rates.

That larger table is what this repo calls **amplify**. The verb is resample. The unit of resampling is a **ride event** that already matches the canonical columns. Amplify does not invent a new kind of event.

A second requirement looks similar and is not the same problem. The speed layer needs a live feed. None of the sources is a live feed. The wrong fix is a second program that invents timestamps and publishes them. The locked fix is to publish the rows you already wrote, in order, at a controlled rate. Batch and stream are two readers of one file. They are not two samplers.

---

## 2. Words

Use these. The glossary in `CONTEXT.md` is the source of the names; the paraphrases here are only so this note can be read alone.

| Term | Meaning here |
|---|---|
| Ride event | One record with the canonical columns. Not a "message", not a "trip record", not a five-step lifecycle. |
| Canonical ride schema | `trip_id`, `city`, `mode`, `vehicle_type`, `timestamp`, `pickup_zone`, `drop_zone`, `status`, `fare`, `cancel_reason`, `source`. Extra columns may sit beside these. Kafka uses this set and no other. |
| Seed | Staged ride events that came from a real extract, after a loader mapped them onto those columns. Amplify reads the seed. It does not repair it. |
| Amplify | Draw seed rows with replacement until there are N rows, mint new `trip_id`s, write a separate parquet. N is 1e6, 1e7, or 1e8 for the report. |
| Staged parquet | Loader and amplify output under `data/staged/<dataset>/` on the host. This is not the data lake. |
| Data lake | The same files on HDFS inside Compose. Host `data/` is a staging mount. Writing a local parquet is not "the lake is filled." |
| Batch layer | Spark jobs and the MapReduce baseline reading the lake. |
| Speed layer | Host producer replays staged rows into Kafka topic `rides.raw`. Spark Structured Streaming, inside Compose, computes windows. |
| Warehouse | Hive tables over the lake, `PARTITIONED BY (city, vehicle_type)`. |

Hour-of-day mix means the fraction of rows in each clock hour. Cancel rate means the fraction of rows whose `status` is a cancellation. Both are properties of the seed. Amplify's job is to keep them. Loaders' job is to make the seed worth keeping.

---

## 3. What resampling actually is

Forget models for a moment. You have a table of n real rows. You want a table of N rows, N much larger than n, that a skeptic will accept as "the same data, more of it."

The empirical distribution of the seed is the discrete distribution that puts mass 1/n on each seed row. A draw from that distribution is: pick an integer uniformly from 0 .. n-1, take that row. Do that N times, independently, **with replacement**. The result is a multinomial allocation of N copies across n originals. On average each seed row appears N/n times. Some appear more, some less, none are edited.

That is bootstrap resampling of whole rows. Three properties matter, and they are why the method is this and not a formula.

**Joints are free.** A row is copied as a tuple. Pickup zone, drop zone, hour, vehicle type, status, fare, and cancel reason travel together. You do not have to remember which pairs are correlated. A trip that was cancelled keeps a null fare if the seed had a null fare. A long trip keeps its long fare. An airport pickup keeps the drop zone that was actually on that row, not a drop zone sampled from some other trip.

**Marginals you care about are preserved in expectation, and closely in practice at these N.** The hour histogram of the amplified table is a multinomial redraw of the seed histogram. At N = 1e6 the sampling noise on a proportion is on the order of 1/sqrt(N), which is small next to the differences a zone-by-hour query will print. The check in §10 is against the seed, not against a hypothetical "true" city. Amplify is faithful to the extract you staged. It is not a claim that the extract is faithful to Bengaluru.

**The method does not create information.** It cannot produce a zone the seed never used, a cancel reason the seed never used, or a timestamp the seed never used. Duplicate rows are clones: same timestamp, same zones, same fare, new `trip_id`. That is a feature for a throughput benchmark and a defect if someone reads the amplified counts as measured demand. The report must say the second thing out loud.

With-replacement is required. Without replacement you cannot exceed n rows. Sampling columns independently ("vehicle type from the vehicle-type frequency, fare from a lognormal, zone from a zone frequency") is a different distribution. It is the thing ADR-0006 forbids when it says pure random synthetic data would break cancellation and hour-of-day structure.

---

## 4. Why the project needs a larger table, and why only rides are scaled

Five pieces of work are in the milestone list (`docs/team.md`). Only two of them need more rows than the extracts contain: the Spark-vs-MapReduce timings, and a Kafka replay that can be paced faster than the original recording. They need different things from the data.

The timing comparison needs **cardinality**. The job has to scan more rows. It does not need new clock times, new cities, or more realistic physics. If hour mix or cancel rate drifts as N grows, a slower job might be slower because the aggregation key space changed, and the chart is no longer about Spark versus MapReduce.

The speed layer needs **a log you can publish**. ADR-0008 is explicit: the producer replays staged rides into `rides.raw` with `--events-per-sec` and event-time interpolation. Wall-clock rate is a sleep between publishes. Event time stays the row's `timestamp`. A streaming demo does not need 100 million generated instants. It needs the rows the batch job already trusts, emitted again.

So there is one write path and two read paths:

1. Loaders write seed parquet.
2. Amplify reads seed ride events and writes `rides_amplified` at the requested N.
3. Spark and MapReduce time themselves on that amplified parquet (after it is on the lake).
4. The producer replays a stated file — the seed for a short demo, or the same amplified file (or a stated slice of it) when the point is "same rows, two engines."

Step 4 must not call the sampler again. A second draw, even from the same function, is a different realization. "We used the same code" is not "we used the same rows." The Kafka-log recompute check (read the published events back in batch and compare window totals) is only defined if the published events are the file you already aggregated.

---

## 5. The datasets, and the decision for each

Ingest names five loaders (`python -m flowstate.ingest --dataset all|kaggle|bmrc|bmtc|mmrda|dgca`). Amplify is not a sixth source. It is a transform of staged ride events.

Evidence labels used below:

- **Repo** — a path, ADR, or comment that exists in this tree. I read it.
- **Source README** — checked against the upstream README on 2026-09-14. File layout, not a row-level audit.
- **Unverified** — named in conversation or a loader comment, not confirmed by a `df.columns` on a file this repo has staged. Do not write joins against these column lists until a loader prints them.

### 5.1 Ride-hailing extracts — the only legal seed

**What the repo says.** `src/flowstate/ingest/kaggle_rides.py` is a kagglehub downloader "for ride-hailing CSVs (datasets #1, #2, #7)". It is supposed to write standardized parquet at `data/staged/kaggle_rides/`. It does not download anything yet. It does not name Kaggle slugs. **Unverified:** exact column names, row counts, whether #7 is the GitHub XRides dump or a third Kaggle file, whether an Ola-versus-Uber flag exists.

**Decision.** These extracts, after mapping onto the canonical columns, are the seed. Nothing else is.

**Why.** They are the only sources whose natural grain is one booking. The benchmark aggregations (zone by hour, cancellation counts) are defined on that grain. The speed layer replays ride events, not daily totals. Scaling a booking table by copying bookings does not change what a row means. Scaling a daily ridership total by copying the total would.

**What the loader must do before amplify is allowed to run.** Map each source row onto the canonical columns. Standardize `timestamp` to one timezone and one ISO-8601 form. Standardize `status` and `cancel_reason` to one vocabulary. Fill `city` and `vehicle_type`, because those are Hive partition keys; amplify will refuse nulls in them rather than invent `"unknown"`. Put a dataset tag in `source`. Keep any source-only fields as extra columns if a later query needs them. Do not impute `fare` on cancelled rows. A null fare on a non-completed booking is a property of the extract, not a hole. Copying the row keeps the null. Filling it would manufacture revenue.

**Place names versus coordinates.** If a source has only a place name, `pickup_zone` / `drop_zone` may be that name. The schema allows a zone, a stop, or a grid id. Geocoding and H3 are loader enrichments, optional, and not part of amplify. Amplify will copy whatever zone string the loader wrote. A demand-hotspot chart on place names is weaker than one on coordinates. Say so in the report. Do not block resampling on Nominatim.

**Do not fit a fare model here.** A regression of fare on distance is a way to invent fares for rows that do not have them, or to jitter fares so clones look unique. The benchmark does not need unique fares. Jittering distance and fare independently destroys the very correlation a later query would "find." Jittering them together still invents values the seed did not contain, and it can put a fare on a cancelled row. Rejected.

### 5.2 XRides Bangalore — useful coordinates, not a second sampler

**What the upstream README lists** (Source README, [adityakumar9/Xride_Bangalore](https://github.com/adityakumar9/Xride_Bangalore); not staged in this repo). About 40,000 trips. Columns named in that README: `id`, `user_id`, `vehicle_model_id`, `package_id`, `travel_type_id`, `from_area_id`, `to_area_id`, `from_city_id`, `to_city_id`, `from_date`, `to_date`, `online_booking`, `mobile_site_booking`, `booking_created`, `from_lat`, `from_long`, `to_lat`, `to_long`, `car_cancellation`. Package ids are hour/km bundles. `travel_type_id` is long-distance, point-to-point, or hourly rental. `car_cancellation` is a 0/1 flag for cancellation due to no car. The README field list has **no fare**. Area ids have no lookup table in that description.

**What the repo says.** No loader named XRides. No occurrence of the string in the tree. The kaggle loader comment lumps "#7" into kagglehub. Those two statements cannot both be an implementation plan until someone picks a file and a slug.

**Decision.** XRides is not the streaming source as a special case. If and when a loader maps it onto the canonical ride schema, its rows join the seed and get resampled like any other ride event. Until then it is not input to amplify and not input to the producer.

**Why not a live generator driven by its timestamps.** It is the only named extract with per-trip coordinates and a booking timestamp, which makes it a good seed for zone-level ride events. That is a loader advantage, not a reason to emit a Poisson process. Replay already produces a stream. A thinning algorithm on top would replace `timestamp` and break the "same rows" rule.

**Why not the fare model either.** It has no fare column. Borrowing Uber fares and attaching them to XRides origin-destination pairs assumes vehicle type and price are independent of which trip supplied the coordinates. That assumption is untested. Whole-row resampling never has to make it: a row without a fare stays a row without a fare, and a Kaggle row with a fare keeps its own zones.

**Opaque ids.** Do not use `from_area_id` as `pickup_zone` unless a lookup exists. Prefer lat/long rounded or hashed into a zone in the loader, and document the function. Amplify does not cluster zones.

### 5.3 BMRCL Namma Metro daily ridership — not a seed

**What the repo says.** `bmrc_ridership.py` scrapes BMRCL HTML into `data/staged/bmrc_ridership/`. Stub.

**Grain.** Daily published totals for Bengaluru metro, not one row per rider. **Unverified** until a parse lands: exact column names, payment-method breakdown, how dirty the HTML is.

**Decision.** Never pass this directory to amplify. Never map a daily total into a fake list of ride events so it can sit on `rides.raw`.

**Why.** A daily count is already the resolution of "how did metro volume move over days." Copying it a thousand times does not create riders, and it destroys the comparison with real ride-hailing daily counts. Cross-modal work reads this table as itself and joins to **seed** ride events rolled up to `(city, date)`, not to `rides_amplified`.

### 5.4 BMTC GTFS — spatial backbone, not a seed

**What the source README shows** (Source README, [Vonter/bmtc-gtfs](https://github.com/Vonter/bmtc-gtfs)). Unofficial feed parsed from the Namma BMTC app. `gtfs/bmtc.zip` is the GTFS bundle. Convenience files include `csv/routes.csv`, `csv/stops.csv`, `csv/aggregated.csv`, plus GeoJSON. Raw archives include `raw/fares.7z` (fare matrix by fare-stage pair), routelines, stops, timetables. A standard GTFS zip contains `stops.txt` (`stop_id`, `stop_lat`, `stop_lon`, `stop_name`), `stop_times.txt`, `trips.txt`, `routes.txt`, and calendar files. This note did not open the zip. Treat exact filenames inside the zip as confirmed only after the loader lists them.

**What the repo says.** `bmtc_gtfs.py` writes `data/staged/bmtc_gtfs/`. Stub.

**Decision.** Do not resample GTFS into ride events. Do not emit a bus arrival process. Use stops as a zone or stop dimension for batch joins against **seed** ride events, if the ride zones can be aligned. Trip counts implied by `stop_times` / `trips` are scheduled service, a supply proxy. They are not ridership. The report must not call them demand.

**Why not "boost taxi λ when a bus is scheduled."** That writes a cross-modal effect into the generator and then discovers it. The honest version, later and optional, is a batch query: seed ride counts in a window after a scheduled stop, labeled as a hypothesis about the timetable, not as observed arrivals. It is not part of amplify. Maintainer comments about GPS coverage or timetable quality, if used in the report, need a citation to the README line. They were not re-checked for this note.

### 5.5 MMRDA metro and monorail — not a seed

**What the repo says.** `mmrda_ridership.py` parses a data.gov.in XLSX into `data/staged/mmrda_ridership/`. Stub. Grain, from the task description rather than a parsed file: daily ridership for Mumbai Metro Line 2A/7 and Monorail. **Unverified** column layout. OGD exports are often dirty; the loader owns the parse.

**Decision.** Same as BMRCL. Second city, same grain, batch fact only. Not amplified. Not streamed. Lets a cross-modal or ridership chart say "not only Bengaluru" without pretending Mumbai has zone-level ride events from this file.

### 5.6 DGCA / Ministry of Civil Aviation — not a seed

**What the source README shows** (Source README, [Vonter/india-aviation-traffic](https://github.com/Vonter/india-aviation-traffic)). Several aggregated CSVs, not one table:

- `aggregated/daily.csv` — Ministry of Civil Aviation daily summary (domestic, international, cargo, on-time performance, and related fields). Mid-2022 onward. The README says updates are irregular, not a true daily panel.
- `aggregated/domestic/city.csv` — monthly city-pair passenger, freight, and mail. Mid-2015 onward.
- `aggregated/domestic/carrier.csv` — monthly carrier traffic.
- `aggregated/international/city.csv` and a country file — quarterly international.

**What the repo says.** `dgca_aviation.py` writes `data/staged/dgca_aviation/`. Stub. Column names inside those CSVs were not opened for this note.

**Decision.** Do not resample. Do not downsample a monthly city-pair into daily ride events. Do not put flights on `rides.raw`.

**Why.** The grain is city and month (or quarter), for a different mode. It can sit next to a monthly rollup of seed ride bookings or metro totals and support a coarse "did aviation volume and local volume move together" question. It cannot support zone-by-hour demand. Forcing it into the ride schema would be a grain lie. Comparing it to `rides_amplified` would be a scale lie: amplified ride counts are copies, aviation counts are reported passengers.

### 5.7 What "multimodal" means, after those decisions

The generator is not multimodal. There is no generator. There is a copier of ride events.

The warehouse is multimodal in the only way the files support:

- **Zone-level batch joins** (seed ride events, and BMTC stops if zones align). Bus supply versus ride-hailing demand. Two modes, one spatial key, seed scale only.
- **City-day or city-month batch joins** (seed ride events rolled up, BMRCL, MMRDA, DGCA). Several modes, a calendar key, coarse on purpose.

A query that needs both does two scans and joins on date. It does not need one mega-table, and it must not read `rides_amplified`.

`mode` on the canonical schema is a label for rows that actually are ride-shaped (`ride_hail`, and only if some other source truly has one event per trip). It is not an invitation to emit a boarding or a flight from the producer.

---

## 6. Decisions that were considered and rejected

Each of these showed up as a serious alternative. They are rejected for this repo, not because the math is nonsense.

### 6.1 Two samplers (Uber for batch, XRides for stream)

Batch volume from one extract, stream timestamps from another. A timing gap can then be the shape of the data, not the engine. Indefensible next to ADR-0007, which requires the same aggregations at each scale.

### 6.2 One sampler function, two emission modes

Call `sample()` N times and write parquet. Call `sample()` again behind a non-homogeneous Poisson process and publish to Kafka. This sounds like "same model." It is two realizations. Event times in the stream are not the event times in the table. The recompute check is undefined. ADR-0008 already specifies the emission mode: replay, with `--events-per-sec`. Pacing is a sleep. It is not thinning.

### 6.3 Parametric fits (lognormal fare, independent origin and destination)

Fitting a family per column adds ways to be wrong (wrong family, broken joints) and does not help a scan-heavy benchmark. The one tempting exception is fare versus distance, which is roughly linear. Still rejected inside amplify: the seed row already contains the pair. A regression is only interesting if you must invent fares the seed does not have. This project does not. Loaders leave nulls as nulls.

### 6.4 Stratified bootstrap as the default

Split the seed by `(vehicle_type, status, hour)`, then draw k copies of each stratum. That also preserves those three margins, by construction. It is unnecessary if you copy whole rows: those margins, and every other joint, are preserved in expectation without declaring strata. Stratification also fails open: an empty stratum, a timezone bug in `hour`, or a status string that does not match the vocabulary, and you have silently reweighted the table. Use it only as a diagnostic if the fidelity check in §10 fails, and treat that failure as a bug in the draw, not as a prompt to add a model.

### 6.5 Correlated jitter so clones are not identical

Draw noise, apply it to distance, recompute fare from a per-vehicle rate. The stated motive is that a Hive query should not "discover" a broken fare-distance correlation. Whole-row copy does not break that correlation. Jitter is what risks breaking it, and it risks filling null fares. Duplicate continuous values do not bias `GROUP BY`. Rejected.

### 6.6 A non-homogeneous Poisson process for arrivals

Bin the seed by weekday and hour, take rates λ(h, d), simulate new times by Lewis-Shedler thinning (candidates from a homogeneous process at λ_max, keep each with probability λ(t)/λ_max). That is a correct way to simulate a point process with a given intensity. It is the wrong tool here.

A constant rate would flatten the surge demo. Direct replay of gaps only reproduces one recording and cannot extend past it without looping. Both criticisms assume the speed layer must **synthesize** time. It must not. Replay keeps the empirical intensity, including whatever clumps the seed actually had, and `--events-per-sec` plus event-time interpolation is how you speed the recording up. You do not need a new point process to get peaks. The peaks are in the `timestamp` column.

Thinning also has a sharp failure mode: if λ_max is below the true intensity, the algorithm is not correct and quietly drops events. There is no reason to carry that bug into a pipeline whose contract is "publish these rows."

### 6.7 A lifecycle state machine

`REQUESTED` then `DRIVER_ASSIGNED` then `STARTED` then `COMPLETED` or `CANCELLED`, with invented dwell times. The canonical schema has one `status` and one `timestamp` per ride event. Intermediate timestamps are not in the seeds (XRides has `booking_created`, `from_date`, `to_date`, not assignment time). Manufacturing them means the stream contains structure the report would have to apologize for.

Cancel rate is a ratio of statuses, not a ratio of synthetic terminal events. Active rides in a window can be defined from rows whose `timestamp` falls in the window. If that definition is too weak for the course rubric, change the streaming job's definition and write that down. Do not invent four extra Kafka event types. A private event schema on the bus is an invariant break (ADR-0005).

### 6.8 Hawkes excitation (train arrives, taxi rate jumps)

A Hawkes intensity is a baseline rate plus a decaying bump from past events. It is the right mathematical object for "this arrival causes more arrivals nearby." It is the wrong object for this data.

BMRCL and MMRDA do not have train-arrival timestamps. BMTC has scheduled stop times, which are not observed arrivals. α (how much one transit event lifts taxi demand) and β (how fast that lift decays) cannot be fit from co-located fine-grained events you do not have. Setting them to "0.3 and a 5-minute half-life because the literature says last-mile" writes the correlation into the stream. A later chart that shows the correlation is circular. The earlier rule — do not let an analytics job discover a generator artifact — applies here more than it applies to fare jitter.

Same-millisecond taxi and train is not a missing feature of the copier. The copier never emits trains. See §5.4 for the only honest analysis (a batch join, labeled as schedule).

### 6.9 GANs or any deep generator

Wrong cost, wrong explainability, and unnecessary once the unit of sampling is the row. Not in the stack. Would not preserve cancel rate unless you trained and checked for that explicitly, which is a worse version of the check resample gets for free.

### 6.10 Resampling every dataset to a common grain

Rejected. Daily metro counts, monthly city-pairs, and per-booking rides do not become comparable by drawing more of them. They become comparable by aggregating the fine table up to the coarse grain, at seed scale. Amplify's N is an engineering knob. It is not a ridership estimate.

### 6.11 Hive versus Spark as the timing pair, and 1×/10×/100×/1000×

Rejected. ADR-0007 is Spark versus MapReduce, same aggregations, at 1M / 10M / 100M, timings committed under `benchmarks/results/`. Hive is the warehouse. Partition keys stay `(city, vehicle_type)`, not `(source, ride_date)`, and there is no bucketing-on-zone requirement in the ADR. A 1000× blowup of a 150k extract is not the locked scale grid (and 1000 × 150k is 150M, which is not 100M). Report the three locked sizes, plus the `--seed` value (ADR-0006).

### 6.12 Per-zone Poisson processes so two zones can share a timestamp

Continuous-time Poisson processes have no simultaneous points, almost surely. That fact is irrelevant to a copier. The seed either contains two rows with the same `timestamp` and different zones, or it does not. Amplify can copy both rows. It can also copy one row twice, which is a same-timestamp tie at the **same** zone, distinguished only by `trip_id`. It will not mint a new time.

The speed layer's windows are 1-minute tumbling and 5-minute sliding, watermark 10 minutes on `timestamp` (ADR-0008, watermark still a spike). Spark already groups many zones in one trigger. Exact millisecond ties are not a deliverable. Rounding timestamps to milliseconds to "look like a database" changes event time and can merge distinct seed events. Do not do it in amplify or in the producer.

---

## 7. The procedure, decision by decision

Host only. `make amplify` runs `uv run python -m flowstate.ingest.amplify --rows $(ROWS)` (Makefile: `ROWS` defaults to 1000000). Spark is not involved. No host JDK. No Kafka.

### 7.1 Input

Read canonical ride events from staged ride-hailing parquet. The only directory the stubs name for that is `data/staged/kaggle_rides/`. If a future loader writes another ride-schema directory (for example a mapped XRides file), it may be included **only** if those files already match `RIDE_COLUMNS`. Do not scan `bmrc_ridership`, `bmtc_gtfs`, `mmrda_ridership`, or `dgca_aviation`.

Empty input is a hard failure. Do not synthesize filler rows. An empty staged directory means ingest has not been run, not that amplify should invent Bengaluru.

### 7.2 Rows amplify will not quietly fix

ADR-0006: loader work owns veracity of the seed; amplify does not fix bad upstream data.

Refuse the run if any seed row is missing `timestamp`, `status`, `city`, or `vehicle_type`.

- `timestamp` and `status` are the fidelity invariants. A row without them cannot be checked, and dropping it silently changes the mix.
- `city` and `vehicle_type` are warehouse partition keys (ADR-0005). A null cannot be loaded into `PARTITIONED BY (city, vehicle_type)` without a sentinel, and a sentinel is a fake city.

Do not impute them. Send the failure back to the loader.

`fare` may be null. `cancel_reason` may be null when `status` is not a cancellation. `pickup_zone` may be a place name. Copy all of that.

### 7.3 The draw

Arguments: `--rows N` and `--seed S` (the CLI already has both; default seed 42).

Use `S` to seed one RNG. Draw N integer indices uniformly from `0 .. n-1` with replacement. Materialize the selected rows in that order. That order is the replay order. Do not sort by time afterwards if you want the producer to be able to replay file order; do not shuffle again in the producer. One order, written once.

N may be any positive integer so `make amplify ROWS=10000` remains a smoke test (`docs/getting-started.md`). Smoke-test output is not a benchmark point. Fidelity noise is larger at 10k. The report uses only 1000000, 10000000, and 100000000.

### 7.4 What is copied and what is minted

Copy every canonical field except `trip_id` and `source`. Copy extra columns too, so a later query can still see a source booking id if the loader kept one.

**`trip_id`.** Mint a new id, deterministic in `(S, output_row_index)`. Output index, not the seed id alone: the same seed row drawn twice must yield two ids, or `COUNT(DISTINCT trip_id)` collapses toward n and the scale chart is a lie. Do not use a fresh random UUID unless it is drawn from the same seeded RNG and recorded. A rerun with the same `--seed` and same seed file must produce the same ids. Suggested form: `"{seed_trip_id}:{output_index}"`. Unique even when `seed_trip_id` repeats.

**`source`.** Set to `synthetic`. The schema's examples already include that tag. Reason: a warehouse scan that concatenates directories must be able to exclude amplified rows from a cross-modal query. Keeping `source=kaggle` on a copied row makes that exclusion a directory convention, which will get missed.

**Provenance.** ADR-0005 allows extra columns beside the canonical set. Write `seed_source` (the seed row's `source`) and `seed_trip_id`. Do not put those on the Kafka payload. `ride_event_schema.py` stays equal to `RIDE_COLUMNS`. Extra columns live in the parquet only.

Do not change `timestamp`, `status`, `cancel_reason`, zones, `fare`, `city`, `vehicle_type`, or `mode`. A date shift that "spreads one month across years" is rejected as a default. It is not required for the benchmark, it changes the calendar a cross-modal job might accidentally join, and hour-of-day preservation is already achieved by not touching `timestamp`. If someone later adds a shift, it has to keep the clock time and the status, and the output is still banned from cross-modal joins.

### 7.5 Output

Write a new directory. Do not overwrite the seed. The stub's own message says the destination is rides-amplified parquet. Use `data/staged/rides_amplified/`.

Keep `city` and `vehicle_type` as ordinary columns in that parquet. Warehouse DDL partitions by those two fields at load time. Do not invent a second partition scheme in the file layout.

One N per invocation. Three benchmark sizes means three runs, three directories or three explicitly named outputs, so a timing row can point at a path. Record `N` and `S` next to any timing (ADR-0006). Overwriting a 1M directory with a 100M run and citing the old timing is a false benchmark.

### 7.6 What this does to duplicate timestamps

Replacement creates clones: many rows, one timestamp, one pair of zones. That is not "two customers booked at once in different places." It is one historical booking counted several times, with distinct `trip_id`s so distinct-counts scale.

Different-location ties appear only if the seed already had them and both rows were drawn. Amplify does not add a collision step. For 1-minute windows this is enough. Interpreting clone multiplicity as concurrent demand is not allowed in the report.

---

## 8. How the stream uses the same rows

The producer (`src/flowstate/streaming/producer.py`) is a stub. The contract, from ADR-0008 and the Makefile:

- Host process. Kafka bootstrap for the host is `localhost:9094`. Containers use `kafka:9092`. Do not point the producer at `kafka:9092`.
- Topic default `rides.raw`.
- `--events-per-sec` (Make variable `RATE`, default 500). The streaming milestone asks for at least 1000 events/s. That is a publish-rate target, not a generative rate.
- Payload is the canonical columns, JSON, same names as `RIDE_COLUMNS`.
- Event-time interpolation: publish faster or slower than the recording, but the event time inside the payload stays `timestamp`. Windowing in Spark uses that field, with a 10-minute watermark until the watermark spike says otherwise.
- Consumers run in Compose Spark, one job per metric (surge, active rides, cancellations), not one opaque job.

**Which file.** Two legitimate choices, and they must not be blurred in the writeup.

| Goal | File to replay | Why |
|---|---|---|
| Show live windows on real mix | Seed parquet | Small, defensible as historical rides, no clone inflation |
| Show that live aggregates match a batch recompute | The same amplified file (or a stated contiguous slice) that the batch job read | Identity of rows |

Replaying the seed while the batch job scanned 100M copies does not validate the pipeline. Regenerating timestamps at publish time does not either.

Surge, in the workstream note, is a demand z-score against a **batch baseline**, not a transit-excited intensity. The baseline has to be computed on the seed (realistic mix), then applied to the replay. Computing the baseline on `rides_amplified` and then replaying those clones measures how often a copied row repeats, which is not a surge.

---

## 9. What each analytics job is allowed to read

| Job | Read | Do not read |
|---|---|---|
| Spark vs MapReduce timings (zone × hour, cancellation counts) | `rides_amplified` at 1M, 10M, 100M | A newly sampled stream; a Hive table built with different filters than the MR job |
| Demand hotspots, fare distributions, cancellation breakdowns presented as mobility findings | Seed ride events | `rides_amplified` (clone counts are not demand) |
| Cross-modal correlation, ridership trends, aviation seasonality | Seed ride rollups, BMRCL, MMRDA, DGCA, GTFS as labeled | `rides_amplified`; any synthetic metro or flight rows |
| Live windows | Replay of a stated file | A Poisson or Hawkes emitter |

Fare and cancellation stories that need a reason code come from whichever seed actually has `cancel_reason`. XRides, if ingested, has a boolean, not a reason vocabulary. Do not invent reasons to fill the column. Null is honest.

GTFS "route popularity" is scheduled trip frequency. Keep it in the GTFS staged tables. Do not copy those frequencies into `fare` or `status` on a ride event.

---

## 10. The fidelity check (this is the done condition for the method)

ADR-0006's consequence is that benchmark claims must state rows and seed. The check that makes "seed-faithful" a fact rather than a slogan:

On the seed and on the amplified output, compute:

1. Histogram of hour-of-day, taken from `timestamp` the same way in both (same timezone assumption the loader wrote; amplify does not reinterpret offsets).
2. Cancel rate: count of rows with cancellation `status` divided by N. The status strings must be the loader's vocabulary, not a second guess (`cancelled` versus `Cancelled by Customer`). If the loader collapsed reasons into one `status`, the check uses that `status`. Do not re-parse free text in amplify.
3. `vehicle_type` frequencies. Not an ADR invariant, but a cheap catch for a bad index draw, and it is a Hive partition key.
4. Row count equals N. Distinct `trip_id` equals N.
5. No nulls in `city` or `vehicle_type`. Null `fare` rate equal to the seed's, within sampling noise. This catches accidental imputation.

Compare amplified proportions to seed proportions. At N ≥ 1e6, a max absolute gap of a few tenths of a percent on hour shares and on the cancel rate is the band you should see from multinomial noise. If the gap is large, the draw is wrong (wrong column, dropped rows, unseeded shuffle, jitter). Do not "fix" it by reweighting after the fact.

This check is amplified versus seed. It is not a holdout versus reality. Whether the Kaggle extract matches Bengaluru is a loader veracity note, written when the loader actually runs, per dataset. Amplify cannot establish that, and must not be written up as if it had.

`make amplify ROWS=10000` may miss a tight band. Do not use that run to claim fidelity. Use it only to see that the command writes a file.

---

## 11. Failure modes

Named so an implementation can be reviewed against them.

1. **Empty staged dir.** Must fail. A green run that writes random rides is the forbidden pattern (unfaithful amplify).
2. **Scanning every dataset directory.** Daily metro totals copied to 100M rows and joined back to themselves is a nonsense cross-modal result.
3. **Jitter or imputation.** Fare-distance correlation and null-fare-on-cancel become artifacts.
4. **Reused `trip_id`.** Distinct counts do not scale. The benchmark query is wrong even if the clock time is fine.
5. **`source` left as the seed tag.** A cross-modal job includes clones and dwarfs BMRCL. `source=synthetic` plus a separate directory is the belt and braces.
6. **Date shift.** Hour mix can survive a shift that keeps the clock, and the calendar still becomes a fiction. Default is copy.
7. **Second sampler at publish time.** Stream and batch no longer match. Watermark policy (still a spike) gets blamed for a discrepancy that is actually two datasets.
8. **Producer on `kafka:9092` from the host, or Spark on the host.** Wrong runtime (ADR-0003). Not an amplify bug, but the usual way a "replay works on my laptop" story is false.
9. **Calling local parquet the lake.** Host `data/staged/` is not HDFS. Timings that never left the laptop SSD are not the Compose benchmark. Hive-on-HDFS and YARN submit are still open spikes. This note does not close them.
10. **Reporting GTFS frequencies or amplified counts as ridership.** Different lies, same section of the report.

---

## 12. What remains unresolved

These are not hidden in the procedure above.
    
- Kaggle slugs, column names, and row counts for datasets #1, #2, and #7. The loader comment is not a schema. A first `df.columns` is still required before any join is written against raw fields.
- Whether #7 is XRides. Until that is decided, XRides is not in the pipeline. Its README columns are not a Kafka schema.
- Timezone. The India extracts should be IST, but no ADR locks the zone string. The loader chooses one format and amplify copies it. Mixed offsets in the seed are a loader bug that amplify will scale up faithfully.
- How staged parquet is copied onto HDFS. Amplify's output path is host staged data. "Lake materialized" is a later load, and Hive warehouse-on-HDFS is a spike.
- Watermark and late data on replay. Default remains 10 minutes. Replay does not make that spike done.
- Exact cancel-status strings. They have to be one vocabulary in the loader before the fidelity check has a numerator.
- Geocoding. Optional, not started, not required for resampling. Place-name zones are legal and weaker.
- No fidelity check has been run. There is nothing to check.

---

## 13. How to defend this without overclaiming

Say this:

We did not generate a city. We copied historical ride events with replacement so Spark and MapReduce could scan 1M, 10M, and 100M rows of the same hour mix and the same cancel rate. The stream is that file published to Kafka at a controlled rate. Metro, bus, and aviation stay at their real grain and are joined only to the un-amplified ride events. Clone counts are not ridership. Scheduled bus trips are not ridership. Assumed cross-modal excitation was rejected because it would invent the correlation the analysis is supposed to test.

Do not say this:

The pipeline simulates realistic simultaneous multi-modal demand with a fitted Poisson process. Batch and stream were validated against each other. Amplify is done.

The first paragraph matches the ADRs. The second does not, and the code does not exist yet.
