# Recruiter Demo Guide

## Goal

Demonstrate the project in approximately five minutes while
starting with the business outcome and then drilling into
engineering depth.

---

## 1. Start the platform

From the repository root:

```powershell
docker compose -f docker\docker-compose.yml --profile demo up -d
```

---

## 2. Open the Fraud Command Center

Open:

`http://localhost:8501`

Start with:

- transaction volume,
- fraud alerts,
- fraud rate,
- critical alerts,
- ML probability,
- recent fraud investigations.

Explain that the dashboard is reading committed Gold Delta
outputs rather than performing fraud detection itself.

---

## 3. Explain one alert

Open **Recent Alerts**.

Select a transaction and discuss signals such as:

- historical amount deviation,
- transaction velocity,
- device behavior,
- rule score,
- Random Forest probability,
- final severity,
- final decision basis.

---

## 4. Show the architecture

Open:

`docs/architecture.md`

Explain:

Kafka
→ Bronze
→ Silver
→ window/stateful feature paths
→ immutable Gold features
→ Random Forest
→ hybrid fraud decision.

---

## 5. Show advanced Spark engineering

Highlight:

- event-time watermarks,
- 10-minute sliding windows,
- 2-minute slide,
- stateful deduplication,
- `applyInPandasWithState`,
- Welford online statistics,
- Delta MERGE,
- idempotent Delta writes,
- checkpoint recovery.

---

## 6. Show performance evidence

Open:

`docs/performance/benchmark_summary.md`

Explain that 4, 8, 16 and 32 shuffle partitions were measured
under both balanced and deliberately skewed traffic.

The chosen configuration is based on measured throughput,
P95 trigger latency and state-store memory.

---

## 7. Show Spark internals

Open:

`http://localhost:18080`

Use Spark History Server to inspect:

- stages,
- tasks,
- shuffle,
- executors,
- SQL execution,
- Spark configuration.

---

## 8. Show reliability

Mention:

- Spark checkpoints,
- Docker restart policies,
- Delta ACID commits,
- retry-safe writes,
- quarantine,
- recovery integration tests.

---

## Suggested 30-Second Summary

> I built a real-time fraud-detection lakehouse using Kafka,
> PySpark Structured Streaming and Delta Lake. It maintains
> both short-term sliding-window behavior and long-lived
> per-user state using applyInPandasWithState. A Spark MLlib
> Random Forest is combined with deterministic fraud rules
> for final scoring. I also implemented checkpoint recovery,
> idempotent Delta writes, automated tests, performance
> benchmarking, Spark observability and a live monitoring
> dashboard.
