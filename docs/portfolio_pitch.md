# Portfolio Copy

## GitHub Repository Description

Real-time financial fraud detection lakehouse using Kafka,
PySpark Structured Streaming, Delta Lake, stateful behavioral
features, Spark MLlib, Docker and a live monitoring dashboard.

---

## Recommended GitHub Topics

`pyspark`

`spark`

`structured-streaming`

`apache-kafka`

`delta-lake`

`machine-learning`

`fraud-detection`

`data-engineering`

`stream-processing`

`docker`

`mlops`

`data-lakehouse`

---

## Portfolio Website Title

**Real-Time Financial Fraud Detection Lakehouse**

---

## Portfolio Website Subtitle

A stateful streaming fraud-detection platform that combines
Apache Kafka, PySpark Structured Streaming, Delta Lake,
behavioral analytics, Spark MLlib and hybrid rule/ML scoring.

---

## Short Portfolio Description

Built an end-to-end real-time financial fraud-detection
lakehouse using Apache Kafka, PySpark Structured Streaming and
Delta Lake. The platform processes event-time transactions,
maintains short-term sliding-window features and long-lived
per-user behavioral state, performs distributed Random Forest
inference, combines ML predictions with deterministic fraud
rules, and produces explainable fraud alerts.

The system also includes checkpoint recovery, idempotent Delta
writes, data-quality quarantine, controlled Spark performance
benchmarks, state-store monitoring, Spark History Server
observability, automated testing, Docker orchestration and a
live fraud-monitoring dashboard.

---

## Resume Version — 3 Bullets

- Engineered an end-to-end real-time fraud-detection lakehouse
  using Apache Kafka, PySpark Structured Streaming and Delta
  Lake, including event-time processing, watermarks, sliding
  windows, transaction deduplication and Bronze/Silver/Gold
  architecture.

- Implemented long-lived per-user behavioral analytics with
  `applyInPandasWithState`, Welford online statistics and hybrid
  fraud rules, then trained and served a class-weighted Spark
  MLlib Random Forest using temporally separated training and
  evaluation data.

- Added production-style reliability and observability through
  Spark checkpoints, idempotent Delta transactions, automated
  recovery tests, Docker Compose orchestration, Spark event
  logs, performance benchmarking and a recruiter-facing fraud
  monitoring dashboard.

---

## LinkedIn Project Description

I built a real-time financial fraud-detection lakehouse to
demonstrate advanced PySpark and distributed streaming
engineering beyond notebook-based analytics.

Transactions are generated continuously and published to Apache
Kafka before being processed through a PySpark Structured
Streaming pipeline using a Bronze/Silver/Gold Delta Lake
architecture.

The platform includes event-time watermarks, sliding behavioral
windows, stateful transaction deduplication and arbitrary
per-user state with `applyInPandasWithState`. Historical user
statistics are maintained incrementally using Welford's
algorithm rather than storing an unlimited transaction history.

For fraud detection, I created a hybrid architecture combining
engineered behavioral rules with a Spark MLlib Random Forest.
The training labels are deliberately kept outside the live Kafka
payload to avoid label leakage, and model evaluation uses a
temporal holdout.

I also implemented checkpoint recovery, retry-safe Delta writes,
data-quality quarantine, Spark state-store monitoring, controlled
shuffle-partition benchmarks, Spark History Server observability,
Docker Compose orchestration and a live monitoring dashboard.

The project was designed not just to produce fraud predictions,
but to explore the engineering challenges behind a stateful,
recoverable and observable real-time analytics system.

---

## 30-Second Interview Pitch

I built a real-time financial fraud-detection lakehouse using
Kafka, PySpark Structured Streaming and Delta Lake. The
interesting part is that I maintain both short-term 10-minute
sliding-window behavior and long-lived per-user state with
`applyInPandasWithState`, including incremental statistics and
device behavior. I trained a Spark MLlib Random Forest and
combined its probabilities with deterministic rules for the
final fraud decision. I also implemented checkpoint recovery,
idempotent writes, performance benchmarks, Spark observability,
Docker orchestration and a live dashboard.

---

## 10-Second Version

A stateful PySpark fraud-detection platform that combines Kafka,
Delta Lake, behavioral streaming features, distributed Random
Forest inference, failure recovery and real-time monitoring.

---

## Suggested Social Preview Concept

**Main headline**

REAL-TIME FRAUD DETECTION LAKEHOUSE

**Secondary line**

Kafka • PySpark • Delta Lake • MLlib

**Center graphic**

Kafka
→ Streaming
→ Stateful Features
→ ML + Rules
→ Fraud Alert

**Three supporting metrics**

STATEFUL STREAMING

HYBRID ML + RULES

CHECKPOINT RECOVERY

**Bottom line**

Portfolio Data Engineering Project