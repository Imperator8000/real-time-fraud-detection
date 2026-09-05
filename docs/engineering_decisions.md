# Engineering Decisions

This document records important design decisions made while
building the fraud-detection platform.

| Decision | Choice | Reason |
|---|---|---|
| Streaming engine | PySpark Structured Streaming | Demonstrates distributed event-time and stateful processing |
| Storage layer | Delta Lake | ACID transactions, schema-aware tables and reliable streaming sinks |
| Event transport | Apache Kafka | Partitioned real-time event ingestion and replay |
| Event-time policy | 5-minute watermark | Bounds state while tolerating intentionally late events |
| Behavioral aggregation | 10-minute window / 2-minute slide | Captures recent velocity while producing overlapping behavioral views |
| Persistent user behavior | `applyInPandasWithState` | Supports arbitrary long-lived per-user statistics |
| Online statistics | Welford algorithm | O(1) user-state footprint for mean and variance |
| Production payload | Fraud labels excluded | Prevents label leakage into live scoring |
| Training split | Temporal holdout | More realistic than randomly mixing future and past events |
| ML algorithm | Spark ML Random Forest | Distributed tabular model with interpretable feature importance and nonlinear behavior |
| Rule + ML design | Hybrid final decision | Allows deterministic controls and probabilistic detection to complement each other |
| Training labels | Separate ground-truth dataset | Keeps simulator truth outside production Kafka messages |
| Serving feature table | Immutable append-only feature events | Avoids treating MERGE-maintained snapshots as normal append streams |
| Retry-sensitive writes | Delta `txnAppId` + `txnVersion` | Protects Gold outputs against duplicate retries |
| Duplicate handling | Watermark + transaction-ID deduplication | Bounded exactly-once-style logical deduplication |
| Hot state key | Do not salt `user_id` | Salting would split one user's historical fraud state into multiple incorrect states |
| Shuffle tuning | Controlled benchmark | Avoids assuming that a higher partition count is automatically better |
| Backpressure | Bounded streaming ingestion | Reduces oversized recovery micro-batches without changing user-state semantics |
| Process orchestration | Docker Compose | Reproducible local multi-service demonstration |
| Runtime isolation | One Spark application per service | Allows independent restart and failure isolation |
| Monitoring | Query progress + Spark event logs | Covers both streaming behavior and lower-level Spark execution |
| Dashboard | Separate lightweight container | Prevents visualization dependencies from bloating every Spark service |
