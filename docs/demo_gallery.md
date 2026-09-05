# Project Demo Gallery

## Fraud Detection Command Center

The Streamlit monitoring interface reads committed Gold Delta
outputs produced by the real-time fraud-detection pipeline.

![Fraud Detection Dashboard](screenshots/dashboard-overview.png)

The dashboard exposes transaction volume, alert volume, fraud
rate, ML risk, severity and recent processing trends.

---

## Fraud Investigation View

![Recent Fraud Alerts](screenshots/recent-alerts.png)

Each alert can be investigated using transaction-level behavioral
and model signals including:

- transaction amount,
- historical deviation,
- 10-minute transaction velocity,
- device behavior,
- fraud-rule score,
- Random Forest probability,
- final severity,
- final decision basis.

---

## Engineering Health

![Engineering Health](screenshots/engineering-health.png)

The presentation layer also exposes engineering-oriented
indicators such as feature-enrichment coverage and event-to-score
latency.

---

## Spark Execution Evidence

![Spark History Server](screenshots/spark-history-server.png)

Spark event logs are persisted and exposed through the Spark
History Server, allowing post-run inspection of jobs, stages,
tasks, executors and shuffle activity.