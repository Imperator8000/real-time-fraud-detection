from __future__ import annotations

from pathlib import Path
from textwrap import dedent


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

DOCS_DIRECTORY = (
    PROJECT_ROOT
    / "docs"
)

SCREENSHOT_DIRECTORY = (
    DOCS_DIRECTORY
    / "screenshots"
)


# ============================================================
# ARCHITECTURE
# ============================================================

ARCHITECTURE_DOCUMENT = dedent(
    r"""
    # System Architecture

    ## Real-Time Financial Fraud Detection Lakehouse

    This project demonstrates a stateful financial fraud-detection
    platform built around Apache Kafka, PySpark Structured Streaming,
    Delta Lake and Apache Spark MLlib.

    The architecture intentionally separates:

    - ingestion,
    - data quality,
    - event-time analytics,
    - persistent per-user state,
    - machine-learning inference,
    - final fraud decisions,
    - reliability,
    - observability,
    - and presentation.

    ---

    ## High-Level Architecture

    ```mermaid
    flowchart TB

        %% ====================================================
        %% INGESTION
        %% ====================================================

        subgraph INGESTION["1. Event Ingestion"]
            GEN["Synthetic Transaction Generator"]
            KP["Kafka Producer"]
            KAFKA[("Kafka<br/>transactions<br/>6 partitions")]

            GEN --> KP
            KP --> KAFKA
        end


        %% ====================================================
        %% MEDALLION / DELTA
        %% ====================================================

        subgraph LAKEHOUSE["2. PySpark + Delta Lake"]

            BRONZE["Bronze Kafka Stream"]
            BRONZE_DELTA[("Bronze Delta<br/>Raw + Kafka Lineage")]

            QUALITY["Data Quality Rules"]
            DEDUPE["5-Min Watermark<br/>Deduplication"]

            SILVER_DELTA[("Silver Delta<br/>Validated Transactions")]
            QUARANTINE[("Quarantine Delta<br/>Rejected Transactions")]

            WINDOW["10-Min Sliding Window<br/>2-Min Slide"]
            WINDOW_DELTA[("User Window Features")]

            STATE["applyInPandasWithState<br/>Per-User Historical State"]
            STATE_DELTA[("Stateful Transaction Features")]

            FEATURE["Hybrid Feature Assembly<br/>+ Rule Engine"]

            FEATURE_EVENTS[("Gold Immutable<br/>Fraud Feature Events")]

            KAFKA --> BRONZE
            BRONZE --> BRONZE_DELTA

            BRONZE_DELTA --> QUALITY
            QUALITY --> DEDUPE
            QUALITY --> QUARANTINE

            DEDUPE --> SILVER_DELTA

            SILVER_DELTA --> WINDOW
            SILVER_DELTA --> STATE

            WINDOW --> WINDOW_DELTA
            STATE --> STATE_DELTA

            WINDOW_DELTA --> FEATURE
            STATE_DELTA --> FEATURE

            FEATURE --> FEATURE_EVENTS
        end


        %% ====================================================
        %% MACHINE LEARNING
        %% ====================================================

        subgraph ML["3. Offline Training + Online Inference"]

            LABELS[("Separate Ground-Truth Labels")]
            TRAINING[("ML Training Dataset")]
            RFTRAIN["Random Forest Training<br/>Temporal Holdout"]
            MODEL[("Saved Spark ML PipelineModel")]

            INFERENCE["Distributed ML Inference"]
            HYBRID["Hybrid Fraud Decision Engine"]

            SCORED[("Gold Scored Transactions")]
            ALERTS[("Gold Fraud Alerts")]

            LABELS --> TRAINING
            FEATURE_EVENTS -. feature schema .-> TRAINING

            TRAINING --> RFTRAIN
            RFTRAIN --> MODEL

            FEATURE_EVENTS --> INFERENCE
            MODEL --> INFERENCE

            INFERENCE --> HYBRID

            HYBRID --> SCORED
            HYBRID --> ALERTS
        end


        %% ====================================================
        %% RELIABILITY
        %% ====================================================

        subgraph RELIABILITY["4. Reliability"]

            CHECKPOINTS[("Spark Checkpoints")]
            DELTA_TXN["Delta txnAppId / txnVersion<br/>Idempotent Writes"]
            DOCKER["Docker Restart Policies"]

        end

        CHECKPOINTS -. recovery .-> BRONZE
        CHECKPOINTS -. recovery .-> DEDUPE
        CHECKPOINTS -. recovery .-> WINDOW
        CHECKPOINTS -. recovery .-> STATE
        CHECKPOINTS -. recovery .-> FEATURE
        CHECKPOINTS -. recovery .-> INFERENCE

        DELTA_TXN -. retry protection .-> FEATURE_EVENTS
        DELTA_TXN -. retry protection .-> SCORED
        DELTA_TXN -. retry protection .-> ALERTS

        DOCKER -. process restart .-> BRONZE
        DOCKER -. process restart .-> STATE
        DOCKER -. process restart .-> INFERENCE


        %% ====================================================
        %% OBSERVABILITY
        %% ====================================================

        subgraph OBSERVABILITY["5. Observability"]

            LISTENER["StreamingQueryListener"]
            METRICS[("JSONL Streaming Metrics")]

            EVENTS[("Spark Event Logs")]
            HISTORY["Spark History Server<br/>Port 18080"]

            SNAPSHOT["Monitoring Snapshot"]
            DASHBOARD["Fraud Command Center<br/>Streamlit :8501"]

            LISTENER --> METRICS
            EVENTS --> HISTORY

            SCORED --> SNAPSHOT
            ALERTS --> SNAPSHOT
            SNAPSHOT --> DASHBOARD
        end
    ```

    ---

    ## Streaming Data Flow

    ### 1. Kafka ingestion

    The transaction generator creates synthetic financial
    transactions containing:

    - transaction ID,
    - user ID,
    - event timestamp,
    - amount,
    - merchant category,
    - device ID.

    Fraud labels remain outside Kafka. This prevents the live
    pipeline from receiving information it would not possess in
    a real production system.

    Kafka records are keyed by `user_id`, improving locality for
    user-oriented downstream processing.

    ---

    ### 2. Bronze layer

    The Bronze stream preserves the original transaction together
    with Kafka lineage:

    - topic,
    - partition,
    - offset,
    - Kafka timestamp,
    - headers,
    - raw JSON,
    - ingestion timestamp.

    Bronze therefore provides replayability and traceability.

    ---

    ### 3. Silver quality layer

    Transactions are validated for conditions including:

    - malformed JSON,
    - missing identifiers,
    - missing timestamps,
    - invalid amounts,
    - excessive amounts,
    - missing merchant or device information,
    - timestamps too far in the future.

    Valid records continue to Silver.

    Invalid records are written to a separate quarantine Delta
    table with explicit rejection reasons.

    A five-minute event-time watermark supports bounded
    transaction-ID deduplication.

    ---

    ### 4. Sliding-window features

    Silver transactions feed a stateful Structured Streaming
    aggregation using:

    - 10-minute windows,
    - 2-minute slide,
    - 5-minute watermark.

    Features include:

    - transaction count,
    - total spending,
    - average amount,
    - maximum and minimum amount,
    - amount standard deviation,
    - transaction velocity,
    - device diversity,
    - merchant diversity.

    Because this is a sliding window, one transaction can
    participate in multiple overlapping windows.

    ---

    ### 5. Arbitrary per-user state

    A separate processing path uses:

    `applyInPandasWithState`

    to maintain long-lived behavior for every user.

    State includes:

    - historical transaction count,
    - incremental mean,
    - incremental variance state,
    - previous event time,
    - previous device,
    - device-change count.

    Welford's online algorithm allows mean and sample standard
    deviation to be updated without storing each user's entire
    transaction history.

    Historical anomaly detection compares each new transaction
    against state **before** that transaction updates the state.

    ---

    ### 6. Hybrid feature assembly

    Transaction-level historical features are enriched with the
    applicable sliding-window features.

    The rule engine evaluates signals such as:

    - historical amount deviation,
    - statistical z-score,
    - high transaction velocity,
    - multiple-device activity,
    - changed-device plus amount anomaly,
    - merchant diversity,
    - rapid repeat activity.

    The resulting feature event is written to an immutable Gold
    Delta table suitable for downstream serving.

    ---

    ### 7. Machine learning

    Training labels are stored separately from the live Kafka
    payload.

    Spark MLlib trains a Random Forest pipeline using:

    - class weighting,
    - median imputation,
    - categorical indexing,
    - one-hot encoding,
    - vector assembly,
    - temporal train/test separation.

    The full `PipelineModel` is persisted and later loaded once
    by the live scoring application.

    ---

    ### 8. Final fraud decision

    The final decision combines:

    - Random Forest probability,
    - rule score,
    - rule-based anomaly status.

    Possible decision paths include:

    - high-confidence ML detection,
    - rule + ML confirmation,
    - exceptionally high rule score.

    The serving layer produces:

    - all scored transactions,
    - fraud alerts,
    - final severity,
    - final decision basis,
    - human-readable fraud reasons.

    ---

    ## Reliability Architecture

    Reliability is handled at several layers:

    1. Kafka offsets provide replayable ingestion.
    2. Spark checkpoints preserve streaming progress.
    3. Stateful checkpoints preserve operator state.
    4. Delta Lake provides ACID table commits.
    5. `txnAppId` and `txnVersion` protect retry-sensitive writes.
    6. Docker restart policies restart failed applications.
    7. Integration tests verify checkpoint continuation.

    ---

    ## Observability Architecture

    Two different levels of Spark observability are retained.

    ### Streaming-level metrics

    `StreamingQueryListener` records:

    - input rows,
    - processing rate,
    - trigger duration,
    - query-planning duration,
    - state rows,
    - state memory,
    - rows dropped by watermark,
    - source offsets.

    ### Spark execution metrics

    Spark event logs are persisted and exposed through the Spark
    History Server.

    This supports post-run inspection of:

    - jobs,
    - stages,
    - tasks,
    - executors,
    - shuffle reads,
    - shuffle writes,
    - SQL plans.

    ---

    ## Presentation Layer

    The recruiter-facing Streamlit dashboard exposes:

    - transaction volume,
    - alert volume,
    - fraud rate,
    - severity distribution,
    - ML probabilities,
    - decision basis,
    - high-risk users,
    - recent alerts,
    - event-to-score latency,
    - feature coverage.

    The dashboard does not perform fraud scoring itself.

    It reads committed Gold Delta snapshots produced by the
    streaming platform.
    """
).strip() + "\n"


# ============================================================
# ENGINEERING DECISIONS
# ============================================================

ENGINEERING_DECISIONS_DOCUMENT = dedent(
    """
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
    """
).strip() + "\n"


# ============================================================
# RELIABILITY
# ============================================================

RELIABILITY_DOCUMENT = dedent(
    """
    # Reliability and Recovery

    ## Failure Model

    The platform is designed to tolerate application-process
    failure without silently restarting the pipeline from the
    beginning.

    ---

    ## Spark Checkpoints

    Every long-running streaming query uses its own checkpoint.

    Checkpoints preserve information such as:

    - source offsets,
    - committed micro-batches,
    - query metadata,
    - state-store data for stateful operators.

    Checkpoints are runtime artifacts and are intentionally not
    committed to Git.

    ---

    ## Stateful Recovery

    `applyInPandasWithState` maintains per-user history.

    Restarting that application with the same compatible
    checkpoint restores the state rather than rebuilding every
    user's history from zero.

    Because checkpoint metadata is tied to the stateful query,
    state schema and query semantics must not be changed
    casually while reusing the same checkpoint.

    ---

    ## Crash-Recovery Test

    Recovery testing intentionally stops the Spark environment
    while Kafka remains available.

    Additional events are produced while Spark is unavailable.

    After Spark restarts using the same checkpoint:

    - previously processed events are not reprocessed as new
      logical transactions,
    - the application resumes from stored progress,
    - newly queued Kafka events are consumed.

    An integration test also performs deterministic file-source
    checkpoint continuation without requiring Kafka.

    ---

    ## Idempotent Gold Writes

    Some `foreachBatch` sinks use:

    - `txnAppId`,
    - `txnVersion`.

    The Spark batch ID becomes the Delta transaction version.

    Repeating a write with the same application ID and batch
    version therefore does not create an additional logical
    commit.

    This is tested independently in the integration suite.

    ---

    ## Docker Recovery

    Streaming containers use:

    `restart: unless-stopped`

    Docker therefore handles process restart while Spark
    checkpoints handle processing recovery.

    These solve different problems:

    - Docker restores the process.
    - Spark restores streaming progress and state.

    ---

    ## Quarantine

    Invalid transactions are not silently discarded.

    They are written to a dedicated quarantine Delta table with
    rejection reasons, enabling investigation of data-quality
    failures.

    ---

    ## Tests

    The repository includes unit and integration coverage for:

    - Welford statistics,
    - transaction quality rules,
    - final fraud decisions,
    - checkpoint recovery,
    - Delta idempotency.
    """
).strip() + "\n"


# ============================================================
# DATA CONTRACT
# ============================================================

DATA_CONTRACT_DOCUMENT = dedent(
    """
    # Data Contract

    ## Kafka Transaction Event

    The production event intentionally contains only fields that
    would reasonably exist at transaction-processing time.

    | Field | Type | Description |
    |---|---|---|
    | `transaction_id` | string | Unique transaction identifier |
    | `user_id` | string | Customer/user identifier |
    | `timestamp` | timestamp | Event-time transaction timestamp |
    | `amount` | double | Transaction amount |
    | `merchant_category` | string | Merchant category |
    | `device_id` | string | Device used for the transaction |

    Simulator labels such as `is_fraud_simulated` and
    `fraud_type` do **not** enter Kafka.

    ---

    ## Major Final Scoring Fields

    The final scored Gold dataset contains the original
    transaction plus engineered behavioral features and final
    fraud decisions.

    Important fields include:

    | Field | Meaning |
    |---|---|
    | `transaction_id` | Transaction identifier |
    | `user_id` | User identifier |
    | `event_timestamp` | Event-time timestamp |
    | `amount` | Transaction amount |
    | `merchant_category` | Merchant category |
    | `device_id` | Current device |
    | `historical_avg_amount` | User historical average before current transaction |
    | `amount_deviation_ratio` | Amount relative to historical baseline |
    | `amount_zscore` | Statistical deviation from historical behavior |
    | `transactions_last_10m` | Recent transaction frequency |
    | `spend_last_10m` | Recent total spending |
    | `transactions_per_minute` | Recent velocity |
    | `unique_devices_10m` | Recent device diversity |
    | `new_device_flag` | New or changed device indicator |
    | `rule_anomaly_flag` | Rule-engine anomaly signal |
    | `fraud_rule_score` | Weighted rule score |
    | `ml_prediction` | Spark ML class prediction |
    | `ml_fraud_probability` | Random Forest fraud probability |
    | `final_fraud_flag` | Final hybrid fraud outcome |
    | `final_fraud_severity` | LOW, MEDIUM, HIGH or CRITICAL |
    | `final_decision_basis` | Primary final decision path |
    | `fraud_reasons` | Human-readable active fraud signals |
    | `window_feature_match_found` | Whether applicable window features were found |
    | `processing_timestamp` | Final processing timestamp |
    | `event_date` | Date partition used by Gold tables |

    ---

    ## Ground-Truth Label Contract

    Training labels are maintained separately.

    Important fields include:

    - `transaction_id`,
    - `user_id`,
    - `event_timestamp`,
    - `is_fraud_label`,
    - `fraud_type`,
    - simulator duplicate metadata,
    - simulator lateness metadata.

    The training dataset is constructed by joining these labels
    to engineered features using `transaction_id`.
    """
).strip() + "\n"


# ============================================================
# DEMO GUIDE
# ============================================================

DEMO_GUIDE_DOCUMENT = dedent(
    r"""
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
    """
).strip() + "\n"


# ============================================================
# WRITE DOCUMENT
# ============================================================

def write_document(
    filename: str,
    contents: str,
) -> None:

    path = (
        DOCS_DIRECTORY
        / filename
    )

    path.write_text(
        contents,
        encoding="utf-8",
    )

    print(
        f"Created: {path}"
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    DOCS_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    SCREENSHOT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_document(
        "architecture.md",
        ARCHITECTURE_DOCUMENT,
    )

    write_document(
        "engineering_decisions.md",
        ENGINEERING_DECISIONS_DOCUMENT,
    )

    write_document(
        "reliability.md",
        RELIABILITY_DOCUMENT,
    )

    write_document(
        "data_contract.md",
        DATA_CONTRACT_DOCUMENT,
    )

    write_document(
        "demo_guide.md",
        DEMO_GUIDE_DOCUMENT,
    )

    gitkeep_path = (
        SCREENSHOT_DIRECTORY
        / ".gitkeep"
    )

    gitkeep_path.touch(
        exist_ok=True
    )

    print()
    print("=" * 78)
    print(
        "PROJECT DOCUMENTATION GENERATED"
    )
    print("=" * 78)


if __name__ == "__main__":
    main()