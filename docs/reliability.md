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
