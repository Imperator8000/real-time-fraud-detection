from __future__ import annotations

import argparse
import json
import time

from datetime import datetime, timezone
from pathlib import Path

from src.producer.kafka_transaction_producer import (
    FraudKafkaProducer,
    print_final_summary,
    print_periodic_metrics,
)

from src.producer.transaction_generator import (
    GeneratedTransaction,
    TransactionGenerator,
)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


DEFAULT_LABEL_FILE = (
    PROJECT_ROOT
    / "data"
    / "training"
    / "ground_truth"
    / "transaction_labels.jsonl"
)


# ============================================================
# GROUND-TRUTH SERIALIZATION
# ============================================================

def build_ground_truth_record(
    event: GeneratedTransaction,
) -> dict:
    """
    Create simulator ground truth.

    IMPORTANT:
    These fields never enter the production Kafka payload.
    """

    return {
        "transaction_id": (
            event.transaction_id
        ),

        "user_id": (
            event.user_id
        ),

        "event_timestamp": (
            event.to_payload()[
                "timestamp"
            ]
        ),

        "is_fraud_label": int(
            event.is_fraud_simulated
        ),

        "fraud_type": (
            event.fraud_type
        ),

        "simulator_is_duplicate": bool(
            event.is_duplicate
        ),

        "simulator_lateness_seconds": int(
            event.lateness_seconds
        ),

        "label_written_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }


# ============================================================
# TRAINING WORKLOAD
# ============================================================

def run_training_workload(
    *,
    rate: float,
    duration_seconds: float,
    number_of_users: int,
    fraud_rate: float,
    seed: int,
    label_file: Path,
) -> None:

    generator = (
        TransactionGenerator(
            number_of_users=(
                number_of_users
            ),
            fraud_rate=(
                fraud_rate
            ),
            seed=seed,
        )
    )

    producer = (
        FraudKafkaProducer(
            bootstrap_servers=(
                "localhost:9092"
            ),
            topic=(
                "transactions"
            ),
        )
    )

    label_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # A training run starts a new ground-truth file rather than
    # accidentally mixing labels from unrelated experiments.
    label_handle = (
        label_file.open(
            "w",
            encoding="utf-8",
        )
    )

    labels_written = 0

    start_time = (
        time.perf_counter()
    )

    last_report_time = (
        start_time
    )

    interval = (
        1.0 / rate
    )

    next_event_time = (
        start_time
    )

    print("=" * 78)
    print(
        "REAL-TIME FRAUD DETECTION"
    )
    print(
        "LABELED ML TRAINING WORKLOAD"
    )
    print("=" * 78)

    print(
        f"Target rate:       "
        f"{rate:,.1f} events/sec"
    )

    print(
        f"Duration:          "
        f"{duration_seconds:,.0f} seconds"
    )

    print(
        f"Users:             "
        f"{number_of_users:,}"
    )

    print(
        f"Fraud probability: "
        f"{fraud_rate:.2%}"
    )

    print(
        f"Ground truth:      "
        f"{label_file}"
    )

    print()

    try:

        while True:

            now = (
                time.perf_counter()
            )

            elapsed = (
                now
                - start_time
            )

            if (
                elapsed
                >= duration_seconds
            ):
                break

            if now < next_event_time:

                time.sleep(
                    min(
                        next_event_time
                        - now,
                        0.01,
                    )
                )

                continue

            # ------------------------------------------------
            # GENERATE ONE EVENT
            # ------------------------------------------------

            event = (
                generator.generate_event()
            )

            # ------------------------------------------------
            # LIVE PATH
            # ------------------------------------------------
            #
            # FraudKafkaProducer.send() uses event.to_payload().
            #
            # Therefore the simulator label is NOT placed in
            # Kafka.
            # ------------------------------------------------

            producer.send(
                event
            )

            # ------------------------------------------------
            # TRAINING-ONLY PATH
            # ------------------------------------------------

            ground_truth = (
                build_ground_truth_record(
                    event
                )
            )

            label_handle.write(
                json.dumps(
                    ground_truth,
                    separators=(
                        ",",
                        ":",
                    ),
                )
                + "\n"
            )

            labels_written += 1

            # Avoid flushing to disk for every individual event.
            if (
                labels_written
                % 1000
                == 0
            ):

                label_handle.flush()

            next_event_time += (
                interval
            )

            # ------------------------------------------------
            # CONSOLE METRICS
            # ------------------------------------------------

            now = (
                time.perf_counter()
            )

            if (
                now
                - last_report_time
                >= 5.0
            ):

                print_periodic_metrics(
                    kafka_producer=(
                        producer
                    ),
                    elapsed_seconds=(
                        now
                        - start_time
                    ),
                )

                last_report_time = (
                    now
                )

    except KeyboardInterrupt:

        print()
        print(
            "Training workload interrupted."
        )

    finally:

        label_handle.flush()
        label_handle.close()

        elapsed = (
            time.perf_counter()
            - start_time
        )

        print()
        print(
            "Flushing Kafka messages..."
        )

        undelivered = (
            producer.close()
        )

        print_final_summary(
            kafka_producer=producer,
            elapsed_seconds=elapsed,
            undelivered_messages=(
                undelivered
            ),
        )

        print()
        print(
            f"Ground-truth labels written: "
            f"{labels_written:,}"
        )

        print(
            f"Ground-truth file: "
            f"{label_file}"
        )

        # The sidecar file is valid for training only when the
        # corresponding Kafka deliveries completed successfully.
        if (
            undelivered > 0
            or producer.metrics.delivery_failures
            > 0
        ):

            raise SystemExit(
                "Training workload is not valid because "
                "one or more Kafka events were not delivered."
            )


# ============================================================
# CLI
# ============================================================

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Generate labeled training traffic while keeping "
            "fraud labels out of Kafka."
        )
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=75.0,
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=120.0,
    )

    parser.add_argument(
        "--users",
        type=int,
        default=200,
    )

    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.05,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=2026,
    )

    parser.add_argument(
        "--label-file",
        type=Path,
        default=(
            DEFAULT_LABEL_FILE
        ),
    )

    arguments = (
        parser.parse_args()
    )

    if arguments.rate <= 0:
        parser.error(
            "--rate must be greater than zero."
        )

    if arguments.duration <= 0:
        parser.error(
            "--duration must be greater than zero."
        )

    return arguments


def main() -> None:

    arguments = (
        parse_arguments()
    )

    run_training_workload(
        rate=arguments.rate,
        duration_seconds=(
            arguments.duration
        ),
        number_of_users=(
            arguments.users
        ),
        fraud_rate=(
            arguments.fraud_rate
        ),
        seed=(
            arguments.seed
        ),
        label_file=(
            arguments.label_file
        ),
    )


if __name__ == "__main__":
    main()