from __future__ import annotations

import argparse
import time

from pathlib import Path


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)


# ============================================================
# PATH RESOLUTION
# ============================================================

def resolve_path(
    raw_path: str,
) -> Path:

    path = Path(
        raw_path
    )

    if path.is_absolute():
        return path

    return (
        PROJECT_ROOT
        / path
    )


# ============================================================
# WAIT
# ============================================================

def wait_for_paths(
    paths: list[Path],
    *,
    interval_seconds: float,
    timeout_seconds: float,
) -> None:

    started_at = (
        time.monotonic()
    )

    previously_missing: set[
        Path
    ] = set()

    while True:

        missing = {
            path
            for path in paths
            if not path.exists()
        }

        if not missing:

            print()
            print(
                "All required dependencies are ready."
            )

            for path in paths:

                print(
                    f"  READY: {path}"
                )

            return

        if (
            missing
            != previously_missing
        ):

            print()
            print(
                "Waiting for dependencies:"
            )

            for path in sorted(
                missing,
                key=str,
            ):

                print(
                    f"  WAITING: {path}"
                )

            previously_missing = (
                missing
            )

        elapsed = (
            time.monotonic()
            - started_at
        )

        if (
            timeout_seconds > 0
            and elapsed
            >= timeout_seconds
        ):

            missing_text = "\n".join(
                f"  - {path}"
                for path in sorted(
                    missing,
                    key=str,
                )
            )

            raise TimeoutError(
                "Timed out waiting for:\n"
                f"{missing_text}"
            )

        time.sleep(
            interval_seconds
        )


# ============================================================
# CLI
# ============================================================

def parse_arguments() -> argparse.Namespace:

    parser = (
        argparse.ArgumentParser(
            description=(
                "Wait until required project artifacts "
                "exist before starting a downstream service."
            )
        )
    )

    parser.add_argument(
        "--path",
        action="append",
        required=True,
        help=(
            "Required path. May be supplied more "
            "than once."
        ),
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help=(
            "Seconds between checks."
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help=(
            "Maximum wait in seconds. "
            "0 means wait indefinitely."
        ),
    )

    return (
        parser.parse_args()
    )


def main() -> None:

    arguments = (
        parse_arguments()
    )

    required_paths = [
        resolve_path(
            value
        )
        for value
        in arguments.path
    ]

    wait_for_paths(
        required_paths,
        interval_seconds=(
            arguments.interval
        ),
        timeout_seconds=(
            arguments.timeout
        ),
    )


if __name__ == "__main__":
    main()