from __future__ import annotations

import re
import subprocess
import sys

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
# REQUIRED PORTFOLIO FILES
# ============================================================

REQUIRED_FILES = [
    "README.md",
    "LICENSE",
    ".gitignore",
    ".dockerignore",
    ".gitattributes",
    ".env.example",

    "docker/docker-compose.yml",

    "docs/architecture.md",
    "docs/data_contract.md",
    "docs/demo_guide.md",
    "docs/engineering_decisions.md",
    "docs/reliability.md",

    "scripts/generate_project_docs.py",
    "scripts/generate_portfolio_readme.py",

    "src/common/spark_session.py",

    "src/streaming/bronze_kafka_stream.py",
    "src/streaming/silver_transaction_stream.py",
    "src/streaming/window_feature_stream.py",
    "src/streaming/stateful_anomaly_stream.py",
    "src/streaming/live_feature_event_stream.py",
    "src/streaming/final_fraud_scoring_stream.py",

    "src/ml/train_random_forest.py",

    "src/dashboard/app.py",

    "tests/unit/test_welford.py",
    "tests/unit/test_transaction_quality.py",
    "tests/unit/test_final_fraud_decision.py",

    "tests/integration/test_checkpoint_recovery.py",
    "tests/integration/test_delta_idempotency.py",
]


# ============================================================
# PATHS THAT SHOULD NEVER BE TRACKED
# ============================================================

FORBIDDEN_TRACKED_PREFIXES = [
    ".env",
    ".venv/",
    "checkpoints/",
    "logs/",
    "models/",

    "data/bronze/",
    "data/silver/",
    "data/gold/",
    "data/quarantine/",
    "data/training/",
    "data/monitoring/",
    "data/benchmarks/",
]


# ============================================================
# SECRET PATTERNS
# ============================================================

SECRET_PATTERNS = {
    "OpenAI-style API key": re.compile(
        r"\bsk-[A-Za-z0-9_-]{20,}\b"
    ),

    "AWS access key": re.compile(
        r"\bAKIA[0-9A-Z]{16}\b"
    ),

    "Private key": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
    ),

    "Hard-coded password": re.compile(
        r"""(?ix)
        \b
        password
        \s*
        =
        \s*
        ["']?
        [^"' \n\r#]{8,}
        """
    ),
}


# ============================================================
# RESULT STORAGE
# ============================================================

passes: list[str] = []
warnings: list[str] = []
failures: list[str] = []


# ============================================================
# HELPERS
# ============================================================

def run_git(
    *arguments: str,
) -> str:

    result = subprocess.run(
        [
            "git",
            *arguments,
        ],

        cwd=PROJECT_ROOT,

        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,

        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:

        raise RuntimeError(
            result.stderr.strip()
            or
            "Git command failed."
        )

    return (
        result.stdout.strip()
    )


def normalize_git_path(
    value: str,
) -> str:

    return (
        value
        .replace(
            "\\",
            "/",
        )
        .strip()
    )


# ============================================================
# FORBIDDEN PATH CHECK
# ============================================================

def is_forbidden_tracked_path(
    path: str,
) -> bool:

    normalized = (
        normalize_git_path(
            path
        )
    )

    for forbidden_path in (
        FORBIDDEN_TRACKED_PREFIXES
    ):

        # ----------------------------------------------------
        # DIRECTORY RULE
        # ----------------------------------------------------
        #
        # Entries ending in "/" represent directories.
        #
        # Example:
        #
        # data/gold/
        #
        # matches:
        #
        # data/gold/scored_transactions/...
        #
        # ----------------------------------------------------

        if forbidden_path.endswith(
            "/"
        ):

            if normalized.startswith(
                forbidden_path
            ):

                return True

        # ----------------------------------------------------
        # EXACT FILE RULE
        # ----------------------------------------------------
        #
        # Entries without "/" represent exact filenames.
        #
        # This is important for:
        #
        # .env          -> BLOCKED
        # .env.example  -> ALLOWED
        #
        # The previous implementation incorrectly used:
        #
        # ".env.example".startswith(".env")
        #
        # which returned True and caused .env.example to be
        # incorrectly treated as secret/runtime data.
        #
        # ----------------------------------------------------

        else:

            if normalized == (
                forbidden_path
            ):

                return True

    return False


def is_probably_text_file(
    path: Path,
) -> bool:

    allowed_suffixes = {
        ".py",
        ".md",
        ".txt",
        ".json",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
        ".env",
        ".example",
        ".ps1",
        ".sh",
        ".dockerfile",
    }

    if (
        path.name
        in {
            "Dockerfile",
            ".gitignore",
            ".dockerignore",
            ".gitattributes",
        }
    ):

        return True

    return (
        path.suffix.lower()
        in allowed_suffixes
    )


# ============================================================
# REQUIRED FILE CHECK
# ============================================================

def check_required_files() -> None:

    missing = []

    for relative_path in (
        REQUIRED_FILES
    ):

        path = (
            PROJECT_ROOT
            / relative_path
        )

        if not path.exists():

            missing.append(
                relative_path
            )

    if missing:

        failures.append(
            "Missing required files:\n"
            +
            "\n".join(
                f"  - {path}"
                for path
                in missing
            )
        )

    else:

        passes.append(
            "All required portfolio files are present."
        )


# ============================================================
# GIT CHECK
# ============================================================

def check_git_repository() -> list[str]:

    try:

        tracked_text = (
            run_git(
                "ls-files"
            )
        )

    except Exception as exc:

        failures.append(
            "Git repository check failed: "
            f"{exc}"
        )

        return []

    tracked_files = [
        normalize_git_path(
            line
        )

        for line in (
            tracked_text.splitlines()
        )

        if line.strip()
    ]

    passes.append(
        f"Git repository detected with "
        f"{len(tracked_files):,} tracked files."
    )

    forbidden = [
        path
        for path
        in tracked_files
        if is_forbidden_tracked_path(
            path
        )
    ]

    if forbidden:

        failures.append(
            "Runtime or secret-sensitive paths "
            "are currently tracked by Git:\n"
            +
            "\n".join(
                f"  - {path}"
                for path
                in forbidden
            )
        )

    else:

        passes.append(
            "No forbidden runtime directories "
            "or secret-sensitive files are tracked by Git."
        )

    return tracked_files


# ============================================================
# .ENV CHECK
# ============================================================

def check_environment_files(
    tracked_files: list[str],
) -> None:

    # --------------------------------------------------------
    # PRIVATE .env MUST NOT BE TRACKED
    # --------------------------------------------------------

    if ".env" in tracked_files:

        failures.append(
            ".env is tracked by Git. "
            "Remove it from Git before publishing."
        )

    else:

        passes.append(
            ".env is not tracked by Git."
        )

    # --------------------------------------------------------
    # PUBLIC .env.example SHOULD EXIST
    # --------------------------------------------------------

    env_example = (
        PROJECT_ROOT
        / ".env.example"
    )

    if not env_example.exists():

        failures.append(
            ".env.example is missing."
        )

        return

    # --------------------------------------------------------
    # .env.example SHOULD BE TRACKED
    # --------------------------------------------------------

    if ".env.example" in tracked_files:

        passes.append(
            ".env.example is correctly tracked by Git."
        )

    else:

        warnings.append(
            ".env.example exists but is not tracked by Git. "
            "It should normally be included in the public "
            "repository as the configuration template."
        )

    # --------------------------------------------------------
    # CHECK TEMPLATE FOR POSSIBLE REAL SECRETS
    # --------------------------------------------------------

    text = (
        env_example.read_text(
            encoding="utf-8",
            errors="replace",
        )
    )

    suspicious_lines = []

    for line_number, line in enumerate(
        text.splitlines(),
        start=1,
    ):

        stripped = (
            line.strip()
        )

        if (
            not stripped
            or stripped.startswith(
                "#"
            )
            or "=" not in stripped
        ):

            continue

        key, value = (
            stripped.split(
                "=",
                1,
            )
        )

        key_upper = (
            key.strip().upper()
        )

        value = (
            value.strip()
        )

        secret_words = (
            "PASSWORD",
            "SECRET",
            "TOKEN",
            "API_KEY",
            "PRIVATE_KEY",
        )

        if (
            any(
                word in key_upper
                for word
                in secret_words
            )
            and value
            and value.lower()
            not in {
                "changeme",
                "example",
                "placeholder",
                "your_value_here",
            }
        ):

            suspicious_lines.append(
                (
                    line_number,
                    key.strip(),
                )
            )

    if suspicious_lines:

        warnings.append(
            ".env.example contains values in "
            "secret-looking variables. Review:\n"
            +
            "\n".join(
                f"  - line {line}: {key}"
                for line, key
                in suspicious_lines
            )
        )

    else:

        passes.append(
            ".env.example does not contain "
            "obvious populated secret variables."
        )


# ============================================================
# SECRET SCAN
# ============================================================

def scan_tracked_files_for_secrets(
    tracked_files: list[str],
) -> None:

    matches = []

    for relative_path in (
        tracked_files
    ):

        path = (
            PROJECT_ROOT
            / relative_path
        )

        if (
            not path.exists()
            or not path.is_file()
            or not is_probably_text_file(
                path
            )
        ):

            continue

        try:

            text = (
                path.read_text(
                    encoding="utf-8",
                    errors="replace",
                )
            )

        except Exception:

            continue

        for secret_name, pattern in (
            SECRET_PATTERNS.items()
        ):

            if pattern.search(
                text
            ):

                matches.append(
                    (
                        relative_path,
                        secret_name,
                    )
                )

    if matches:

        failures.append(
            "Potential secrets detected in tracked files:\n"
            +
            "\n".join(
                f"  - {path}: {kind}"
                for path, kind
                in matches
            )
        )

    else:

        passes.append(
            "No obvious secrets detected in tracked text files."
        )


# ============================================================
# README CHECK
# ============================================================

def check_readme() -> None:

    readme_path = (
        PROJECT_ROOT
        / "README.md"
    )

    if not readme_path.exists():

        return

    text = (
        readme_path.read_text(
            encoding="utf-8",
            errors="replace",
        )
    )

    expected_sections = [
        "# Real-Time Financial Fraud Detection Lakehouse",
        "## Architecture",
        "# Machine Learning",
        "# Performance Engineering",
        "# Reliability",
        "# Observability",
        "# Fraud Monitoring Dashboard",
        "# Docker Quick Start",
    ]

    missing = [
        section
        for section
        in expected_sections
        if section not in text
    ]

    if missing:

        warnings.append(
            "README is missing expected portfolio sections:\n"
            +
            "\n".join(
                f"  - {section}"
                for section
                in missing
            )
        )

    else:

        passes.append(
            "README contains the major recruiter-facing sections."
        )

    if (
        "Not available in generated metrics"
        in text
    ):

        warnings.append(
            "README still contains unavailable model metrics. "
            "This is not a release blocker, but regenerate the "
            "README after successful model training if possible."
        )


# ============================================================
# GIT STATUS
# ============================================================

def check_git_status() -> None:

    try:

        status = (
            run_git(
                "status",
                "--short",
            )
        )

    except Exception:

        return

    if status:

        warnings.append(
            "The working tree still contains "
            "uncommitted changes."
        )

    else:

        passes.append(
            "Git working tree is clean."
        )


# ============================================================
# REPORT
# ============================================================

def print_section(
    title: str,
    items: list[str],
    symbol: str,
) -> None:

    if not items:

        return

    print()
    print(
        title
    )
    print(
        "-" * 78
    )

    for item in items:

        print(
            f"{symbol} {item}"
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print(
        "=" * 78
    )

    print(
        "PORTFOLIO RELEASE CHECK"
    )

    print(
        "=" * 78
    )

    check_required_files()

    tracked_files = (
        check_git_repository()
    )

    check_environment_files(
        tracked_files
    )

    scan_tracked_files_for_secrets(
        tracked_files
    )

    check_readme()

    check_git_status()

    print_section(
        "PASSED",
        passes,
        "[PASS]",
    )

    print_section(
        "WARNINGS",
        warnings,
        "[WARN]",
    )

    print_section(
        "FAILURES",
        failures,
        "[FAIL]",
    )

    print()
    print(
        "=" * 78
    )

    if failures:

        print(
            "RELEASE CHECK FAILED"
        )

        print(
            "Resolve the failures before "
            "publishing this repository."
        )

        print(
            "=" * 78
        )

        sys.exit(
            1
        )

    print(
        "RELEASE CHECK PASSED"
    )

    if warnings:

        print(
            "There are non-blocking warnings to review."
        )

    else:

        print(
            "No release warnings detected."
        )

    print(
        "=" * 78
    )


if __name__ == "__main__":
    main()