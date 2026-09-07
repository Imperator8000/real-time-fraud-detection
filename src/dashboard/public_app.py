from __future__ import annotations

import os
import sys

from pathlib import Path


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

if str(
    PROJECT_ROOT
) not in sys.path:

    sys.path.insert(
        0,
        str(
            PROJECT_ROOT
        ),
    )


# ============================================================
# FORCE PUBLIC DEMO MODE
# ============================================================

os.environ[
    "DASHBOARD_PUBLIC_DEMO"
] = "true"


# ============================================================
# RUN STANDARD DASHBOARD
# ============================================================

import src.dashboard.app  # noqa: E402, F401