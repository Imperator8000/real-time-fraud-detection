from __future__ import annotations

import json
import os
import sys

from pathlib import Path

from pyspark.sql import functions as F


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.common.spark_session import (
    get_spark_session,
)


# ============================================================
# PATHS
# ============================================================

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "transactions"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "benchmarks"
    / "streaming_state"
    / "user_skew_profile.json"
)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    threshold = float(
        os.getenv(
            "HOT_USER_SHARE_THRESHOLD",
            "0.20",
        )
    )

    spark = (
        get_spark_session(
            app_name=(
                "FraudDetection-"
                "UserSkewProfiler"
            )
        )
    )

    try:

        df = (
            spark.read
            .format(
                "delta"
            )
            .load(
                str(
                    SILVER_PATH
                )
            )
        )

        total_rows = (
            df.count()
        )

        if total_rows == 0:

            raise RuntimeError(
                "Silver table contains no transactions."
            )

        user_counts = (
            df
            .groupBy(
                "user_id"
            )
            .count()
        )

        top_users = (
            user_counts

            .withColumn(
                "traffic_share",

                F.col(
                    "count"
                )
                /
                F.lit(
                    float(
                        total_rows
                    )
                ),
            )

            .orderBy(
                F.desc(
                    "count"
                )
            )

            .limit(
                20
            )

            .collect()
        )

        user_count = (
            user_counts.count()
        )

        hottest_user = (
            top_users[
                0
            ]
        )

        hottest_share = float(
            hottest_user[
                "traffic_share"
            ]
        )

        hot_users = [
            {
                "user_id": (
                    row[
                        "user_id"
                    ]
                ),

                "transaction_count": int(
                    row[
                        "count"
                    ]
                ),

                "traffic_share": float(
                    row[
                        "traffic_share"
                    ]
                ),
            }

            for row in top_users

            if float(
                row[
                    "traffic_share"
                ]
            )
            >= threshold
        ]

        result = {
            "total_transactions": (
                total_rows
            ),

            "distinct_users": (
                user_count
            ),

            "hot_user_threshold": (
                threshold
            ),

            "hottest_user": (
                hottest_user[
                    "user_id"
                ]
            ),

            "hottest_user_transactions": int(
                hottest_user[
                    "count"
                ]
            ),

            "hottest_user_share": (
                hottest_share
            ),

            "hot_users_detected": (
                hot_users
            ),

            "hot_user_detected": (
                len(
                    hot_users
                )
                > 0
            ),
        }

        OUTPUT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with OUTPUT_PATH.open(
            "w",
            encoding="utf-8",
        ) as handle:

            json.dump(
                result,
                handle,
                indent=2,
            )

        print("=" * 78)
        print(
            "USER KEY SKEW PROFILE"
        )
        print("=" * 78)

        print(
            f"Transactions:      "
            f"{total_rows:,}"
        )

        print(
            f"Distinct users:    "
            f"{user_count:,}"
        )

        print(
            f"Hottest user:      "
            f"{hottest_user['user_id']}"
        )

        print(
            f"Hottest share:     "
            f"{hottest_share:.2%}"
        )

        print(
            f"Hot threshold:     "
            f"{threshold:.2%}"
        )

        if hot_users:

            print()
            print(
                "HOT-KEY CONDITION DETECTED"
            )

            for user in hot_users:

                print(
                    f"  {user['user_id']}: "
                    f"{user['traffic_share']:.2%}"
                )

            print()
            print(
                "Do not salt the user_id state key. "
                "Per-user historical state must remain "
                "logically indivisible."
            )

        else:

            print()
            print(
                "No user exceeds the configured "
                "hot-key threshold."
            )

        print()
        print(
            f"Saved: {OUTPUT_PATH}"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()