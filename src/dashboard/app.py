from __future__ import annotations

# ============================================================
# IMPORTS
# ============================================================

import os
import sys
import math

from pathlib import Path

import pandas as pd
import streamlit as st


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


# ============================================================
# PUBLIC DEMO MODE
# ============================================================

PUBLIC_DEMO_MODE = (
    os.getenv(
        "DASHBOARD_PUBLIC_DEMO",
        "false",
    )
    .strip()
    .lower()
    == "true"
)


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Fraud Detection Command Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# PAGE STYLING
# ============================================================

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 3rem;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.65rem;
        }

        .fraud-subtitle {
            opacity: 0.75;
            margin-top: -0.8rem;
            margin-bottom: 1.3rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(
    ttl=8,
    show_spinner=False,
)
def load_snapshot(
    public_demo_mode: bool,
) -> dict:
    """
    Load dashboard data.

    PUBLIC MODE:
        Uses deterministic synthetic serving data.
        Does not require Spark, Kafka, Delta Lake or Java.

    LOCAL MODE:
        Reads committed Gold Delta tables from the full
        fraud-detection pipeline.
    """

    # --------------------------------------------------------
    # PUBLIC RECRUITER DEMO
    # --------------------------------------------------------

    if public_demo_mode:

        from src.dashboard.demo_snapshot import (
            build_demo_snapshot,
        )

        snapshot = (
            build_demo_snapshot()
        )

        snapshot["summary"] = dict(
            snapshot["summary"]
        )

        snapshot["summary"]["demo_mode"] = True

        snapshot["summary"]["data_source"] = (
            "synthetic_portfolio_replay"
        )

        return snapshot

    # --------------------------------------------------------
    # LOCAL FULL-PIPELINE MODE
    # --------------------------------------------------------
    #
    # Import lazily so Streamlit Community Cloud does not need
    # the Delta Lake Python package or local Gold tables.
    # --------------------------------------------------------

    from src.monitoring.dashboard_snapshot import (
        build_dashboard_snapshot,
    )

    snapshot = (
        build_dashboard_snapshot(
            persist=True
        )
    )

    snapshot["summary"] = dict(
        snapshot["summary"]
    )

    snapshot["summary"]["demo_mode"] = False

    snapshot["summary"]["data_source"] = (
        "live_delta"
    )

    return snapshot


# ============================================================
# FORMATTERS
# ============================================================

def safe_number(
    value,
) -> float | None:
    """
    Convert a numeric value safely.

    Missing, invalid and non-finite values return None rather
    than being displayed as fabricated zero measurements.
    """

    if value is None:
        return None

    try:

        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (
        TypeError,
        ValueError,
    ):

        return None


def format_count(
    value,
) -> str:

    number = safe_number(value)

    if number is None:
        return "N/A"

    return f"{int(number):,}"


def format_money(
    value,
) -> str:

    number = safe_number(value)

    if number is None:
        return "N/A"

    return f"${number:,.2f}"


def format_percentage(
    value,
    decimals: int = 2,
) -> str:
    """
    Expects a proportion such as 0.025, not 2.5.
    """

    number = safe_number(value)

    if number is None:
        return "N/A"

    return f"{number:.{decimals}%}"


def format_probability(
    value,
) -> str:

    number = safe_number(value)

    if number is None:
        return "N/A"

    return f"{number:.3f}"


def format_latency(
    milliseconds,
) -> str:

    number = safe_number(milliseconds)

    if number is None:
        return "N/A"

    if number < 1000:

        return f"{number:,.0f} ms"

    return f"{number / 1000.0:,.2f} s"


def get_dataframe(
    snapshot: dict,
    key: str,
) -> pd.DataFrame:
    """
    Return a copy so dashboard formatting never mutates the
    cached source DataFrame.
    """

    value = snapshot.get(key)

    if isinstance(value, pd.DataFrame):

        return value.copy()

    return pd.DataFrame()


def format_timestamp(
    value,
) -> str:

    if value is None:
        return "Not available"

    try:

        timestamp = pd.to_datetime(
            value,
            utc=True,
            errors="coerce",
        )

        if pd.isna(timestamp):
            return "Not available"

        return timestamp.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )

    except (
        TypeError,
        ValueError,
    ):

        return str(value)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title(
        "Fraud Platform"
    )

    st.caption(
        "Real-Time Financial Fraud "
        "Detection Lakehouse"
    )

    st.divider()

    if PUBLIC_DEMO_MODE:

        st.info(
            "Public synthetic replay",
            icon="🌐",
        )

    else:

        st.success(
            "Local Delta Lake mode",
            icon="✅",
        )

    st.markdown(
        """
        **Architecture**

        Kafka  
        → PySpark Structured Streaming  
        → Delta Lake  
        → Stateful Features  
        → Random Forest  
        → Hybrid Fraud Decision
        """
    )

    st.divider()

    st.caption(
        "Dashboard refresh interval: 10 seconds"
    )

    if st.button(
        "Refresh now",
        use_container_width=True,
    ):

        st.cache_data.clear()

        st.rerun()

    if not PUBLIC_DEMO_MODE:

        st.divider()

        st.link_button(
            "Open Spark History Server",
            "http://localhost:18080",
            use_container_width=True,
        )


# ============================================================
# HEADER
# ============================================================

st.title(
    "🛡️ Fraud Detection Command Center"
)

st.markdown(
    """
    <div class="fraud-subtitle">
        Monitoring transaction behavior, Spark-engineered
        features, Random Forest inference and hybrid fraud
        decisions.
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# AUTO-REFRESHING DASHBOARD
# ============================================================

@st.fragment(
    run_every="10s"
)
def render_dashboard():

    # ========================================================
    # LOAD DATA
    # ========================================================

    try:

        snapshot = (
            load_snapshot(
                PUBLIC_DEMO_MODE
            )
        )

    except Exception as exc:

        st.error(
            "The dashboard could not load its data."
        )

        if PUBLIC_DEMO_MODE:

            st.info(
                "The public synthetic replay could not be "
                "initialized. Please refresh the page."
            )

        else:

            st.info(
                "The dashboard is running, but the scored "
                "Gold Delta data may not be available yet."
            )

            st.code(
                "docker compose -f docker\\docker-compose.yml "
                "--profile demo up -d",
                language="powershell",
            )

            st.caption(
                f"{type(exc).__name__}: {exc}"
            )

        return


    # ========================================================
    # EXTRACT SNAPSHOT
    # ========================================================

    summary = dict(
        snapshot.get(
            "summary",
            {},
        )
    )

    severity_counts = (
        get_dataframe(
            snapshot,
            "severity_counts",
        )
    )

    decision_basis_counts = (
        get_dataframe(
            snapshot,
            "decision_basis_counts",
        )
    )

    hourly_metrics = (
        get_dataframe(
            snapshot,
            "hourly_metrics",
        )
    )

    top_alert_users = (
        get_dataframe(
            snapshot,
            "top_alert_users",
        )
    )

    recent_alerts = (
        get_dataframe(
            snapshot,
            "recent_alerts",
        )
    )


    # ========================================================
    # PUBLIC DEMO DISCLOSURE
    # ========================================================

    if summary.get(
        "demo_mode",
        False,
    ):

        st.info(
            "🌐 **Public Portfolio Demo** — This deployment "
            "uses a deterministic synthetic replay of "
            "fraud-scoring output so recruiters can explore "
            "the dashboard without provisioning Kafka and "
            "Spark. The complete real-time implementation is "
            "available in the GitHub repository and runs "
            "locally through Docker Compose."
        )


    # ========================================================
    # DATA FRESHNESS
    # ========================================================

    st.caption(
        "Snapshot refreshed: "
        + format_timestamp(
            summary.get(
                "snapshot_created_at_utc"
            )
        )
    )


    # ========================================================
    # PRIMARY KPI ROW
    # ========================================================

    (
        metric_1,
        metric_2,
        metric_3,
        metric_4,
        metric_5,
    ) = st.columns(
        5
    )

    metric_1.metric(
        "Transactions",
        format_count(
            summary.get(
                "transactions_processed"
            )
        ),
    )

    metric_2.metric(
        "Fraud Alerts",
        format_count(
            summary.get(
                "fraud_alerts"
            )
        ),
    )

    metric_3.metric(
        "Fraud Rate",
        format_percentage(
            summary.get(
                "fraud_rate"
            )
        ),
    )

    metric_4.metric(
        "Critical Alerts",
        format_count(
            summary.get(
                "critical_alerts"
            )
        ),
    )

    metric_5.metric(
        "Avg ML Probability",
        format_probability(
            summary.get(
                "average_ml_fraud_probability"
            )
        ),
    )


    # ========================================================
    # SECONDARY KPI ROW
    # ========================================================

    (
        metric_6,
        metric_7,
        metric_8,
        metric_9,
        metric_10,
    ) = st.columns(
        5
    )

    metric_6.metric(
        "Average Transaction",
        format_money(
            summary.get(
                "average_transaction_value"
            )
        ),
    )

    metric_7.metric(
        "P95 Transaction",
        format_money(
            summary.get(
                "p95_transaction_amount"
            )
        ),
    )

    metric_8.metric(
        "Window Coverage",
        format_percentage(
            summary.get(
                "window_feature_coverage"
            )
        ),
    )

    metric_9.metric(
        "Avg Event→Score",
        format_latency(
            summary.get(
                "average_event_to_score_latency_ms"
            )
        ),
    )

    metric_10.metric(
        "P95 Event→Score",
        format_latency(
            summary.get(
                "p95_event_to_score_latency_ms"
            )
        ),
    )


    st.divider()


    # ========================================================
    # DASHBOARD TABS
    # ========================================================

    (
        overview_tab,
        alert_tab,
        user_tab,
        recent_tab,
        engineering_tab,
    ) = st.tabs(
        [
            "Overview",
            "Alert Analysis",
            "User Risk",
            "Recent Alerts",
            "Engineering Health",
        ]
    )


    # ========================================================
    # TAB 1 — OVERVIEW
    # ========================================================

    with overview_tab:

        st.subheader(
            "Transaction & Fraud Trend"
        )

        if (
            not hourly_metrics.empty
            and {
                "hour",
                "transactions",
                "alerts",
            }.issubset(
                hourly_metrics.columns
            )
        ):

            hourly_chart = (
                hourly_metrics[
                    [
                        "hour",
                        "transactions",
                        "alerts",
                    ]
                ]
                .copy()
            )

            hourly_chart[
                "hour"
            ] = pd.to_datetime(
                hourly_chart[
                    "hour"
                ],
                utc=True,
                errors="coerce",
            )

            hourly_chart = (
                hourly_chart
                .dropna(
                    subset=[
                        "hour"
                    ]
                )
                .sort_values(
                    "hour"
                )
                .set_index(
                    "hour"
                )
            )

            st.line_chart(
                hourly_chart[
                    [
                        "transactions",
                        "alerts",
                    ]
                ],
                height=350,
            )

            st.caption(
                "Hourly transaction volume and final fraud "
                "alerts, grouped by transaction event time."
            )

        else:

            st.info(
                "No hourly trend data is available yet."
            )


        # ----------------------------------------------------
        # SEVERITY AND DECISION CHARTS
        # ----------------------------------------------------

        left, right = (
            st.columns(
                2
            )
        )

        with left:

            st.subheader(
                "Severity Distribution"
            )

            if (
                not severity_counts.empty
                and {
                    "severity",
                    "transaction_count",
                }.issubset(
                    severity_counts.columns
                )
            ):

                severity_chart = (
                    severity_counts
                    .set_index(
                        "severity"
                    )
                )

                st.bar_chart(
                    severity_chart[
                        [
                            "transaction_count"
                        ]
                    ],
                    height=330,
                )

                st.caption(
                    "Distribution of final risk severity "
                    "across scored transactions."
                )

            else:

                st.info(
                    "No severity data is available yet."
                )


        with right:

            st.subheader(
                "Fraud Decision Basis"
            )

            if (
                not decision_basis_counts.empty
                and {
                    "decision_basis",
                    "alert_count",
                }.issubset(
                    decision_basis_counts.columns
                )
            ):

                decision_chart = (
                    decision_basis_counts
                    .set_index(
                        "decision_basis"
                    )
                )

                st.bar_chart(
                    decision_chart[
                        [
                            "alert_count"
                        ]
                    ],
                    height=330,
                )

                st.caption(
                    "Primary decision paths for transactions "
                    "that received a final fraud flag."
                )

            else:

                st.info(
                    "No final fraud alerts are available yet."
                )


    # ========================================================
    # TAB 2 — ALERT ANALYSIS
    # ========================================================

    with alert_tab:

        st.subheader(
            "Rule vs ML Detection"
        )

        (
            rule_column,
            ml_column,
            hybrid_column,
            critical_rule_column,
        ) = st.columns(
            4
        )

        rule_column.metric(
            "Rule Anomalies",
            format_count(
                summary.get(
                    "rule_anomaly_count"
                )
            ),
        )

        ml_column.metric(
            "ML High Confidence",
            format_count(
                summary.get(
                    "ml_high_confidence_alerts"
                )
            ),
        )

        hybrid_column.metric(
            "Rule + ML",
            format_count(
                summary.get(
                    "hybrid_confirmation_alerts"
                )
            ),
        )

        critical_rule_column.metric(
            "Critical Rule Escalation",
            format_count(
                summary.get(
                    "critical_rule_alerts"
                )
            ),
        )

        st.markdown(
            """
            The final fraud decision engine combines
            deterministic behavioral rules with Random Forest
            probabilities. A transaction may be escalated
            through high-confidence ML detection, rule-plus-ML
            confirmation, or an exceptionally high rule score.
            """
        )

        st.caption(
            "Rule anomalies and final decision categories "
            "are different measures and should not be added "
            "together as independent alert totals."
        )


    # ========================================================
    # TAB 3 — USER RISK
    # ========================================================

    with user_tab:

        st.subheader(
            "Users With the Most Fraud Alerts"
        )

        if not top_alert_users.empty:

            display_users = (
                top_alert_users.copy()
            )

            # -----------------------------------------------
            # FORMAT NUMERIC COLUMNS
            # -----------------------------------------------

            for column in [
                "maximum_ml_probability",
                "total_alert_amount",
            ]:

                if column in display_users.columns:

                    display_users[
                        column
                    ] = pd.to_numeric(
                        display_users[
                            column
                        ],
                        errors="coerce",
                    ).round(
                        4
                        if column
                        ==
                        "maximum_ml_probability"
                        else 2
                    )

            column_config = {}

            if (
                "maximum_ml_probability"
                in display_users.columns
            ):

                column_config[
                    "maximum_ml_probability"
                ] = st.column_config.NumberColumn(
                    "Max ML Probability",
                    format="%.4f",
                )

            if (
                "total_alert_amount"
                in display_users.columns
            ):

                column_config[
                    "total_alert_amount"
                ] = st.column_config.NumberColumn(
                    "Total Alert Amount",
                    format="$%.2f",
                )

            st.dataframe(
                display_users,
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
            )

            st.caption(
                "Ranked by final fraud-alert count. "
                "User identifiers are synthetic in the "
                "public demonstration."
            )

        else:

            st.info(
                "No alerting users are available yet."
            )


    # ========================================================
    # TAB 4 — RECENT ALERTS
    # ========================================================

    with recent_tab:

        st.subheader(
            "Latest Fraud Alerts"
        )

        if not recent_alerts.empty:

            # ------------------------------------------------
            # SEVERITY FILTER
            # ------------------------------------------------

            severity_order = [
                "CRITICAL",
                "HIGH",
                "MEDIUM",
                "LOW",
            ]

            if (
                "final_fraud_severity"
                in recent_alerts.columns
            ):

                available_values = (
                    recent_alerts[
                        "final_fraud_severity"
                    ]
                    .dropna()
                    .astype(
                        str
                    )
                    .unique()
                    .tolist()
                )

                available_severities = [
                    "ALL",
                    *[
                        severity
                        for severity
                        in severity_order
                        if severity
                        in available_values
                    ],
                    *sorted(
                        severity
                        for severity
                        in available_values
                        if severity
                        not in severity_order
                    ),
                ]

            else:

                available_severities = [
                    "ALL"
                ]


            selected_severity = (
                st.selectbox(
                    "Filter by severity",
                    available_severities,
                    key=(
                        "recent_alert_"
                        "severity_filter"
                    ),
                )
            )

            if (
                selected_severity
                !=
                "ALL"
            ):

                recent_alerts = (
                    recent_alerts[
                        recent_alerts[
                            "final_fraud_severity"
                        ]
                        ==
                        selected_severity
                    ]
                    .copy()
                )


            # ------------------------------------------------
            # DISPLAY COLUMNS
            # ------------------------------------------------

            preferred_columns = [
                "processing_timestamp",
                "transaction_id",
                "user_id",
                "amount",
                "amount_deviation_ratio",
                "transactions_last_10m",
                "new_device_flag",
                "fraud_rule_score",
                "ml_fraud_probability",
                "final_fraud_severity",
                "final_decision_basis",
                "fraud_reasons",
            ]

            display_columns = [
                column
                for column
                in preferred_columns
                if column
                in recent_alerts.columns
            ]

            display_alerts = (
                recent_alerts[
                    display_columns
                ]
                .copy()
            )


            # ------------------------------------------------
            # FORMAT TIMESTAMPS
            # ------------------------------------------------

            if (
                "processing_timestamp"
                in display_alerts.columns
            ):

                display_alerts[
                    "processing_timestamp"
                ] = pd.to_datetime(
                    display_alerts[
                        "processing_timestamp"
                    ],
                    utc=True,
                    errors="coerce",
                )


            # ------------------------------------------------
            # FORMAT NUMERIC VALUES
            # ------------------------------------------------

            for column, decimals in [
                ("amount", 2),
                ("amount_deviation_ratio", 2),
                ("ml_fraud_probability", 4),
            ]:

                if column in display_alerts.columns:

                    display_alerts[
                        column
                    ] = pd.to_numeric(
                        display_alerts[
                            column
                        ],
                        errors="coerce",
                    ).round(
                        decimals
                    )


            # ------------------------------------------------
            # FORMAT FRAUD REASONS
            # ------------------------------------------------

            if (
                "fraud_reasons"
                in display_alerts.columns
            ):

                def format_reasons(
                    value,
                ) -> str:

                    if isinstance(
                        value,
                        (
                            list,
                            tuple,
                            set,
                        ),
                    ):

                        return ", ".join(
                            str(item)
                            for item
                            in value
                        )

                    if value is None:

                        return ""

                    try:

                        if pd.isna(value):

                            return ""

                    except (
                        TypeError,
                        ValueError,
                    ):

                        pass

                    return str(
                        value
                    )


                display_alerts[
                    "fraud_reasons"
                ] = (
                    display_alerts[
                        "fraud_reasons"
                    ]
                    .apply(
                        format_reasons
                    )
                )


            # ------------------------------------------------
            # TABLE CONFIGURATION
            # ------------------------------------------------

            column_config = {}

            if (
                "amount"
                in display_alerts.columns
            ):

                column_config[
                    "amount"
                ] = st.column_config.NumberColumn(
                    "Amount",
                    format="$%.2f",
                )

            if (
                "amount_deviation_ratio"
                in display_alerts.columns
            ):

                column_config[
                    "amount_deviation_ratio"
                ] = st.column_config.NumberColumn(
                    "Historical Ratio",
                    format="%.2f",
                )

            if (
                "ml_fraud_probability"
                in display_alerts.columns
            ):

                column_config[
                    "ml_fraud_probability"
                ] = st.column_config.NumberColumn(
                    "ML Probability",
                    format="%.4f",
                )

            st.dataframe(
                display_alerts,
                use_container_width=True,
                hide_index=True,
                height=500,
                column_config=column_config,
            )

            st.caption(
                f"Showing {len(display_alerts):,} alert rows. "
                "The public version contains synthetic "
                "demonstration records."
                if PUBLIC_DEMO_MODE
                else
                f"Showing {len(display_alerts):,} alert rows "
                "from the committed Gold serving data."
            )

        else:

            st.success(
                "No final fraud alerts are currently present."
            )


    # ========================================================
    # TAB 5 — ENGINEERING HEALTH
    # ========================================================

    with engineering_tab:

        st.subheader(
            "Feature & Serving Health"
        )

        (
            engineering_1,
            engineering_2,
            engineering_3,
        ) = st.columns(
            3
        )

        engineering_1.metric(
            "Window Feature Coverage",
            format_percentage(
                summary.get(
                    "window_feature_coverage"
                )
            ),
        )

        engineering_2.metric(
            "Unique Users",
            format_count(
                summary.get(
                    "unique_users"
                )
            ),
        )

        engineering_3.metric(
            "P95 Event→Score Latency",
            format_latency(
                summary.get(
                    "p95_event_to_score_latency_ms"
                )
            ),
        )

        st.divider()

        st.markdown(
            """
            **Metric interpretation**

            **Window feature coverage** measures the proportion
            of scored transactions that were enriched with an
            applicable 10-minute behavioral window.

            **Event-to-score latency** measures elapsed time
            from the transaction's event timestamp to its final
            scoring timestamp. It may include intentional
            simulator lateness, Kafka backlog and streaming
            processing delay.

            It is not the same as Spark trigger execution
            latency. The latter is measured separately through
            `StreamingQueryProgress` and Spark execution logs.
            """
        )

        if PUBLIC_DEMO_MODE:

            st.warning(
                "Engineering metrics shown in this public "
                "replay are illustrative synthetic values. "
                "They are not live infrastructure-health "
                "measurements or performance benchmark results."
            )

            st.markdown(
                """
                The full repository includes:

                - Structured Streaming progress listeners
                - State-store row and memory metrics
                - Watermark-drop monitoring
                - Controlled shuffle-partition benchmarks
                - Spark event logs
                - Spark History Server
                - Checkpoint recovery tests
                """
            )

        else:

            st.success(
                "These metrics are derived from committed "
                "local Gold Delta outputs."
            )

            st.link_button(
                "Open Spark History Server",
                "http://localhost:18080",
            )

            st.caption(
                "Spark History Server provides jobs, stages, "
                "tasks, executors, shuffle statistics and "
                "persisted execution plans."
            )


# ============================================================
# RENDER
# ============================================================

render_dashboard()