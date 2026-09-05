from __future__ import annotations

import sys

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

if str(
    PROJECT_ROOT
) not in sys.path:

    sys.path.insert(
        0,
        str(
            PROJECT_ROOT
        ),
    )


from src.monitoring.dashboard_snapshot import (
    build_dashboard_snapshot,
)


# ============================================================
# PAGE
# ============================================================

st.set_page_config(
    page_title=(
        "Fraud Detection Command Center"
    ),
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# SMALL UI POLISH
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
# CACHE
# ============================================================

@st.cache_data(
    ttl=8,
    show_spinner=False,
)
def load_snapshot():

    return (
        build_dashboard_snapshot(
            persist=True
        )
    )


# ============================================================
# FORMATTERS
# ============================================================

def format_latency(
    milliseconds: float,
) -> str:

    if milliseconds < 1000:

        return (
            f"{milliseconds:,.0f} ms"
        )

    return (
        f"{milliseconds / 1000.0:,.2f} s"
    )


def format_money(
    value: float,
) -> str:

    return (
        f"${value:,.2f}"
    )


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

    st.markdown(
        "**Refresh interval:** 10 seconds"
    )

    if st.button(
        "Refresh now",
        use_container_width=True,
    ):

        st.cache_data.clear()

        st.rerun()


# ============================================================
# HEADER
# ============================================================

st.title(
    "🛡️ Fraud Detection Command Center"
)

st.markdown(
    """
    <div class="fraud-subtitle">
        Live monitoring of transaction behavior, Spark-engineered
        features, Random Forest inference and hybrid fraud decisions.
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

    try:

        snapshot = (
            load_snapshot()
        )

    except (
        FileNotFoundError,
        RuntimeError,
    ) as exc:

        st.info(
            "The dashboard is running, but no scored "
            "transaction data is available yet."
        )

        st.code(
            "docker compose -f docker\\docker-compose.yml "
            "--profile demo up -d"
        )

        st.caption(
            str(
                exc
            )
        )

        return


    summary = (
        snapshot[
            "summary"
        ]
    )

    severity_counts = (
        snapshot[
            "severity_counts"
        ]
    )

    decision_basis_counts = (
        snapshot[
            "decision_basis_counts"
        ]
    )

    hourly_metrics = (
        snapshot[
            "hourly_metrics"
        ]
    )

    top_alert_users = (
        snapshot[
            "top_alert_users"
        ]
    )

    recent_alerts = (
        snapshot[
            "recent_alerts"
        ]
    )


    # ========================================================
    # DATA FRESHNESS
    # ========================================================

    st.caption(
        "Snapshot refreshed: "
        f"{summary['snapshot_created_at_utc']}"
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
        f"{summary['transactions_processed']:,}",
    )

    metric_2.metric(
        "Fraud Alerts",
        f"{summary['fraud_alerts']:,}",
    )

    metric_3.metric(
        "Fraud Rate",
        f"{summary['fraud_rate']:.2%}",
    )

    metric_4.metric(
        "Critical Alerts",
        f"{summary['critical_alerts']:,}",
    )

    metric_5.metric(
        "Avg ML Probability",
        (
            f"{summary['average_ml_fraud_probability']:.3f}"
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
            summary[
                "average_transaction_value"
            ]
        ),
    )

    metric_7.metric(
        "P95 Transaction",
        format_money(
            summary[
                "p95_transaction_amount"
            ]
        ),
    )

    metric_8.metric(
        "Window Coverage",
        (
            f"{summary['window_feature_coverage']:.2%}"
        ),
    )

    metric_9.metric(
        "Avg Event→Score",
        format_latency(
            summary[
                "average_event_to_score_latency_ms"
            ]
        ),
    )

    metric_10.metric(
        "P95 Event→Score",
        format_latency(
            summary[
                "p95_event_to_score_latency_ms"
            ]
        ),
    )


    st.divider()


    # ========================================================
    # TABS
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
    # OVERVIEW
    # ========================================================

    with overview_tab:

        st.subheader(
            "Transaction & Fraud Trend"
        )

        if not hourly_metrics.empty:

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
                ]
            )

            hourly_chart = (
                hourly_chart
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

        else:

            st.info(
                "No hourly trend data yet."
            )


        left, right = (
            st.columns(
                2
            )
        )

        with left:

            st.subheader(
                "Severity Distribution"
            )

            if not severity_counts.empty:

                chart_df = (
                    severity_counts
                    .set_index(
                        "severity"
                    )
                )

                st.bar_chart(
                    chart_df[
                        [
                            "transaction_count"
                        ]
                    ],

                    height=330,
                )

            else:

                st.info(
                    "No severity data yet."
                )


        with right:

            st.subheader(
                "Fraud Decision Basis"
            )

            if not decision_basis_counts.empty:

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

            else:

                st.info(
                    "No final fraud alerts yet."
                )


    # ========================================================
    # ALERT ANALYSIS
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
            (
                f"{summary['rule_anomaly_count']:,}"
            ),
        )

        ml_column.metric(
            "ML High Confidence",
            (
                f"{summary['ml_high_confidence_alerts']:,}"
            ),
        )

        hybrid_column.metric(
            "Rule + ML",
            (
                f"{summary['hybrid_confirmation_alerts']:,}"
            ),
        )

        critical_rule_column.metric(
            "Critical Rule Escalation",
            (
                f"{summary['critical_rule_alerts']:,}"
            ),
        )

        st.markdown(
            """
            The final decision engine can escalate through:

            - high-confidence Random Forest prediction,
            - rule anomaly confirmed by ML,
            - or an exceptionally high rule score.
            """
        )


    # ========================================================
    # USER RISK
    # ========================================================

    with user_tab:

        st.subheader(
            "Users With the Most Fraud Alerts"
        )

        if not top_alert_users.empty:

            display_users = (
                top_alert_users.copy()
            )

            display_users[
                "maximum_ml_probability"
            ] = (
                display_users[
                    "maximum_ml_probability"
                ]
                .round(
                    4
                )
            )

            display_users[
                "total_alert_amount"
            ] = (
                display_users[
                    "total_alert_amount"
                ]
                .round(
                    2
                )
            )

            st.dataframe(
                display_users,
                use_container_width=True,
                hide_index=True,
            )

        else:

            st.info(
                "No alerting users yet."
            )


    # ========================================================
    # RECENT ALERTS
    # ========================================================

    with recent_tab:

        st.subheader(
            "Latest Fraud Alerts"
        )

        if not recent_alerts.empty:

            display_alerts = (
                recent_alerts.copy()
            )

            preferred_columns = [
                column
                for column in [
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
                if column
                in display_alerts.columns
            ]

            display_alerts = (
                display_alerts[
                    preferred_columns
                ]
            )

            if (
                "ml_fraud_probability"
                in display_alerts.columns
            ):

                display_alerts[
                    "ml_fraud_probability"
                ] = (
                    display_alerts[
                        "ml_fraud_probability"
                    ]
                    .round(
                        4
                    )
                )

            if (
                "amount_deviation_ratio"
                in display_alerts.columns
            ):

                display_alerts[
                    "amount_deviation_ratio"
                ] = (
                    display_alerts[
                        "amount_deviation_ratio"
                    ]
                    .round(
                        2
                    )
                )

            st.dataframe(
                display_alerts,
                use_container_width=True,
                hide_index=True,
                height=500,
            )

        else:

            st.success(
                "No final fraud alerts are currently present."
            )


    # ========================================================
    # ENGINEERING HEALTH
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
            (
                f"{summary['window_feature_coverage']:.2%}"
            ),
        )

        engineering_2.metric(
            "Unique Users",
            (
                f"{summary['unique_users']:,}"
            ),
        )

        engineering_3.metric(
            "P95 Event→Score Latency",
            format_latency(
                summary[
                    "p95_event_to_score_latency_ms"
                ]
            ),
        )

        st.markdown(
            """
            **Interpretation**

            `Window Feature Coverage` measures whether each
            transaction was successfully enriched with its
            applicable 10-minute behavioral window.

            `Event→Score Latency` measures the elapsed event-time
            to final-decision time. It can include deliberately
            late simulator events and Kafka backlog, so Spark
            trigger execution latency should still be examined
            separately through the Structured Streaming metrics
            and Spark History Server.
            """
        )

        st.markdown(
            "**Spark History Server:** "
            "`http://localhost:18080`"
        )


render_dashboard()