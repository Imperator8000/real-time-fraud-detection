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
