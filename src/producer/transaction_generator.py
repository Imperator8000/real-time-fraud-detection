from __future__ import annotations

import argparse
import json
import random
import uuid

from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Deque


# ============================================================
# SIMULATION CONSTANTS
# ============================================================

MERCHANT_CATEGORIES = (
    "grocery",
    "restaurant",
    "fuel",
    "electronics",
    "travel",
    "entertainment",
    "healthcare",
    "clothing",
    "utilities",
    "online_retail",
)


CATEGORY_AMOUNT_MULTIPLIERS = {
    "grocery": 0.75,
    "restaurant": 0.55,
    "fuel": 0.60,
    "electronics": 2.40,
    "travel": 2.80,
    "entertainment": 0.85,
    "healthcare": 1.40,
    "clothing": 1.10,
    "utilities": 1.00,
    "online_retail": 1.20,
}


FRAUD_TYPES = (
    "high_amount",
    "new_device_high_amount",
    "merchant_anomaly",
    "rapid_burst",
)


# ============================================================
# USER BEHAVIOURAL PROFILE
# ============================================================

@dataclass(frozen=True)
class UserProfile:
    """
    Synthetic behavioural characteristics for one user.

    These profiles simulate the historical behavioural patterns
    that our streaming state engine will later attempt to learn.
    """

    user_id: str
    baseline_amount: float
    primary_device_id: str
    secondary_device_id: str
    preferred_categories: tuple[str, ...]


# ============================================================
# GENERATED TRANSACTION
# ============================================================

@dataclass(frozen=True)
class GeneratedTransaction:
    """
    One generated event.

    Only the six business fields returned by to_payload() will
    eventually be sent to the production Kafka `transactions`
    topic.

    The remaining fields are simulator-only ground truth.
    """

    transaction_id: str
    user_id: str
    timestamp: datetime
    amount: float
    merchant_category: str
    device_id: str

    # --------------------------------------------------------
    # SIMULATION-ONLY METADATA
    # --------------------------------------------------------

    is_fraud_simulated: bool = False
    fraud_type: str | None = None
    lateness_seconds: int = 0
    is_duplicate: bool = False

    def to_payload(self) -> dict:
        """
        Return the exact JSON payload that will later be placed
        onto the Kafka transactions topic.
        """

        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "timestamp": format_timestamp(
                self.timestamp
            ),
            "amount": self.amount,
            "merchant_category": self.merchant_category,
            "device_id": self.device_id,
        }

    def to_debug_record(self) -> dict:
        """
        Return the event plus simulator ground truth.

        Used during development and testing only.
        """

        return {
            **self.to_payload(),
            "is_fraud_simulated": self.is_fraud_simulated,
            "fraud_type": self.fraud_type,
            "lateness_seconds": self.lateness_seconds,
            "is_duplicate": self.is_duplicate,
        }


# ============================================================
# TIMESTAMP UTILITIES
# ============================================================

def utc_now() -> datetime:
    """
    Return an explicit timezone-aware UTC timestamp.
    """

    return datetime.now(
        timezone.utc
    )


def format_timestamp(
    value: datetime,
) -> str:
    """
    Serialize timestamps using an ISO-8601 UTC representation.

    Example:
        2026-08-26T21:15:32.123456Z
    """

    utc_value = value.astimezone(
        timezone.utc
    )

    return utc_value.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


# ============================================================
# TRANSACTION GENERATOR
# ============================================================

class TransactionGenerator:
    """
    Stateful synthetic financial transaction generator.

    The generator maintains stable behavioural profiles for users
    so that normal spending differs meaningfully between users.

    It can also deliberately inject:

    - high-value fraud
    - new-device fraud
    - unusual merchant activity
    - rapid fraud bursts
    - duplicate messages
    - late-arriving events
    - events beyond the future Spark watermark
    """

    def __init__(
        self,
        *,
        number_of_users: int = 2_000,
        fraud_rate: float = 0.02,
        duplicate_rate: float = 0.01,
        late_within_watermark_rate: float = 0.03,
        late_beyond_watermark_rate: float = 0.01,
        seed: int = 42,
    ) -> None:

        if number_of_users <= 0:
            raise ValueError(
                "number_of_users must be greater than zero."
            )

        probability_values = (
            fraud_rate,
            duplicate_rate,
            late_within_watermark_rate,
            late_beyond_watermark_rate,
        )

        if any(
            probability < 0.0
            or probability > 1.0
            for probability in probability_values
        ):
            raise ValueError(
                "Simulation probabilities must be between 0 and 1."
            )

        self.rng = random.Random(
            seed
        )

        self.fraud_rate = fraud_rate
        self.duplicate_rate = duplicate_rate

        self.late_within_watermark_rate = (
            late_within_watermark_rate
        )

        self.late_beyond_watermark_rate = (
            late_beyond_watermark_rate
        )

        self.user_profiles = (
            self._create_user_profiles(
                number_of_users
            )
        )

        # Recently emitted events are retained only so we can
        # occasionally reproduce one as a duplicate.
        #
        # This deque has a fixed maximum size, preventing simulator
        # memory from growing indefinitely.
        self.recent_transactions: Deque[
            GeneratedTransaction
        ] = deque(
            maxlen=1_000
        )

        # Rapid-burst fraud produces several related events.
        # Remaining burst events are temporarily queued here.
        self.pending_events: Deque[
            GeneratedTransaction
        ] = deque()

    # ========================================================
    # PROFILE GENERATION
    # ========================================================

    def _create_user_profiles(
        self,
        number_of_users: int,
    ) -> tuple[UserProfile, ...]:

        profiles: list[
            UserProfile
        ] = []

        for user_number in range(
            1,
            number_of_users + 1,
        ):

            user_id = (
                f"user_{user_number:06d}"
            )

            # Different users have meaningfully different normal
            # spending levels.
            baseline_amount = round(
                self.rng.uniform(
                    20.0,
                    250.0,
                ),
                2,
            )

            preferred_categories = tuple(
                self.rng.sample(
                    MERCHANT_CATEGORIES,
                    k=3,
                )
            )

            profiles.append(
                UserProfile(
                    user_id=user_id,
                    baseline_amount=baseline_amount,
                    primary_device_id=(
                        f"{user_id}_device_primary"
                    ),
                    secondary_device_id=(
                        f"{user_id}_device_secondary"
                    ),
                    preferred_categories=(
                        preferred_categories
                    ),
                )
            )

        return tuple(
            profiles
        )

    # ========================================================
    # ID GENERATION
    # ========================================================

    def _new_transaction_id(
        self,
    ) -> str:

        # We use the seeded Random instance instead of uuid4()
        # so development runs remain reproducible.
        random_uuid = uuid.UUID(
            int=self.rng.getrandbits(
                128
            )
        )

        return (
            f"txn_{random_uuid.hex}"
        )

    # ========================================================
    # EVENT-TIME SIMULATION
    # ========================================================

    def _create_event_timestamp(
        self,
    ) -> tuple[datetime, int]:

        now = utc_now()

        probability = (
            self.rng.random()
        )

        # ----------------------------------------------------
        # VERY LATE EVENT
        # ----------------------------------------------------
        # Our future Spark watermark will tolerate 5 minutes.
        # These events deliberately exceed that threshold.

        if (
            probability
            < self.late_beyond_watermark_rate
        ):

            delay_seconds = (
                self.rng.randint(
                    6 * 60,
                    15 * 60,
                )
            )

            return (
                now
                - timedelta(
                    seconds=delay_seconds
                ),
                delay_seconds,
            )

        # ----------------------------------------------------
        # LATE BUT WITHIN WATERMARK
        # ----------------------------------------------------

        if (
            probability
            < (
                self.late_beyond_watermark_rate
                + self.late_within_watermark_rate
            )
        ):

            delay_seconds = (
                self.rng.randint(
                    30,
                    4 * 60,
                )
            )

            return (
                now
                - timedelta(
                    seconds=delay_seconds
                ),
                delay_seconds,
            )

        # ----------------------------------------------------
        # NORMAL ON-TIME EVENT
        # ----------------------------------------------------

        delay_seconds = (
            self.rng.randint(
                0,
                5,
            )
        )

        return (
            now
            - timedelta(
                seconds=delay_seconds
            ),
            delay_seconds,
        )

    # ========================================================
    # NORMAL SPENDING
    # ========================================================

    def _normal_amount(
        self,
        profile: UserProfile,
        merchant_category: str,
    ) -> float:

        category_multiplier = (
            CATEGORY_AMOUNT_MULTIPLIERS[
                merchant_category
            ]
        )

        # Transaction values are typically right-skewed rather
        # than normally distributed.
        #
        # A log-normal distribution gives us many ordinary
        # purchases and occasional naturally larger purchases.
        sigma = 0.50

        random_multiplier = (
            self.rng.lognormvariate(
                -0.5 * sigma**2,
                sigma,
            )
        )

        amount = (
            profile.baseline_amount
            * category_multiplier
            * random_multiplier
        )

        # Normal events are capped so extreme values are much
        # more strongly associated with injected fraud.
        amount = min(
            amount,
            2_500.0,
        )

        return round(
            max(
                amount,
                1.00,
            ),
            2,
        )

    def _generate_normal_transaction(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        merchant_category = (
            self.rng.choice(
                profile.preferred_categories
            )
        )

        amount = self._normal_amount(
            profile,
            merchant_category,
        )

        device_id = (
            profile.primary_device_id
            if self.rng.random() < 0.90
            else profile.secondary_device_id
        )

        (
            event_timestamp,
            lateness_seconds,
        ) = self._create_event_timestamp()

        return GeneratedTransaction(
            transaction_id=(
                self._new_transaction_id()
            ),
            user_id=profile.user_id,
            timestamp=event_timestamp,
            amount=amount,
            merchant_category=(
                merchant_category
            ),
            device_id=device_id,
            lateness_seconds=(
                lateness_seconds
            ),
        )

    # ========================================================
    # FRAUD SCENARIOS
    # ========================================================

    def _generate_high_amount_fraud(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        merchant_category = (
            self.rng.choice(
                MERCHANT_CATEGORIES
            )
        )

        multiplier = (
            self.rng.uniform(
                4.0,
                8.0,
            )
        )

        amount = round(
            profile.baseline_amount
            * CATEGORY_AMOUNT_MULTIPLIERS[
                merchant_category
            ]
            * multiplier,
            2,
        )

        timestamp, lateness = (
            self._create_event_timestamp()
        )

        return GeneratedTransaction(
            transaction_id=(
                self._new_transaction_id()
            ),
            user_id=profile.user_id,
            timestamp=timestamp,
            amount=max(
                amount,
                profile.baseline_amount
                * 4.0,
            ),
            merchant_category=(
                merchant_category
            ),
            device_id=(
                profile.primary_device_id
            ),
            is_fraud_simulated=True,
            fraud_type="high_amount",
            lateness_seconds=lateness,
        )

    def _generate_new_device_fraud(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        merchant_category = (
            self.rng.choice(
                (
                    "electronics",
                    "travel",
                    "online_retail",
                )
            )
        )

        amount = round(
            profile.baseline_amount
            * self.rng.uniform(
                4.0,
                7.0,
            ),
            2,
        )

        timestamp, lateness = (
            self._create_event_timestamp()
        )

        suspicious_device = (
            "unknown_device_"
            + uuid.UUID(
                int=self.rng.getrandbits(
                    128
                )
            ).hex[:12]
        )

        return GeneratedTransaction(
            transaction_id=(
                self._new_transaction_id()
            ),
            user_id=profile.user_id,
            timestamp=timestamp,
            amount=amount,
            merchant_category=(
                merchant_category
            ),
            device_id=(
                suspicious_device
            ),
            is_fraud_simulated=True,
            fraud_type=(
                "new_device_high_amount"
            ),
            lateness_seconds=lateness,
        )

    def _generate_merchant_anomaly(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        unusual_categories = [
            category
            for category
            in MERCHANT_CATEGORIES
            if category
            not in profile.preferred_categories
        ]

        merchant_category = (
            self.rng.choice(
                unusual_categories
            )
        )

        amount = round(
            profile.baseline_amount
            * self.rng.uniform(
                2.0,
                4.0,
            ),
            2,
        )

        timestamp, lateness = (
            self._create_event_timestamp()
        )

        return GeneratedTransaction(
            transaction_id=(
                self._new_transaction_id()
            ),
            user_id=profile.user_id,
            timestamp=timestamp,
            amount=amount,
            merchant_category=(
                merchant_category
            ),
            device_id=(
                profile.primary_device_id
            ),
            is_fraud_simulated=True,
            fraud_type="merchant_anomaly",
            lateness_seconds=lateness,
        )

    # ========================================================
    # RAPID-BURST FRAUD
    # ========================================================

    def _create_rapid_burst(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        burst_size = (
            self.rng.randint(
                5,
                8,
            )
        )

        now = utc_now()

        burst_events: list[
            GeneratedTransaction
        ] = []

        for event_number in range(
            burst_size
        ):

            seconds_ago = (
                (burst_size - event_number)
                * self.rng.randint(
                    6,
                    15,
                )
            )

            event_timestamp = (
                now
                - timedelta(
                    seconds=seconds_ago
                )
            )

            merchant_category = (
                self.rng.choice(
                    MERCHANT_CATEGORIES
                )
            )

            amount = round(
                profile.baseline_amount
                * self.rng.uniform(
                    0.8,
                    2.2,
                ),
                2,
            )

            burst_events.append(
                GeneratedTransaction(
                    transaction_id=(
                        self._new_transaction_id()
                    ),
                    user_id=(
                        profile.user_id
                    ),
                    timestamp=(
                        event_timestamp
                    ),
                    amount=amount,
                    merchant_category=(
                        merchant_category
                    ),
                    device_id=(
                        profile.primary_device_id
                    ),
                    is_fraud_simulated=True,
                    fraud_type="rapid_burst",
                    lateness_seconds=(
                        max(
                            0,
                            seconds_ago,
                        )
                    ),
                )
            )

        # Return the first event now and queue the remaining
        # events for subsequent calls.
        first_event = (
            burst_events[0]
        )

        self.pending_events.extend(
            burst_events[1:]
        )

        return first_event

    # ========================================================
    # FRAUD ROUTER
    # ========================================================

    def _generate_fraud_transaction(
        self,
        profile: UserProfile,
    ) -> GeneratedTransaction:

        fraud_type = (
            self.rng.choices(
                FRAUD_TYPES,
                weights=(
                    45,
                    25,
                    15,
                    15,
                ),
                k=1,
            )[0]
        )

        if fraud_type == "high_amount":
            return (
                self._generate_high_amount_fraud(
                    profile
                )
            )

        if (
            fraud_type
            == "new_device_high_amount"
        ):
            return (
                self._generate_new_device_fraud(
                    profile
                )
            )

        if (
            fraud_type
            == "merchant_anomaly"
        ):
            return (
                self._generate_merchant_anomaly(
                    profile
                )
            )

        return (
            self._create_rapid_burst(
                profile
            )
        )

    # ========================================================
    # PUBLIC GENERATION METHOD
    # ========================================================

    def generate_event(
        self,
    ) -> GeneratedTransaction:

        # ----------------------------------------------------
        # FINISH AN ACTIVE FRAUD BURST FIRST
        # ----------------------------------------------------

        if self.pending_events:

            event = (
                self.pending_events.popleft()
            )

            self.recent_transactions.append(
                event
            )

            return event

        # ----------------------------------------------------
        # OCCASIONAL KAFKA-LIKE DUPLICATE
        # ----------------------------------------------------

        if (
            self.recent_transactions
            and self.rng.random()
            < self.duplicate_rate
        ):

            original = (
                self.rng.choice(
                    tuple(
                        self.recent_transactions
                    )
                )
            )

            return replace(
                original,
                is_duplicate=True,
            )

        profile = self.rng.choice(
            self.user_profiles
        )

        # ----------------------------------------------------
        # NORMAL VS FRAUD EVENT
        # ----------------------------------------------------

        if (
            self.rng.random()
            < self.fraud_rate
        ):

            event = (
                self._generate_fraud_transaction(
                    profile
                )
            )

        else:

            event = (
                self._generate_normal_transaction(
                    profile
                )
            )

        self.recent_transactions.append(
            event
        )

        return event


# ============================================================
# DEVELOPMENT PREVIEW
# ============================================================

def preview_events(
    *,
    count: int,
    seed: int,
) -> None:

    generator = (
        TransactionGenerator(
            seed=seed
        )
    )

    fraud_count = 0
    duplicate_count = 0
    late_within_count = 0
    late_beyond_count = 0

    for _ in range(
        count
    ):

        event = (
            generator.generate_event()
        )

        print(
            json.dumps(
                event.to_debug_record(),
                indent=None,
            )
        )

        if event.is_fraud_simulated:
            fraud_count += 1

        if event.is_duplicate:
            duplicate_count += 1

        if (
            30
            <= event.lateness_seconds
            <= 5 * 60
        ):
            late_within_count += 1

        if (
            event.lateness_seconds
            > 5 * 60
        ):
            late_beyond_count += 1

    print()
    print("=" * 72)
    print(
        "TRANSACTION GENERATOR SUMMARY"
    )
    print("=" * 72)
    print(
        f"Events generated:            {count:,}"
    )
    print(
        f"Simulated fraud events:      {fraud_count:,}"
    )
    print(
        f"Duplicate events:            {duplicate_count:,}"
    )
    print(
        f"Late within watermark:       {late_within_count:,}"
    )
    print(
        f"Beyond 5-minute watermark:   {late_beyond_count:,}"
    )


def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Preview synthetic financial "
            "transactions."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=25,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    return parser.parse_args()


def main() -> None:

    arguments = (
        parse_arguments()
    )

    preview_events(
        count=arguments.count,
        seed=arguments.seed,
    )


if __name__ == "__main__":
    main()