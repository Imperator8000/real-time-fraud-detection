import pytest

from src.state.user_behavior_state import (
    calculate_sample_stddev,
    update_welford,
)


def test_welford_running_mean_and_stddev():

    count = 0
    mean = 0.0
    m2 = 0.0

    for amount in [
        10.0,
        20.0,
        30.0,
    ]:

        (
            count,
            mean,
            m2,
        ) = update_welford(
            count=count,
            mean=mean,
            m2=m2,
            amount=amount,
        )

    stddev = (
        calculate_sample_stddev(
            count=count,
            m2=m2,
        )
    )

    assert count == 3

    assert mean == pytest.approx(
        20.0
    )

    # Sample standard deviation:
    #
    # values = 10, 20, 30
    # sample stddev = 10
    assert stddev == pytest.approx(
        10.0
    )


def test_welford_single_value_has_zero_stddev():

    (
        count,
        mean,
        m2,
    ) = update_welford(
        count=0,
        mean=0.0,
        m2=0.0,
        amount=75.0,
    )

    assert count == 1

    assert mean == pytest.approx(
        75.0
    )

    assert (
        calculate_sample_stddev(
            count=count,
            m2=m2,
        )
        == pytest.approx(
            0.0
        )
    )