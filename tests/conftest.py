"""Shared pytest fixtures for sentinel tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

_EVENT_TYPES = ["page_view", "click", "search", "play", "pause"]


def _make_base_df(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    user_ids = [f"user_{i:05d}" for i in rng.integers(0, 5000, size=n)]
    ratings = rng.choice(np.arange(0.5, 5.5, 0.5), size=n)
    events = rng.choice(_EVENT_TYPES, size=n)
    event_ts = [now - timedelta(minutes=int(m)) for m in rng.integers(1, 60, size=n)]
    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "rating": ratings.astype(float),
            "event_type": events,
            "event_ts": pd.to_datetime(event_ts).tz_localize(None),
        }
    )
    df["user_id"] = df["user_id"].astype(object)
    df["event_type"] = df["event_type"].astype(object)
    df["event_ts"] = df["event_ts"].astype("datetime64[ns]")
    return df


@pytest.fixture
def sample_df_clean() -> pd.DataFrame:
    return _make_base_df(1000)


@pytest.fixture
def sample_df_with_nulls() -> pd.DataFrame:
    df = _make_base_df(1000)
    null_idx = np.random.default_rng(7).choice(len(df), size=50, replace=False)
    df.loc[null_idx, "user_id"] = None
    return df


@pytest.fixture
def sample_df_wrong_schema() -> pd.DataFrame:
    df = _make_base_df(1000)
    df["rating"] = df["rating"].astype(str)
    return df


@pytest.fixture
def sample_df_stale() -> pd.DataFrame:
    df = _make_base_df(1000)
    stale_ts = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None) - timedelta(days=3)
    df["event_ts"] = stale_ts
    return df


@pytest.fixture
def sample_df_small() -> pd.DataFrame:
    return _make_base_df(10)


@pytest.fixture
def sample_df_out_of_range() -> pd.DataFrame:
    df = _make_base_df(1000)
    df.loc[df.index[:5], "rating"] = 99.0
    return df


@pytest.fixture
def sample_df_low_variety_user() -> pd.DataFrame:
    df = _make_base_df(1000)
    df["user_id"] = "single_user"
    return df
