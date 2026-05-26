"""Generate a synthetic MovieLens-shape CSV for the ratings_etl_with_sentinel DAG.

Writes to airflow/data/ratings_sample.csv with columns matching what MovieLens
exports use: userId, movieId, rating, timestamp (epoch seconds).

The data is sized and shaped to pass the checks declared in
include/etl_transforms.py without further tuning:
  - n=12,000 rows (RowCountCheck min=10_000)
  - rating in {0.5, 1.0, ..., 5.0} biased toward 3.5 (RangeCheck + DistributionCheck)
  - ~200 distinct movies (>= 100 rows after aggregation)
  - timestamps within the last 24h (FreshnessCheck max_lag_hours=48)
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "ratings_sample.csv"


def main(n: int = 12_000, n_users: int = 2_000, n_movies: int = 200, seed: int = 42) -> Path:
    rng = np.random.default_rng(seed)
    now = int(datetime.now(timezone.utc).timestamp())
    ratings_grid = np.arange(0.5, 5.5, 0.5)
    rating_weights = np.array([0.02, 0.03, 0.05, 0.08, 0.12, 0.18, 0.20, 0.18, 0.10, 0.04])
    rating_weights = rating_weights / rating_weights.sum()

    df = pd.DataFrame(
        {
            "userId": rng.integers(1, n_users + 1, n),
            "movieId": rng.integers(1, n_movies + 1, n),
            "rating": rng.choice(ratings_grid, size=n, p=rating_weights),
            "timestamp": now - rng.integers(0, 24 * 3600, n),
        }
    )
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(
        f"wrote {len(df):,} rows to {OUT_PATH} "
        f"(rating mean={df['rating'].mean():.2f}, movies={df['movieId'].nunique()})"
    )
    return OUT_PATH


if __name__ == "__main__":
    main()
