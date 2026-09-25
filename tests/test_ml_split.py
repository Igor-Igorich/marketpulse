import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ml.train import check_feature_variance, temporal_split


def test_temporal_split_does_not_shuffle():
    df = pd.DataFrame(
        {
            "trade_time": pd.date_range(
                "2026-09-15 09:00", periods=100, freq="1min"
            ),
            "price": range(100),
        }
    )
    train_df, test_df = temporal_split(df, test_fraction=0.2)

    assert len(train_df) == 80
    assert len(test_df) == 20
    assert train_df["trade_time"].max() < test_df["trade_time"].min()


def test_check_feature_variance_detects_constant_column(caplog):
    X = pd.DataFrame(
        {
            "hour": [17, 17, 17],
            "lag_1": [100.0, 101.0, 99.0],
        }
    )
    with caplog.at_level("WARNING"):
        check_feature_variance(X, ticker="TEST")

    assert "hour" in caplog.text
    assert "lag_1" not in caplog.text
