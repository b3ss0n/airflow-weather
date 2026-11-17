"""Сравнивает бейзлайн и Ridge-поправку к прогнозу температуры на следующие сутки."""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

from train_temperature import (
    HOLDOUT_DAYS,
    VALIDATION_BLOCK,
    VARIANTS,
    date_range,
    fit_model,
    make_features,
    score,
)


WEIGHTS = (0.0, 0.25, 0.5, 0.75, 1.0)
FEATURES = VARIANTS["temperature_weather"]


def predict_change(train: pd.DataFrame, test: pd.DataFrame) -> pd.DataFrame:
    changes = train.assign(target=train["target"] - train["temp_avg"])
    result = test[["city", "target_date", "target", "temp_avg"]].copy()
    result["predicted_change"] = fit_model(changes, FEATURES).predict(test)
    return result


def evaluate(predictions: pd.DataFrame, weight: float) -> dict:
    actual = predictions["target"]
    baseline = predictions["temp_avg"]
    blended = baseline + weight * predictions["predicted_change"]
    baseline_error = (actual - baseline).abs()
    blend_error = (actual - blended).abs()
    tied = np.isclose(baseline_error, blend_error, atol=1e-10, rtol=0)
    return {
        "rows": len(predictions),
        "baseline": score(actual, baseline),
        "blend": score(actual, blended),
        "better": int(((blend_error < baseline_error) & ~tied).sum()),
        "worse": int(((blend_error > baseline_error) & ~tied).sum()),
        "tied": int(tied.sum()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Тот же CSV из daily_stats, что и для обучения Ridge")
    args = parser.parse_args()
    data = make_features(pd.read_csv(args.input)).dropna(subset=["target"])
    dates = pd.DatetimeIndex(data["target_date"].unique())
    if len(dates) < HOLDOUT_DAYS + 7 * VALIDATION_BLOCK + 90:
        parser.error("Недостаточно истории для семи окон валидации и финального теста")
    validation_dates, holdout_dates = dates[:-HOLDOUT_DAYS], dates[-HOLDOUT_DAYS:]

    folds = []
    splitter = TimeSeriesSplit(n_splits=7, test_size=VALIDATION_BLOCK)
    for train_idx, test_idx in splitter.split(validation_dates):
        train = data[data["target_date"].isin(validation_dates[train_idx])]
        test = data[data["target_date"].isin(validation_dates[test_idx])]
        folds.append(predict_change(train, test))
    validation = pd.concat(folds)
    weight_results = {weight: evaluate(validation, weight) for weight in WEIGHTS}
    selected = min(WEIGHTS, key=lambda weight: weight_results[weight]["blend"]["mae"])

    train = data[data["target_date"].isin(validation_dates)]
    test = data[data["target_date"].isin(holdout_dates)]
    holdout = predict_change(train, test)
    block_index = (holdout["target_date"] - holdout_dates.min()).dt.days // 14
    actual_change = holdout["target"] - holdout["temp_avg"]
    print(json.dumps({
        "selected_weight": selected,
        "validation_weights": weight_results,
        "validation_folds": [
            {"dates": date_range(fold["target_date"]), **evaluate(fold, selected)}
            for fold in folds
        ],
        "holdout_dates": date_range(holdout_dates),
        "holdout": evaluate(holdout, selected),
        "holdout_by_city": {
            city: evaluate(group, selected) for city, group in holdout.groupby("city")
        },
        "holdout_blocks": [
            {"dates": date_range(group["target_date"]), **evaluate(group, selected)}
            for _, group in holdout.groupby(block_index)
        ],
        "holdout_by_actual_change": {
            name: evaluate(holdout[mask], selected)
            for name, mask in {
                "cooling": actual_change < -1,
                "stable": actual_change.abs() <= 1,
                "warming": actual_change > 1,
            }.items()
            if mask.any()
        },
    }, indent=2))


if __name__ == "__main__":
    main()
