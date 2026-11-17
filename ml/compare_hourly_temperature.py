"""Сравнивает дневные модели с двумя вариантами Ridge на почасовых признаках."""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

from train_temperature import (
    BASELINE,
    HOLDOUT_DAYS,
    VALIDATION_BLOCK,
    VARIANTS,
    date_range,
    fit_model,
    make_features,
    summarize,
)


HOURLY = ["temp_at_23_utc", "temp_change_6h", "temp_change_12h"]
DAILY = VARIANTS["temperature_weather"]
WEIGHT = 0.25
MODELS = [BASELINE, "daily_direct", "hourly_direct", "daily_blend", "hourly_blend"]


def predict(train: pd.DataFrame, test: pd.DataFrame) -> pd.DataFrame:
    predictions = test[["city", "target_date", "target"]].copy()
    predictions[BASELINE] = test["temp_avg"]
    changes = train.assign(target=train["target"] - train["temp_avg"])
    for label, features in (("daily", DAILY), ("hourly", DAILY + HOURLY)):
        predictions[f"{label}_direct"] = fit_model(train, features).predict(test)
        adjustment = fit_model(changes, features).predict(test)
        predictions[f"{label}_blend"] = test["temp_avg"] + WEIGHT * adjustment
    return predictions


def paired_counts(predictions: pd.DataFrame, candidate: str, reference: str) -> dict:
    candidate_error = (predictions[candidate] - predictions["target"]).abs()
    reference_error = (predictions[reference] - predictions["target"]).abs()
    equal = np.isclose(candidate_error, reference_error, atol=1e-10, rtol=0)
    return {
        "better": int(((candidate_error < reference_error) & ~equal).sum()),
        "worse": int(((candidate_error > reference_error) & ~equal).sum()),
        "tied": int(equal.sum()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="CSV из daily_stats с тремя почасовыми признаками")
    args = parser.parse_args()
    source = pd.read_csv(args.input)
    if not set(HOURLY).issubset(source) or not np.isfinite(source[HOURLY].to_numpy(dtype=float)).all():
        parser.error("Нужны три почасовых признака без пропусков для каждой пары город — дата")

    data = make_features(source).dropna(subset=["target"])
    dates = pd.DatetimeIndex(data["target_date"].unique())
    if len(dates) < HOLDOUT_DAYS + 7 * VALIDATION_BLOCK + 90:
        parser.error("Недостаточно истории для семи окон валидации и финального теста")
    validation_dates, holdout_dates = dates[:-HOLDOUT_DAYS], dates[-HOLDOUT_DAYS:]

    folds = []
    splitter = TimeSeriesSplit(n_splits=7, test_size=VALIDATION_BLOCK)
    for train_idx, test_idx in splitter.split(validation_dates):
        train = data[data["target_date"].isin(validation_dates[train_idx])]
        test = data[data["target_date"].isin(validation_dates[test_idx])]
        folds.append(predict(train, test))
    validation = pd.concat(folds)
    train = data[data["target_date"].isin(validation_dates)]
    test = data[data["target_date"].isin(holdout_dates)]
    holdout = predict(train, test)
    block_index = (holdout["target_date"] - holdout_dates.min()).dt.days // 14
    actual_change = holdout["target"] - holdout[BASELINE]

    print(json.dumps({
        "source_rows": len(source),
        "labeled_rows": len(data),
        "validation_rows": len(validation),
        "holdout_rows": len(holdout),
        "weight": WEIGHT,
        "validation": summarize(validation, MODELS),
        "validation_folds": [
            {"dates": date_range(fold["target_date"]), "metrics": summarize(fold, MODELS)}
            for fold in folds
        ],
        "holdout_dates": date_range(holdout_dates),
        "holdout": summarize(holdout, MODELS),
        "holdout_by_city": {
            city: summarize(group, MODELS) for city, group in holdout.groupby("city")
        },
        "holdout_blocks": [
            {"dates": date_range(group["target_date"]), "metrics": summarize(group, MODELS)}
            for _, group in holdout.groupby(block_index)
        ],
        "holdout_by_actual_change": {
            name: {"rows": int(mask.sum()), "metrics": summarize(holdout[mask], MODELS)}
            for name, mask in {
                "cooling": actual_change < -1,
                "stable": actual_change.abs() <= 1,
                "warming": actual_change > 1,
            }.items()
            if mask.any()
        },
        "holdout_comparisons": {
            "hourly_direct_vs_baseline": paired_counts(holdout, "hourly_direct", BASELINE),
            "hourly_direct_vs_daily": paired_counts(holdout, "hourly_direct", "daily_direct"),
            "hourly_blend_vs_baseline": paired_counts(holdout, "hourly_blend", BASELINE),
            "hourly_blend_vs_daily": paired_counts(holdout, "hourly_blend", "daily_blend"),
        },
    }, indent=2))


if __name__ == "__main__":
    main()
