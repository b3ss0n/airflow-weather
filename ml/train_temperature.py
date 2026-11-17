"""Обучает прогноз средней температуры на следующие сутки по CSV из daily_stats."""

import argparse
import json
import platform
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


BASE = ["temp_avg", "temp_avg_prev", "temp_avg_3d", "temp_avg_7d", "temp_min", "temp_max"]
WEATHER = ["precipitation_sum", "wind_speed_avg"]
SEASON = ["year_sin", "year_cos"]
VARIANTS = {
    "temperature": BASE,
    "temperature_season": BASE + SEASON,
    "temperature_weather": BASE + WEATHER,
    "temperature_weather_season": BASE + WEATHER + SEASON,
}
BASELINE = "persistence"
VALIDATION_BLOCK = 28
HOLDOUT_DAYS = 42


def make_features(source: pd.DataFrame) -> pd.DataFrame:
    data = source.copy()
    data["date"] = pd.to_datetime(data["date"], format="%Y-%m-%d", errors="raise")
    measurements = ["temp_avg", "temp_min", "temp_max", *WEATHER]
    calendar = pd.date_range(data["date"].min(), data["date"].max(), freq="D")
    days_per_city = data.groupby("city")["date"].nunique()
    if (
        data[["city", "date"]].isna().any().any()
        or data.duplicated(["city", "date"]).any()
        or len(days_per_city) != 4
        or not days_per_city.eq(len(calendar)).all()
        or not np.isfinite(data[measurements].to_numpy(dtype=float)).all()
    ):
        raise ValueError("Нужны непрерывные дневные ряды без пропусков для четырёх городов")

    data = data.sort_values(["city", "date"])
    temperatures = data.groupby("city")["temp_avg"]
    data["temp_avg_prev"] = temperatures.shift(1)
    for window in (3, 7):
        data[f"temp_avg_{window}d"] = temperatures.transform(lambda series: series.rolling(window).mean())

    data["target"] = temperatures.shift(-1)
    data["target_date"] = data["date"].to_numpy(dtype="datetime64[ns]") + np.timedelta64(1, "D")
    days_in_year = np.where(data["target_date"].dt.is_leap_year, 366, 365)
    angle = 2 * np.pi * (data["target_date"].dt.dayofyear - 1) / days_in_year
    data["year_sin"], data["year_cos"] = np.sin(angle), np.cos(angle)
    return data.dropna(subset=["temp_avg_7d"]).sort_values(["target_date", "city"])


def fit_model(train: pd.DataFrame, numerical: list[str]):
    preprocess = ColumnTransformer([
        ("numeric", StandardScaler(), numerical),
        ("city", OneHotEncoder(handle_unknown="error"), ["city"]),
    ])
    return make_pipeline(preprocess, Ridge(alpha=1.0)).fit(train, train["target"])


def score(actual, predicted) -> dict:
    errors = np.asarray(predicted) - np.asarray(actual)
    return {
        "mae": float(np.abs(errors).mean()),
        "rmse": float(np.sqrt(np.square(errors).mean())),
        "bias": float(errors.mean()),
        "max_absolute_error": float(np.abs(errors).max()),
    }


def date_range(dates) -> list[str]:
    return [dates.min().date().isoformat(), dates.max().date().isoformat()]


def evaluate(train, test, variants):
    predictions = test[["city", "target_date", "target"]].copy()
    predictions[BASELINE] = test["temp_avg"]
    for name, features in variants.items():
        predictions[name] = fit_model(train, features).predict(test)
    return predictions


def summarize(predictions, model_names):
    return {name: score(predictions["target"], predictions[name]) for name in model_names}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="CSV с колонками таблицы daily_stats")
    parser.add_argument("--model", required=True, type=Path, help="Путь для артефакта joblib")
    args = parser.parse_args()

    source = pd.read_csv(args.input)
    features = make_features(source)
    labeled = features.dropna(subset=["target"])
    dates = pd.DatetimeIndex(labeled["target_date"].unique())
    if len(dates) < HOLDOUT_DAYS + 7 * VALIDATION_BLOCK + 90:
        parser.error("Недостаточно истории для семи окон валидации и финального теста")
    selection_dates = dates[:-HOLDOUT_DAYS]
    test_dates = dates[-HOLDOUT_DAYS:]

    names = [BASELINE, *VARIANTS]
    folds, validation_predictions = [], []
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=7, test_size=VALIDATION_BLOCK).split(selection_dates):
        train_dates, validation_dates = selection_dates[train_idx], selection_dates[validation_idx]
        train = labeled[labeled["target_date"].isin(train_dates)]
        validation = labeled[labeled["target_date"].isin(validation_dates)]
        predictions = evaluate(train, validation, VARIANTS)
        validation_predictions.append(predictions)
        folds.append({
            "training_dates": date_range(train_dates),
            "training_days": len(train_dates),
            "validation_dates": date_range(validation_dates),
            "metrics": summarize(predictions, names),
        })
    validation_metrics = summarize(pd.concat(validation_predictions), names)
    selected = min(VARIANTS, key=lambda name: validation_metrics[name]["mae"])

    train = labeled[labeled["target_date"].isin(selection_dates)]
    test = labeled[labeled["target_date"].isin(test_dates)]
    predictions = evaluate(train, test, {selected: VARIANTS[selected]})
    finalists = [BASELINE, selected]
    holdout_metrics = summarize(predictions, finalists)
    holdout_by_city = {city: summarize(group, finalists) for city, group in predictions.groupby("city")}
    block_index = (predictions["target_date"] - test_dates.min()).dt.days // 14
    holdout_blocks = [
        {"dates": date_range(group["target_date"]), "metrics": summarize(group, finalists)}
        for _, group in predictions.groupby(block_index)
    ]

    fitted = fit_model(labeled, VARIANTS[selected])
    latest = features[features["target_date"].eq(features["target_date"].max())]
    artifact = {
        "model": fitted,
        "variant": selected,
        "features": ["city", *VARIANTS[selected]],
        "trained_through": dates.max().date().isoformat(),
        "cities": sorted(labeled["city"].unique()),
        "sklearn_version": sklearn.__version__,
    }
    joblib.dump(artifact, args.model)
    restored = joblib.load(args.model)
    np.testing.assert_allclose(restored["model"].predict(latest), fitted.predict(latest))

    print(json.dumps({
        "python_version": platform.python_version(),
        "sklearn_version": sklearn.__version__,
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
        "joblib_version": joblib.__version__,
        "source_rows": len(source),
        "labeled_rows": len(labeled),
        "labeled_dates": date_range(dates),
        "selected": selected,
        "folds": folds,
        "validation": validation_metrics,
        "holdout_dates": date_range(test_dates),
        "holdout": holdout_metrics,
        "holdout_by_city": holdout_by_city,
        "holdout_blocks": holdout_blocks,
    }, indent=2))


if __name__ == "__main__":
    main()
