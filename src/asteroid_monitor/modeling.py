"""A teaching classifier for NASA labels, with a held-out test split."""

import hashlib
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from asteroid_monitor.processing import validate_frame

FEATURES = ["diameter_km", "velocity_km_s", "miss_distance_km"]
MIN_RECORDS = 20
MIN_PER_CLASS = 4
RANDOM_SEED = 42


def _fingerprint(frame: pd.DataFrame) -> str:
    return hashlib.sha256(frame.to_json(orient="records", double_precision=15).encode()).hexdigest()


def train_classifier(frame: pd.DataFrame) -> dict:
    """Fit scaling and logistic regression on training rows only; persist plain JSON parameters.

    Daily datasets often lack enough examples of both classes. In that case, return
    a skipped artifact rather than fitting or claiming an evaluated model exists.
    """
    validate_frame(frame)
    if frame["asteroid_id"].duplicated().any():
        raise ValueError("Model input must contain unique asteroid identifiers")
    if frame["approach_date"].nunique() > 1:
        raise ValueError("Model input must contain one query date")
    artifact = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "algorithm": "StandardScaler + LogisticRegression",
        "purpose": "Educational approximation of NASA hazardous labels, not collision prediction",
        "sklearn_version": sklearn.__version__,
        "features": FEATURES.copy(),
        "dataset_sha256": _fingerprint(frame),
        "record_count": len(frame),
        "random_seed": RANDOM_SEED,
        "test_fraction": 0.25,
    }
    counts = frame["is_hazardous"].value_counts()
    if len(frame) < MIN_RECORDS or len(counts) != 2 or counts.min() < MIN_PER_CLASS:
        return {
            **artifact,
            "status": "skipped",
            "reason": (
                f"Need at least {MIN_RECORDS} unique asteroids and {MIN_PER_CLASS} examples "
                "of each NASA class. No model was trained."
            ),
        }
    train, test = train_test_split(
        frame, test_size=0.25, stratify=frame["is_hazardous"], random_state=RANDOM_SEED
    )
    pipeline = make_pipeline(
        StandardScaler(),
        LogisticRegression(class_weight="balanced", max_iter=1000, random_state=RANDOM_SEED),
    )
    pipeline.fit(train[FEATURES], train["is_hazardous"].astype(int))
    scaler, classifier = pipeline.steps[0][1], pipeline.steps[1][1]
    return {
        **artifact,
        "status": "trained",
        "train_ids": train["asteroid_id"].tolist(),
        "test_ids": test["asteroid_id"].tolist(),
        "majority_class": bool(train["is_hazardous"].mode().iloc[0]),
        "scaler_mean": scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "coefficients": classifier.coef_[0].tolist(),
        "intercept": float(classifier.intercept_[0]),
    }


def evaluate_classifier(frame: pd.DataFrame, artifact: dict) -> dict:
    """Evaluate saved parameters on held-out IDs; never refit on test data."""
    validate_frame(frame)
    if artifact["dataset_sha256"] != _fingerprint(frame):
        raise ValueError("Model dataset differs from the processed dataset")
    if artifact["features"] != FEATURES or artifact["schema_version"] != 1:
        raise ValueError("Unsupported model schema or feature ordering")
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "dataset_sha256": artifact["dataset_sha256"],
        "purpose": artifact["purpose"],
        "status": artifact["status"],
    }
    if artifact["status"] == "skipped":
        return {**report, "reason": artifact["reason"], "metrics": None}
    if artifact["status"] != "trained":
        raise ValueError("Unsupported model status")
    train_ids, test_ids = set(artifact["train_ids"]), set(artifact["test_ids"])
    if (
        not train_ids
        or not test_ids
        or train_ids & test_ids
        or train_ids | test_ids != set(frame["asteroid_id"])
    ):
        raise ValueError("Invalid or overlapping model train/test split")
    test = frame.set_index("asteroid_id").loc[artifact["test_ids"]]
    mean = np.asarray(artifact["scaler_mean"], dtype=float)
    scale = np.asarray(artifact["scaler_scale"], dtype=float)
    coefficients = np.asarray(artifact["coefficients"], dtype=float)
    if (
        any(
            values.shape != (len(FEATURES),) or not np.isfinite(values).all()
            for values in (mean, scale, coefficients)
        )
        or (scale <= 0).any()
    ):
        raise ValueError("Invalid model parameters")
    intercept = float(artifact["intercept"])
    if not np.isfinite(intercept):
        raise ValueError("Invalid model intercept")
    # Binary logistic regression predicts the positive class when the logit is > 0.
    scores = ((test[FEATURES].to_numpy() - mean) / scale) @ coefficients + intercept
    predicted = scores > 0
    actual = test["is_hazardous"].to_numpy(dtype=bool)
    baseline = np.full(len(test), artifact["majority_class"], dtype=bool)
    return {
        **report,
        "status": "evaluated",
        "train_records": len(train_ids),
        "test_records": len(test_ids),
        "metrics": {
            "accuracy": float(accuracy_score(actual, predicted)),
            "balanced_accuracy": float(balanced_accuracy_score(actual, predicted)),
            "precision": float(precision_score(actual, predicted, zero_division=0)),
            "recall": float(recall_score(actual, predicted, zero_division=0)),
            "f1": float(f1_score(actual, predicted, zero_division=0)),
            "baseline_accuracy": float(accuracy_score(actual, baseline)),
            "baseline_balanced_accuracy": float(balanced_accuracy_score(actual, baseline)),
        },
        "confusion_matrix": confusion_matrix(actual, predicted, labels=[False, True]).tolist(),
        "confusion_matrix_labels": ["Non-hazardous", "Potentially hazardous"],
    }
