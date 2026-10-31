import numpy as np
import pytest

from asteroid_monitor.modeling import FEATURES, evaluate_classifier, train_classifier
from asteroid_monitor.storage import read_json, write_json


def test_training_and_held_out_evaluation(training_frame, tmp_path):
    model = train_classifier(training_frame)
    assert model["status"] == "trained"
    assert model["features"] == FEATURES
    assert "is_hazardous" not in FEATURES
    assert set(model["train_ids"]).isdisjoint(model["test_ids"])
    assert len(model["train_ids"]) == 60
    assert len(model["test_ids"]) == 20
    # Scaling must fit only the training subset, not the held-out rows.
    train = training_frame.set_index("asteroid_id").loc[model["train_ids"]]
    np.testing.assert_allclose(model["scaler_mean"], train[FEATURES].mean())
    assert model["scaler_mean"][0] != training_frame[FEATURES].mean().iloc[0]
    path = tmp_path / "models/classifier.json"
    write_json(path, model)
    report = evaluate_classifier(training_frame, read_json(path))
    assert report["status"] == "evaluated"
    assert report["metrics"]["accuracy"] > report["metrics"]["baseline_accuracy"]
    assert report["metrics"]["balanced_accuracy"] == 1.0
    assert report["metrics"]["f1"] == 1.0
    assert np.asarray(report["confusion_matrix"]).sum() == 20


def test_reproducible_split_and_model(training_frame):
    first, second = train_classifier(training_frame), train_classifier(training_frame)
    for model in (first, second):
        model.pop("generated_at")
    assert first == second


@pytest.mark.parametrize("size,hazard_count", [(0, 0), (2, 1), (20, 0), (20, 3)])
def test_insufficient_data_is_explicitly_skipped(training_frame, size, hazard_count):
    frame = training_frame.iloc[:size].copy()
    frame["is_hazardous"] = [i < hazard_count for i in range(size)]
    model = train_classifier(frame)
    assert model["status"] == "skipped"
    assert "coefficients" not in model
    evaluation = evaluate_classifier(frame, model)
    assert evaluation["metrics"] is None
    assert "No model was trained" in evaluation["reason"]


def test_changed_data_is_rejected(training_frame):
    model = train_classifier(training_frame)
    training_frame.loc[0, "velocity_km_s"] += 1
    with pytest.raises(ValueError, match="dataset differs"):
        evaluate_classifier(training_frame, model)


def test_overlapping_split_is_rejected(training_frame):
    model = train_classifier(training_frame)
    model["test_ids"][0] = model["train_ids"][0]
    with pytest.raises(ValueError, match="overlapping"):
        evaluate_classifier(training_frame, model)


def test_bad_parameters_are_rejected(training_frame):
    model = train_classifier(training_frame)
    model["coefficients"] = [float("nan")] * 3
    with pytest.raises(ValueError, match="parameters"):
        evaluate_classifier(training_frame, model)


def test_repeated_asteroids_are_rejected(training_frame):
    training_frame.loc[1, "asteroid_id"] = training_frame.loc[0, "asteroid_id"]
    training_frame.loc[1, "approach_date"] = "2025-01-02"
    with pytest.raises(ValueError, match="unique asteroid"):
        train_classifier(training_frame)
