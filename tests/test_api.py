from unittest.mock import Mock, patch

import pytest
import requests

from asteroid_monitor.api import fetch_feed


def test_success(feed, monkeypatch):
    monkeypatch.setenv("NASA_API_KEY", "secret-test-key")
    response = Mock()
    response.json.return_value = feed
    with patch("asteroid_monitor.api.requests.get", return_value=response) as get:
        assert fetch_feed("2025-01-01") == feed
    assert get.call_args.kwargs["params"]["api_key"] == "secret-test-key"
    assert get.call_args.kwargs["params"]["end_date"] == "2025-01-01"
    assert get.call_args.kwargs["timeout"] == (10, 60)


@pytest.mark.parametrize("error", [requests.Timeout("secret"), requests.HTTPError("secret")])
def test_request_failures_redact_secrets(error):
    with patch("asteroid_monitor.api.requests.get", side_effect=error):
        with pytest.raises(RuntimeError) as caught:
            fetch_feed("2025-01-01")
    assert "secret" not in str(caught.value)


def test_http_status_failure():
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("403 secret")
    with patch("asteroid_monitor.api.requests.get", return_value=response):
        with pytest.raises(RuntimeError, match="quota"):
            fetch_feed("2025-01-01")


@pytest.mark.parametrize("value", [[], None])
def test_malformed_json_object(value):
    response = Mock()
    response.json.return_value = value
    with patch("asteroid_monitor.api.requests.get", return_value=response):
        with pytest.raises(ValueError, match="object"):
            fetch_feed("2025-01-01")


def test_invalid_json_and_demo_key(monkeypatch):
    monkeypatch.delenv("NASA_API_KEY", raising=False)
    response = Mock()
    response.json.side_effect = ValueError("bad JSON")
    with patch("asteroid_monitor.api.requests.get", return_value=response) as get:
        with pytest.raises(ValueError, match="valid JSON"):
            fetch_feed("2025-01-01")
    assert get.call_args.kwargs["params"]["api_key"] == "DEMO_KEY"
