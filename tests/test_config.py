import pytest

from app.config import Settings


def test_settings_use_expected_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.environment == "local"
    assert settings.log_level == "INFO"
    assert settings.redis_url == "redis://localhost:6379/0"


def test_settings_read_prefixed_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENVIRONMENT", "test")
    monkeypatch.setenv("APP_LOG_LEVEL", "DEBUG")

    settings = Settings(_env_file=None)

    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"
