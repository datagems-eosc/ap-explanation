"""How environment values are read: the empty value and the types are the traps."""

from ap_explanation.settings import QueryLimits, Settings


def test_a_blank_limit_is_lifted(monkeypatch):
    monkeypatch.setenv("PROVENANCE_STATEMENT_TIMEOUT", "")
    monkeypatch.setenv("PROVENANCE_MAX_AGGREGATED_ROWS", "")

    limits = QueryLimits(_env_file=None)

    assert limits.statement_timeout is None
    assert limits.max_aggregated_rows is None
    assert "statement_timeout" not in limits.settings()


def test_a_blank_limit_that_cannot_be_lifted_is_the_default(monkeypatch):
    monkeypatch.setenv("PROVENANCE_PRECHECK_TIMEOUT", "")
    monkeypatch.setenv("PROVENANCE_MAX_FORMULA_ROWS", "")

    limits = QueryLimits(_env_file=None)

    assert limits.precheck_timeout == "180s"
    assert limits.max_formula_rows == 100


def test_limits_keep_their_variable_names(monkeypatch):
    monkeypatch.setenv("PROVENANCE_MAX_MEMORY", "2GB")
    monkeypatch.setenv("PROVENANCE_MAX_PARALLEL_WORKERS", "2")

    assert QueryLimits(_env_file=None).settings() == {
        "statement_timeout": "10min",
        "provsql.max_memory": "2GB",
        "temp_file_limit": "5GB",
        "max_parallel_workers_per_gather": "2",
    }


def test_values_are_not_guessed_from_their_look(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "12345")
    monkeypatch.setenv("LLM_API_KEY", "1e5")
    monkeypatch.setenv("LLM_SSL_VERIFY", "FALSE")

    settings = Settings(_env_file=None)

    assert settings.postgres_password == "12345"
    assert settings.llm_api_key == "1e5"
    assert settings.llm_ssl_verify is False


def test_blank_settings_are_unset(monkeypatch):
    monkeypatch.setenv("LLM_API_BASE", "")
    monkeypatch.setenv("LLM_SSL_VERIFY", "")

    settings = Settings(_env_file=None)

    assert settings.llm_api_base is None
    assert settings.llm_ssl_verify is True
