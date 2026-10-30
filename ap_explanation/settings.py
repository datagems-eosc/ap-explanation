"""
The service's configuration, read once from the environment and ``.env``.

It imports nothing from the service, so that any module can import it.
"""
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, PositiveFloat, field_validator
from pydantic_core import PydanticUseDefault
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).parent.parent / ".env"


class Settings(BaseSettings):
    # An empty value (LLM_API_BASE=) is the same as no value
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE, extra="ignore", env_ignore_empty=True, frozen=True
    )

    redis_broker_uri: str = "redis://redis:6379/0"
    s3_mount_path: Path = Path("/mnt/s3")
    root_path: str = ""
    # When True (default), the FastAPI process spawns a Celery worker in a daemon
    # thread so no separate worker process is needed. Set to 'false' when running
    # dedicated standalone workers (e.g. via Docker) to avoid double-processing.
    use_embedded_celery_worker: bool = True
    # How long (seconds) a circuit cleanup waits for the other sessions on the
    # database to leave
    circuit_cleanup_wait: float = 60

    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_host: str | None = None
    postgres_port: str = "5432"
    postgres_timescale_host: str | None = None
    postgres_timescale_port: str = "5433"

    llm_api_base: str | None = None
    llm_api_key: str | None = None
    llm_api_model: str | None = None
    llm_ssl_verify: bool = True
    llm_timeout: PositiveFloat = 300.0

    oidc_issuer: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: str | None = None
    oidc_exchange_scope: str | None = None
    jwks_ttl_seconds: int = 300


def _empty_is_none(value: object) -> object:
    return None if value == "" else value


_EmptyIsNone = BeforeValidator(_empty_is_none)


class QueryLimits(BaseSettings):
    """
    Settings bounding the resources of a provenance query, in PostgreSQL's
    syntax ('10min', '1GB'). ``None`` leaves the server's value.

    Without them a costly query runs until it finishes or the database dies,
    holding the per-database lock all along.

    Read from ``PROVENANCE_<field>``. An empty value means ``None``, unlike in
    :class:`Settings`: that is how a limit is lifted.
    """

    model_config = SettingsConfigDict(
        env_prefix="PROVENANCE_", env_file=_ENV_FILE, extra="ignore", frozen=True
    )

    #: The one guard that holds whatever process grows: ProvSQL >= 1.13
    #: honours cancellation.
    statement_timeout: Annotated[str | None, _EmptyIsNone] = "10min"
    #: provsql.max_memory, the memory ProvSQL's evaluations may add to the
    #: backend. It does not cover ProvSQL's store worker, hence the timeout.
    max_memory: Annotated[str | None, _EmptyIsNone] = "1GB"
    #: PostgreSQL's default is no limit, so a query can fill the disk.
    #: Superuser-only (or GRANT SET): skipped with a warning otherwise.
    temp_file_limit: Annotated[str | None, _EmptyIsNone] = "5GB"
    #: max_parallel_workers_per_gather. Parallel workers grow outside
    #: provsql.max_memory's budget.
    max_parallel_workers: Annotated[str | None, _EmptyIsNone] = "0"
    #: Most rows the aggregates of the queries running at once on a server
    #: may receive. The circuit of an aggregate holds a gate per row, built
    #: while the query runs, before provsql.max_memory applies: past 16
    #: million rows on a 3 GB server, ProvSQL's store worker is killed and the
    #: whole server restarts, and the queries of all its databases add up.
    #: Counted by running the plain query first (see
    #: QueryRepository._check_aggregated_rows); None skips the check.
    max_aggregated_rows: Annotated[int | None, _EmptyIsNone] = 8_000_000
    #: Rows each query reserves at least, whatever its aggregates: the
    #: evaluations of queries running at once add up in the store worker too.
    #: None is an eighth of max_aggregated_rows: at most eight queries at once,
    #: as many as were measured together on a 3 GB server.
    min_reserved_rows: Annotated[int | None, _EmptyIsNone] = None
    #: statement_timeout of that plain run. A plain plan can be much slower
    #: than ProvSQL's rewritten one: a question of the meteo use case whose
    #: provenance query takes 0.3 s was refused at 60 s, and explained in
    #: 157 s with 300 s.
    precheck_timeout: str = "180s"
    #: Most rows of an aggregate's formula to read (sr_formula_rows, ProvSQL
    #: 1.14): the first is the aggregate with its number of terms, each other
    #: a term of a few dozen symbols.
    max_formula_rows: int = 100

    @field_validator("precheck_timeout", "max_formula_rows", mode="before")
    @classmethod
    def _empty_is_default(cls, value: object) -> object:
        """These two cannot be lifted."""
        if value == "":
            raise PydanticUseDefault()
        return value

    def settings(self) -> dict[str, str]:
        """The settings to apply, keyed by PostgreSQL parameter name."""
        return {
            name: value
            for name, value in {
                "statement_timeout": self.statement_timeout,
                "provsql.max_memory": self.max_memory,
                "temp_file_limit": self.temp_file_limit,
                "max_parallel_workers_per_gather": self.max_parallel_workers,
            }.items()
            if value
        }


settings = Settings()
