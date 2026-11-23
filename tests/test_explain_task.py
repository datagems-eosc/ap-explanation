"""The task's cache, as two tasks explaining the same AP see it."""

import json
from contextlib import nullcontext
from pathlib import Path

import pytest

from ap_explanation.tasks import explain

AP = json.loads((Path(__file__).parent.parent / "fixtures/explain_sql_query_mathe.json").read_text())
RESULT: dict[str, object] = {"derivations": [], "explanation": None}


def test_a_task_that_waited_for_the_lock_reuses_the_result(monkeypatch: pytest.MonkeyPatch):
    """The other task stored its result while this one waited: nothing to compute."""
    stored: dict[str, object] = {}

    class Cache:
        def get(self, key: str) -> object:
            return stored.get(key)

        def set(self, key: str, value: object) -> None:
            stored[key] = value

    def locked(db_name: str) -> nullcontext[None]:
        # The task holding the lock finishes while this one waits for it
        stored[explain.RedisCacheProvider.make_key(AP, None, False)] = RESULT
        return nullcontext()

    def must_not_run(*args: object) -> object:
        raise AssertionError("computed again")

    monkeypatch.setattr(explain, "get_cache_provider", Cache)
    monkeypatch.setattr(explain, "locked", locked)
    monkeypatch.setattr(explain, "provenance_service_for", must_not_run)

    assert explain.explain_task.run(AP) == RESULT
