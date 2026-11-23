"""The LLM call leaves the event loop free: the task's database lock is extended from it."""

import asyncio
import time

import pytest

from ap_explanation.domains.nl_explanation import explanation_agent
from ap_explanation.domains.nl_explanation.explanation_agent import ExplanationAgent


@pytest.mark.asyncio
async def test_the_event_loop_runs_while_the_model_answers(monkeypatch: pytest.MonkeyPatch):
    def slow_completion(**kwargs: object) -> object:
        time.sleep(0.5)  # litellm's completion blocks
        raise RuntimeError("answered")

    monkeypatch.setattr(explanation_agent, "completion", slow_completion)
    agent = ExplanationAgent("http://llm.invalid", "model", timeout=1)
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.05)
            ticks += 1

    ticker = asyncio.create_task(tick())
    with pytest.raises(RuntimeError, match="answered"):
        await agent.explain("SELECT 1", "t@k1", "")
    ticker.cancel()

    assert ticks >= 5
