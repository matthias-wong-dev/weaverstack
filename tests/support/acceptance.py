"""One acceptance estate, driven through Weaver's public operations.

This moves one estate through ``build``, ``load``, ``test`` and ``wipe`` from
the desktop, which is what a user drives.

The no-cascade contract is the same: a failed step is recorded, and every later
step is skipped naming the step that broke, so a broken journey reports one
failure rather than a screen of them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Step:
    """What one transition did, or the exception that stopped it."""

    name: str
    result: Any = None
    error: BaseException | None = None
    observation: Any = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class Acceptance:
    """An ordered series of public operations over one estate."""

    name: str
    steps: dict[str, Step] = field(default_factory=dict)
    _failed: str | None = None

    def step(self, name: str, action: Callable[[], Any]) -> Step:
        """Take one transition. A recorded failure skips everything after it."""

        if self._failed is not None:
            step = Step(
                name=name, error=RuntimeError(f"upstream step {self._failed!r} failed")
            )
            self.steps[name] = step
            return step
        try:
            step = Step(name=name, result=action())
        except BaseException as exc:  # recorded, not raised: the journey continues
            self._failed = name
            step = Step(name=name, error=exc)
        self.steps[name] = step
        return step

    def fail(self, name: str) -> None:
        """Mark a step failed on a claim its action could not raise on."""

        self._failed = name

    def __getitem__(self, name: str) -> Step:
        step = self.steps[name]
        if step.error is not None:
            raise AssertionError(f"acceptance step {name!r} failed: {step.error}")
        return step

    def require(self, *names: str) -> None:
        """Skip this test when a step it reads has not succeeded."""

        import pytest

        for name in names:
            step = self.steps.get(name)
            if step is None:
                pytest.skip(f"step {name!r} has not run")
            if step.error is not None:
                pytest.skip(f"step {name!r} failed: {step.error}")
