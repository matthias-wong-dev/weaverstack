"""One acceptance estate, driven through Weaver's public operations.

This moves one estate through ``build``, ``load``, ``test`` and ``wipe`` from
the desktop, which is what a user drives.

A failed step is reported once, by the first test that requires it, with the
original error. Every later test requiring it skips naming the step, so a broken
journey is one failure rather than a screen of them, and never a pass. A failure
no test required fails the journey's ``close``.

A result with a ``succeeded`` property that is false is a failed step: a build,
load or test run reports its outcome rather than raising it.
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
    #: The failed step this one did not run after.
    upstream: str | None = None
    reported: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None and self.upstream is None


@dataclass
class Acceptance:
    """An ordered series of public operations over one estate."""

    name: str
    steps: dict[str, Step] = field(default_factory=dict)
    _failed: str | None = None

    def step(
        self,
        name: str,
        action: Callable[[], Any],
        *,
        observe: Callable[[], Any] | None = None,
    ) -> Step:
        """Take one transition, then ``observe`` the estate it left."""

        if self._failed is not None:
            step = self.steps[name] = Step(name=name, upstream=self._failed)
            return step
        step = self.steps[name] = Step(name=name)
        try:
            step.result = action()
            if getattr(step.result, "succeeded", True) is False:
                raise AssertionError(
                    f"step {name!r} did not succeed: {_shown(step.result)}"
                )
            if observe is not None:
                step.observation = observe()
        except Exception as exc:
            step.error = exc
            self._failed = name
        except BaseException as exc:
            # A pytest outcome or an interrupt belongs to the test that raised it.
            step.error, step.reported = exc, True
            self._failed = name
            raise
        return step

    def __getitem__(self, name: str) -> Step:
        step = self.steps[name]
        if not step.ok:
            raise AssertionError(f"acceptance step {name!r} failed")
        return step

    def require(self, *names: str) -> None:
        """Skip this test when a step it reads has not succeeded.

        The first test to require a failed step fails with its error instead.
        """

        import pytest

        for name in names:
            step = self.steps.get(name)
            if step is None:
                pytest.skip(f"step {name!r} has not run")
            origin = self.steps[step.upstream] if step.upstream else step
            if origin.error is None:
                continue
            if not origin.reported:
                origin.reported = True
                raise origin.error
            pytest.skip(f"step {origin.name!r} failed")

    def close(self) -> None:
        """Fail on a step failure no test reported."""

        unreported = [
            step
            for step in self.steps.values()
            if step.error is not None and not step.reported
        ]
        for step in unreported:
            step.reported = True
        if unreported:
            names = ", ".join(repr(step.name) for step in unreported)
            raise AssertionError(
                f"{self.name}: step {names} failed and no test reported it"
            ) from unreported[0].error


def _shown(result: Any) -> Any:
    to_mapping = getattr(result, "to_mapping", None)
    return to_mapping() if callable(to_mapping) else result
