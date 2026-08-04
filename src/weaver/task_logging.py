"""Immutable file evidence for one top-level Weaver task.

Generic across Weaver's five top-level tasks — ``wipe``, ``mirror``, ``build``,
``load`` and ``test`` — because a log that knew what a load was would need a
second one the first time anything else wanted evidence, and the two would drift.
Nothing here understands a DAG: a task writes a plan, some steps and a
completion, and what those contain is the task's business.

.. code-block:: text

    Files/_/Log/
    └── task_date=2026-08-03/
        └── 20260803T091522.123456Z_load_<task-uuid>/
            ├── plan.json
            ├── 20260803T091523.012345Z_load_<step-uuid>.json
            ├── 20260803T091530.441092Z_refresh_<step-uuid>.json
            └── 20260803T091540.102334Z_complete_<task-uuid>.json

**The folder is a declared Weaver document, not a path this module knows.**
``_.Log`` is an ordinary ``Folder`` in the built-in control-plane item, so it is
projected into the catalogue, inventoried, installed, converged on and protected
from prune by the machinery that already exists — and this module asks a resolver
where that folder is rather than composing ``Files/_/Log`` for itself. A special
path known only to the logger would need its own creation rule, its own prune
exemption and its own removal rule, each of which is a rule nothing else has.

**Nothing is ever rewritten.** A step's file is written when the step finishes
and is never touched again, which is what makes the log usable after an
interruption: the plan says what was intended, the step files say what completed,
and the *absence* of a completion file is how you know the task did not finish.
A log the runner updated in place could not say that — a crashed task and a
finished one would look the same.

**A dry run writes nothing at all.** Validation is not execution, and a folder of
plausible-looking steps that never ran is worse than no folder: it is evidence of
work nobody did. The dry run's complete result is returned in memory instead.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from .catalogue.builtin import LOG_FOLDER, LOG_FOLDER_ID
from .catalogue.tables import CATALOGUE_SCHEMA
from .errors import CommandError
from .locations import Location
from .store import Store
from .targets import FolderTarget, ItemRef

#: Weaver's top-level tasks. A task type is part of a folder name, so a reader
#: can see what ran without opening anything.
TASK_TYPES = ("wipe", "mirror", "build", "load", "test")

#: The date partition's key. ``task_date`` is the UTC date the task *started*; a
#: task that crosses midnight stays in the partition it began in, because a run
#: is one thing and splitting it across two partitions would make it two.
DATE_PARTITION = "task_date"

PLAN_FILE = "plan.json"
COMPLETE_STEP = "complete"

_TIMESTAMP = "%Y%m%dT%H%M%S.%f"


def log_folder(resolver: Any, weaver_lakehouse: ItemRef | str) -> Location:
    """Where the declared ``_.Log`` folder materialises, per the resolver.

    Derived from the folder's *identity* through the same resolution every other
    Folder object goes through, so the logger writes where the build installed
    rather than where the logger guessed.
    """

    item = ItemRef(weaver_lakehouse) if isinstance(weaver_lakehouse, str) else weaver_lakehouse
    return resolver.folder_object(FolderTarget(item), CATALOGUE_SCHEMA, LOG_FOLDER)


@dataclass(frozen=True)
class TaskLog:
    """One task's evidence folder, open for writing.

    Holds no mutable run state beyond the files it has written — which is the
    point. Every method appends; nothing amends.
    """

    task_id: str
    task_type: str
    started: datetime
    root: Location
    store: Store
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    #: Every file written, in the order it was written. A record for the caller,
    #: never read back to decide anything.
    written: list[Location] = field(default_factory=list)

    @property
    def partition(self) -> str:
        return f"{DATE_PARTITION}={self.started.date().isoformat()}"

    def write_plan(self, plan: Mapping[str, Any]) -> Location:
        """Write the complete intended task, once, before execution begins."""

        return self._write(PLAN_FILE, {**plan, **self._identity()})

    def write_step(self, step_type: str, result: Mapping[str, Any]) -> Location:
        """Write one executed step's immutable result.

        ``step_type`` is the broad kind, and it is in the *filename* so the folder
        reads as a sequence without opening anything. The exact identity — which
        object, in which target, through which primitive — is in the JSON, where
        it can be as precise as it needs to be.
        """

        name = (
            f"{self._stamp()}_{_slug(step_type)}_{uuid.uuid4().hex}.json"
        )
        return self._write(name, {**result, **self._identity(), "step_type": step_type})

    def write_completion(self, summary: Mapping[str, Any]) -> Location:
        """Write the one file whose presence means the task finished normally."""

        name = f"{self._stamp()}_{COMPLETE_STEP}_{self.task_id}.json"
        return self._write(
            name,
            {
                **summary,
                **self._identity(),
                "started_at": _iso(self.started),
                "ended_at": _iso(self.clock()),
            },
        )

    # --- writing --------------------------------------------------------------

    def _identity(self) -> dict[str, str]:
        return {"task_id": self.task_id, "task_type": self.task_type}

    def _stamp(self) -> str:
        return _stamp(self.clock())

    def _write(self, name: str, payload: Mapping[str, Any]) -> Location:
        location = self.root / name
        self.store.make_directory(self.root)
        self.store.write(
            location,
            json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8"),
        )
        self.written.append(location)
        return location


def open_task_log(
    *,
    task_type: str,
    folder: Location,
    store: Store,
    task_id: str | None = None,
    clock: Callable[[], datetime] | None = None,
) -> TaskLog:
    """Create one task's folder beneath the declared log folder.

    ``folder`` is where ``_.Log`` materialises — see :func:`log_folder`. Taking
    the location rather than resolving it means the logger can be tested against
    its folder abstraction, with no control plane anywhere near it.
    """

    if task_type not in TASK_TYPES:
        raise CommandError(
            f"{task_type!r} is not a Weaver task type; expected one of "
            + ", ".join(TASK_TYPES)
        )
    clock = clock or (lambda: datetime.now(timezone.utc))
    started = clock()
    task_id = task_id or uuid.uuid4().hex
    partition = f"{DATE_PARTITION}={started.date().isoformat()}"
    name = f"{_stamp(started)}_{_slug(task_type)}_{task_id}"
    root = folder / partition / name
    store.make_directory(root)
    return TaskLog(
        task_id=task_id,
        task_type=task_type,
        started=started,
        root=root,
        store=store,
        clock=clock,
    )


def _stamp(moment: datetime) -> str:
    """``20260803T091522.123456Z`` — sortable, and unambiguous about its zone."""

    return moment.astimezone(timezone.utc).strftime(_TIMESTAMP) + "Z"


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def _slug(value: str) -> str:
    return str(value).replace("/", "-").replace(" ", "-")


__all__ = [
    "COMPLETE_STEP",
    "DATE_PARTITION",
    "LOG_FOLDER_ID",
    "PLAN_FILE",
    "TASK_TYPES",
    "TaskLog",
    "log_folder",
    "open_task_log",
]
