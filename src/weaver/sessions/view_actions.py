"""Session-owned orchestration for native and directly published Spark Views."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Sequence

from ..locations import Location
from .direct_view import (
    VIEW_PROPERTIES,
    bound_view_paths,
    compile_view_metadata,
    load_native_view_template,
    parse_build_view_statement,
)


@dataclass(frozen=True)
class PreparedView:
    label: str
    statement: str
    query: str
    stage: Location
    destination: Location
    schema: str

    @property
    def key(self) -> tuple[str, str]:
        return self.stage.value.rsplit("/Files/", 1)[0], self.schema


def create_view_actions(
    session, actions: Sequence[tuple[str, str]], *, workspace
) -> list[dict[str, Any]]:
    """Capture a live View template, then analyse and publish independent siblings."""
    ordered = list(actions)
    if not ordered:
        return []
    origin = time.monotonic()
    scope = session.scope(workspace)
    outcomes: list[dict[str, Any]] = []

    def failed(label, error, started, route):
        return {
            "label": label,
            "succeeded": False,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "started_after_seconds": started - origin,
            "duration_seconds": time.monotonic() - started,
            "view_route": route,
        }

    def spark(group: Sequence[PreparedView], route: str) -> list[dict[str, Any]]:
        submitted = time.monotonic()
        pairs = [(item.label, item.statement) for item in group]
        try:
            results = session.execute_spark_sql_actions(
                pairs, exact_case=True, workspace=workspace
            )
            if (
                not isinstance(results, list)
                or [result.get("label") for result in results]
                != [label for label, _ in pairs]
                or any(
                    type(result.get("succeeded")) is not bool
                    or not isinstance(result.get("started_after_seconds"), (int, float))
                    or not math.isfinite(result["started_after_seconds"])
                    or result["started_after_seconds"] < 0
                    or not isinstance(result.get("duration_seconds"), (int, float))
                    or not math.isfinite(result["duration_seconds"])
                    or result["duration_seconds"] < 0
                    for result in results
                )
            ):
                raise ValueError(
                    "View Spark outcomes did not match the submitted actions"
                )
            return [
                {
                    **result,
                    "started_after_seconds": submitted
                    - origin
                    + result["started_after_seconds"],
                    "view_route": route,
                }
                for result in results
            ]
        except Exception as exc:
            return [failed(label, exc, submitted, route) for label, _ in pairs]

    index = 0
    while index < len(ordered):
        started = time.monotonic()
        label, statement = ordered[index]
        try:
            qualified, query = parse_build_view_statement(statement)
            stage, destination, schema = bound_view_paths(scope.resolver, qualified)
        except Exception as exc:
            outcomes.append(failed(label, exc, started, "invalid_target"))
            index += 1
            continue
        first = PreparedView(label, statement, query, stage, destination, schema)
        group = [first]
        index += 1
        while index < len(ordered):
            next_label, next_statement = ordered[index]
            try:
                next_qualified, next_query = parse_build_view_statement(next_statement)
                next_stage, next_destination, next_schema = bound_view_paths(
                    scope.resolver, next_qualified
                )
            except Exception:
                break
            next_item = PreparedView(
                next_label,
                next_statement,
                next_query,
                next_stage,
                next_destination,
                next_schema,
            )
            if next_item.key != first.key:
                break
            group.append(next_item)
            index += 1

        livy = session._foreground_livy(scope)
        saved = session._view_templates.get(first.key)
        if saved is None or saved[0] is not livy:
            seed = group.pop(0)
            (created,) = spark([seed], "spark_template")
            outcomes.append(created)
            template = None
            if created["succeeded"]:
                try:
                    native = scope.transport_store.read_view_file(seed.destination)
                    template = load_native_view_template(
                        native.content,
                        content_type=native.content_type,
                        content_encoding=native.content_encoding,
                        properties=native.properties,
                    )
                except Exception:
                    pass
                session._view_templates[first.key] = (livy, template)
        else:
            template = saved[1]
        if not group:
            continue
        if template is None:
            outcomes.extend(spark(group, "spark_fallback"))
            continue

        try:
            shapes = session.describe_spark_view_queries(
                [(item.label, item.query) for item in group], workspace=workspace
            )
            if (
                not isinstance(shapes, list)
                or [shape.get("label") for shape in shapes]
                != [item.label for item in group]
                or any(type(shape.get("succeeded")) is not bool for shape in shapes)
            ):
                raise ValueError(
                    "View output shapes did not match the submitted actions"
                )
        except Exception:
            outcomes.extend(spark(group, "spark_fallback"))
            continue
        for item, shape in zip(group, shapes, strict=True):
            action_started = time.monotonic()
            if not shape["succeeded"]:
                outcomes.extend(spark([item], "spark_fallback"))
                continue
            try:
                decoded = compile_view_metadata(
                    template,
                    shape.get("schema"),
                    item.query,
                    now_ms=int(time.time() * 1000),
                )
            except ValueError:
                outcomes.extend(spark([item], "spark_fallback"))
                continue
            try:
                with session.telemetry.timing("onelake.view"):
                    scope.transport_store.publish_view_file(
                        item.stage,
                        item.destination,
                        decoded,
                        properties=VIEW_PROPERTIES,
                    )
            except Exception as exc:
                outcomes.append(failed(item.label, exc, action_started, "direct"))
            else:
                outcomes.append(
                    {
                        "label": item.label,
                        "succeeded": True,
                        "started_after_seconds": action_started - origin,
                        "duration_seconds": time.monotonic() - action_started,
                        "view_route": "direct",
                    }
                )
    return outcomes
