"""Compatibility admission for a published Weaver Build runtime."""

from __future__ import annotations

from importlib import metadata

from ..errors import BuildError

PROTOCOL = 1


def identity() -> dict:
    from ..fabric.environment import local_weaver
    from ..mutation.bundle import SUPPORTED_FORMAT_VERSION
    from .archive_runtime import mutation_drivers

    try:
        writer = metadata.version("deltalake")
    except metadata.PackageNotFoundError:
        writer = None
    return {
        "protocol": PROTOCOL,
        "format_version": SUPPORTED_FORMAT_VERSION,
        "version": local_weaver().version,
        "deltalake": writer,
        "executors": sorted(mutation_drivers({})),
    }


def require_compatible(observed, *, expected=None, plan=None) -> dict:
    from .delta_profile import WRITER_VERSION

    expected = identity() if expected is None else expected
    reason = None
    if not isinstance(observed, dict):
        reason = "is missing"
    elif (
        type(observed.get("protocol")) is not int
        or observed["protocol"] != expected["protocol"]
    ):
        reason = "uses an incompatible execution protocol"
    elif (
        type(observed.get("format_version")) is not int
        or observed["format_version"] != expected["format_version"]
    ):
        reason = "cannot read this sealed plan format"
    elif observed.get("version") != expected["version"]:
        reason = f"has version {observed.get('version')!r}; this Build requires {expected['version']!r}"
    elif observed.get("deltalake") != WRITER_VERSION:
        reason = f"requires deltalake {WRITER_VERSION}"
    else:
        executors = observed.get("executors")
        required = (
            set(expected["executors"])
            if plan is None
            else (
                {c.executor for c in plan.driver_contracts}
                | {a.executor for _, _, a in plan.actions()}
            )
            - {"completion_gate"}
        )
        if (
            not isinstance(executors, list)
            or not all(isinstance(name, str) for name in executors)
            or not required <= set(executors)
        ):
            reason = "lacks this plan's executor capabilities"
    if reason:
        raise BuildError(
            f"The published Weaver Build runtime {reason}. Publish the target Environment, then build again."
        )
    return observed


def probe_source() -> str:
    return """try:
    from weaver.sessions.hosted_runtime import identity as _weaver_build_identity
    _weaver_build_runtime = _weaver_build_identity()
except Exception as _weaver_build_error:
    emit({"runtime": None, "error": str(_weaver_build_error)})
else:
    emit({"runtime": _weaver_build_runtime})
"""
