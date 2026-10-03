"""Refresh evidence without table row counts or source-window bookmarks."""

from dataclasses import asdict, dataclass, fields


@dataclass(frozen=True)
class SemanticRefreshResult:
    status: str | None
    request_id: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    error_message: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "Completed" and self.error_message is None

    @classmethod
    def from_response(cls, body, *, error_message=None):
        return cls(
            status=body.get("status"),
            request_id=body.get("request_id"),
            start_time=body.get("startTime"),
            end_time=body.get("endTime"),
            error_message=error_message,
        )

    def as_row(self) -> dict:
        return {**asdict(self), "succeeded": self.succeeded}

    @classmethod
    def from_row(cls, row):
        return cls(**{field.name: row.get(field.name) for field in fields(cls)})
