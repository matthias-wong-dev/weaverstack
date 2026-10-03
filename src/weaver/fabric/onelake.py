"""OneLake DFS storage for desktop access to Fabric Lakehouses."""

from __future__ import annotations

import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import quote, unquote, urlencode, urlsplit

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError, StoreNotFoundError, StoreOutcomeUnknown
from .auth import STORAGE_SCOPE, token_source
from .client import (
    ONELAKE_DFS,
    READ_METHODS,
    TRANSIENT_STATUSES,
    _response_message,
    never_sent,
    outcome_unknown,
    send_until_answered,
)

STORAGE_API_VERSION = "2023-11-03"
DEFAULT_TIMEOUT = 120.0


def lakehouse_artifact_segment(item: str) -> str:
    """The URL segment for a Fabric item name or ID; a name gains ``.Lakehouse``."""

    try:
        uuid.UUID(item)
        return item
    except ValueError:
        return f"{item}.Lakehouse"


def onelake_url(
    workspace: str,
    item: str,
    relative_path: str = "",
    *,
    base_url: str = ONELAKE_DFS,
    query: dict[str, str] | None = None,
) -> str:
    """Render a URL from a Fabric item name or ID."""

    return OneLakePath(workspace, lakehouse_artifact_segment(item), relative_path).url(
        base_url=base_url, query=query
    )


def abfss_root(workspace_id: str, item_id: str) -> str:
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{item_id}"


def abfss_path(location: Location) -> str:
    """A bound OneLake location as Spark and NotebookUtils in Fabric address it."""

    from urllib.parse import urlsplit

    value = location.value
    if not value.startswith("https://"):
        return value
    address = urlsplit(value)
    parts = address.path.strip("/").split("/", 1)
    if (
        address.hostname != "onelake.dfs.fabric.microsoft.com"
        or len(parts) != 2
        or address.query
        or address.fragment
    ):
        raise CommandError(f"{value!r} is not a bound OneLake location.")
    return "abfss://" + parts[0] + "@" + address.hostname + "/" + parts[1]


@dataclass(frozen=True)
class OneLakePath:
    """A OneLake location's parts. ``segment`` is the item as the URL spells it."""

    workspace: str
    segment: str
    relative: str

    def url(
        self, *, base_url: str = ONELAKE_DFS, query: dict[str, str] | None = None
    ) -> str:
        parts = [self.workspace, self.segment]
        parts.extend(part for part in self.relative.strip("/").split("/") if part)
        url = f"{base_url.rstrip('/')}/" + "/".join(
            quote(part, safe="") for part in parts
        )
        return f"{url}?{urlencode(query)}" if query else url


def parse_onelake(location: Location, *, base_url: str = ONELAKE_DFS) -> OneLakePath:
    prefix = base_url.rstrip("/") + "/"
    if not location.value.startswith(prefix):
        raise CommandError(
            f"{location.value!r} is not a OneLake location. Expected it to start "
            f"with {prefix}"
        )
    parts = [unquote(part) for part in location.value[len(prefix) :].split("/") if part]
    if len(parts) < 2:
        raise CommandError(f"{location.value!r} names no item beneath its workspace")
    return OneLakePath(
        workspace=parts[0], segment=parts[1], relative="/".join(parts[2:])
    )


class OneLakeDfsClient:
    """An ADLS Gen2 DFS client explicitly constructed outside Fabric.

    Inside Fabric, storage uses the NotebookUtils-backed ``FabricStore``.
    """

    def __init__(
        self,
        *,
        base_url: str = ONELAKE_DFS,
        token: str | Callable[[], str] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        telemetry=None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.telemetry = telemetry
        self._token_source = token_source(token, scope=STORAGE_SCOPE)

    @property
    def token(self) -> str:
        """A currently-valid bearer, renewed when it is close to expiring.

        A push or a repository upload can run for a long time on one client, so
        the token has to be read per request rather than snapshotted.
        """

        return self._token_source()

    def _request(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        expected: tuple[int, ...] = (200, 201, 202),
    ):
        import requests

        merged = {
            "Authorization": f"Bearer {self.token}",
            "x-ms-version": STORAGE_API_VERSION,
        }
        merged.update(headers or {})
        observation = (
            self.telemetry.external("onelake", method.lower())
            if self.telemetry is not None
            else nullcontext()
        )
        with observation:
            try:
                response = send_until_answered(
                    method,
                    url,
                    expected=expected,
                    # A mutation refused with a transient status may still have
                    # been acted on, so only a read repeats one.
                    retry_transient=method in READ_METHODS,
                    headers=merged,
                    data=data,
                    timeout=self.timeout,
                )
            except requests.exceptions.RequestException as exc:
                error = StoreError if never_sent(exc) else StoreOutcomeUnknown
                raise error(
                    f"{method} {url.split('?')[0]} could not be reached: {exc}",
                    executor="OneLake",
                ) from exc
            if response.status_code not in expected:
                # A mutation refused with a transient status may have been acted on.
                unknown = outcome_unknown(method, response.status_code) or (
                    method not in READ_METHODS
                    and response.status_code in TRANSIENT_STATUSES
                )
                raise (StoreOutcomeUnknown if unknown else StoreError)(
                    f"{method} {url.split('?')[0]} returned {response.status_code}: "
                    f"{_response_message(response)}",
                    executor="OneLake",
                )
            return response

    def _url(self, location: Location, query: dict[str, str] | None = None) -> str:
        return parse_onelake(location, base_url=self.base_url).url(
            base_url=self.base_url, query=query
        )

    def exists(self, location: Location) -> bool:
        return (
            self._request("HEAD", self._url(location), expected=(200, 404)).status_code
            == 200
        )

    def is_directory(self, location: Location) -> bool:
        response = self._request("HEAD", self._url(location), expected=(200, 404))
        if response.status_code != 200:
            return False
        return response.headers.get("x-ms-resource-type") == "directory"

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]:
        parsed = parse_onelake(location, base_url=self.base_url)
        directory = "/".join(part for part in (parsed.segment, parsed.relative) if part)
        query = {
            "resource": "filesystem",
            "recursive": "true" if recursive else "false",
            "directory": directory,
        }
        entries: list[Entry] = []
        prefix = f"{parsed.segment}/"
        while True:
            url = f"{self.base_url}/{quote(parsed.workspace, safe='')}?" + urlencode(
                query
            )
            response = self._request("GET", url, expected=(200, 404))
            if response.status_code == 404:
                raise StoreNotFoundError(
                    f"cannot list a location that does not exist: {location}",
                    executor="OneLake",
                )
            for path in response.json().get("paths", []):
                name = path.get("name", "")
                relative = name[len(prefix) :] if name.startswith(prefix) else name
                entries.append(
                    Entry(
                        location=Location(
                            f"{self.base_url}/{parsed.workspace}/"
                            f"{parsed.segment}/{relative}"
                        ),
                        is_directory=str(path.get("isDirectory", "false")).lower()
                        == "true",
                        size=int(path["contentLength"])
                        if path.get("contentLength")
                        else None,
                        modified=_parse_time(path.get("lastModified")),
                        etag=path.get("etag"),
                    )
                )
            continuation = response.headers.get("x-ms-continuation")
            if not continuation:
                return entries
            query["continuation"] = continuation

    def read(self, location: Location) -> bytes:
        return self._request("GET", self._url(location), expected=(200,)).content

    def write(self, location: Location, data: bytes) -> None:
        url = self._url(location)
        self._request("PUT", f"{url}?resource=file", expected=(201,))
        if data:
            self._request(
                "PATCH",
                f"{url}?action=append&position=0",
                data=data,
                headers={"Content-Length": str(len(data))},
                expected=(202,),
            )
        self._request(
            "PATCH", f"{url}?action=flush&position={len(data)}", expected=(200,)
        )

    def delete(self, location: Location, *, recursive: bool = False) -> None:
        query = "?recursive=true" if recursive else ""
        self._request(
            "DELETE", f"{self._url(location)}{query}", expected=(200, 202, 204, 404)
        )

    def make_directory(self, location: Location) -> None:
        self._request(
            "PUT", f"{self._url(location)}?resource=directory", expected=(201, 409)
        )

    def rename_directory(self, source: Location, destination: Location) -> None:
        """Publish one validated private directory without replacing its target."""
        source = self._publication_location(source)
        destination = self._publication_location(destination)
        origin = parse_onelake(source, base_url=self.base_url)
        target = parse_onelake(destination, base_url=self.base_url)
        if (origin.workspace, origin.segment) != (target.workspace, target.segment):
            raise StoreError("Delta publication must stay in the same Lakehouse")
        if not origin.relative.startswith("Files/") or not target.relative.startswith(
            "Tables/"
        ):
            raise StoreError("Delta publication needs Files stage and Tables target")
        source_path = "/".join(("", origin.workspace, origin.segment, origin.relative))
        response = self._request(
            "PUT",
            self._url(destination),
            headers={
                "x-ms-rename-source": quote(source_path, safe="/"),
                "If-None-Match": "*",
            },
            expected=(201,),
        )
        if response.headers.get("x-ms-continuation"):
            raise StoreError("Delta publication returned an incomplete rename")

    def _publication_location(self, location: Location) -> Location:
        if not location.value.startswith("abfss://"):
            return location
        address = urlsplit(location.value)
        segments = address.path.strip("/").split("/")
        if (
            address.hostname != "onelake.dfs.fabric.microsoft.com"
            or not address.username
            or address.password
            or address.port
            or address.query
            or address.fragment
            or len(segments) < 3
            or any(segment in ("", ".", "..") for segment in segments)
        ):
            raise StoreError("Delta publication needs a bound OneLake path")
        return Location(
            OneLakePath(address.username, segments[0], "/".join(segments[1:])).url(
                base_url=self.base_url
            )
        )


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
