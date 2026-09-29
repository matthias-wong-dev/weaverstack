"""OneLake DFS storage for desktop access to Fabric Lakehouses."""

from __future__ import annotations

import uuid
import zlib
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import quote, unquote, urlencode, urlsplit

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError, StoreNotFoundError
from .auth import STORAGE_SCOPE, token_source
from .client import ONELAKE_DFS, _response_message

STORAGE_API_VERSION = "2023-11-03"
DEFAULT_TIMEOUT = 120.0


def lakehouse_artifact_segment(item: str) -> str:
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
    parts = [workspace, lakehouse_artifact_segment(item)]
    parts.extend(part for part in relative_path.strip("/").split("/") if part)
    url = f"{base_url.rstrip('/')}/" + "/".join(quote(part, safe="") for part in parts)
    return f"{url}?{urlencode(query)}" if query else url


def abfss_root(workspace_id: str, item_id: str) -> str:
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{item_id}"


@dataclass(frozen=True)
class OneLakePath:
    workspace: str
    item: str
    relative: str


@dataclass(frozen=True)
class ViewFileSnapshot:
    content: bytes
    content_type: str
    content_encoding: str
    properties: str
    content_length: int
    etag: str


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
    return OneLakePath(workspace=parts[0], item=parts[1], relative="/".join(parts[2:]))


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
                response = requests.request(
                    method, url, headers=merged, data=data, timeout=self.timeout
                )
            except requests.exceptions.RequestException as exc:
                raise StoreError(
                    f"{method} {url.split('?')[0]} could not be reached: {exc}",
                    executor="OneLake",
                ) from exc
            if response.status_code not in expected:
                raise StoreError(
                    f"{method} {url.split('?')[0]} returned {response.status_code}: "
                    f"{_response_message(response)}",
                    executor="OneLake",
                )
            return response

    def _url(self, location: Location, query: dict[str, str] | None = None) -> str:
        parsed = parse_onelake(location, base_url=self.base_url)
        return onelake_url(
            parsed.workspace,
            parsed.item,
            parsed.relative,
            base_url=self.base_url,
            query=query,
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
        directory = "/".join(
            part
            for part in (lakehouse_artifact_segment(parsed.item), parsed.relative)
            if part
        )
        url = f"{self.base_url}/{quote(parsed.workspace, safe='')}?" + urlencode(
            {
                "resource": "filesystem",
                "recursive": "true" if recursive else "false",
                "directory": directory,
            }
        )
        response = self._request("GET", url, expected=(200, 404))
        if response.status_code == 404:
            raise StoreNotFoundError(
                f"cannot list a location that does not exist: {location}",
                executor="OneLake",
            )

        # Never return a partial listing: callers use it for destructive and
        # reconciliation operations.
        if response.headers.get("x-ms-continuation"):
            raise NotImplementedError("OneLake listing pagination is not implemented")

        entries: list[Entry] = []
        prefix = f"{lakehouse_artifact_segment(parsed.item)}/"
        for path in response.json().get("paths", []):
            name = path.get("name", "")
            relative = name[len(prefix) :] if name.startswith(prefix) else name
            entries.append(
                Entry(
                    location=Location(
                        f"{self.base_url}/{parsed.workspace}/"
                        f"{lakehouse_artifact_segment(parsed.item)}/{relative}"
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
        return entries

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
        if (origin.workspace, origin.item) != (target.workspace, target.item):
            raise StoreError("Delta publication must stay in the same Lakehouse")
        if not origin.relative.startswith("Files/") or not target.relative.startswith(
            "Tables/"
        ):
            raise StoreError("Delta publication needs Files stage and Tables target")
        source_path = "/".join(("", origin.workspace, origin.item, origin.relative))
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

    def read_view_file(self, location: Location) -> ViewFileSnapshot:
        """Read decoded View JSON and its stored-file headers together."""
        url = self._url(location)
        head = self._request("HEAD", url, expected=(200,))
        try:
            length = int(head.headers["Content-Length"])
            etag = head.headers["ETag"]
        except (KeyError, TypeError, ValueError) as exc:
            raise StoreError("View file has incomplete OneLake headers") from exc
        return ViewFileSnapshot(
            content=self.read(location),
            content_type=head.headers.get("Content-Type", ""),
            content_encoding=head.headers.get("Content-Encoding", ""),
            properties=head.headers.get("x-ms-properties", ""),
            content_length=length,
            etag=etag,
        )

    def publish_view_file(
        self, stage: Location, destination: Location, decoded: bytes, *, properties: str
    ) -> ViewFileSnapshot:
        """Verify a private deflate file, then publish it without overwriting a View."""
        stage = self._publication_location(stage)
        destination = self._publication_location(destination)
        source = parse_onelake(stage, base_url=self.base_url)
        target = parse_onelake(destination, base_url=self.base_url)
        if (
            (source.workspace, source.item) != (target.workspace, target.item)
            or not source.relative.startswith("Files/")
            or not target.relative.startswith("Tables/")
            or any(p in ("", ".", "..") for p in source.relative.split("/"))
            or any(p in ("", ".", "..") for p in target.relative.split("/"))
        ):
            raise StoreError("View publication needs a private Files stage and bound Tables target")
        if not isinstance(decoded, bytes) or not decoded or not properties:
            raise StoreError("View publication needs verified JSON and View properties")
        stage_url = self._url(stage)
        destination_url = self._url(destination)
        if self._request("HEAD", destination_url, expected=(200, 404)).status_code != 404:
            raise StoreError("View destination already exists")
        encoded = zlib.compress(decoded)
        created = False
        publishing = False
        try:
            self._request(
                "PUT", f"{stage_url}?resource=file",
                headers={
                    "If-None-Match": "*", "x-ms-properties": properties,
                    "x-ms-content-type": "application/json",
                    "x-ms-content-encoding": "deflate",
                },
                expected=(201,),
            )
            created = True
            self._request(
                "PATCH", f"{stage_url}?action=append&position=0", data=encoded,
                headers={"Content-Length": str(len(encoded))}, expected=(202,),
            )
            self._request(
                "PATCH", f"{stage_url}?action=flush&position={len(encoded)}", expected=(200,),
            )
            before = self._request("HEAD", stage_url, expected=(200,))
            self._request(
                "PATCH", f"{stage_url}?action=setProperties",
                headers={
                    "If-Match": before.headers["ETag"],
                    "x-ms-content-type": "application/json",
                    "x-ms-content-encoding": "deflate",
                    "x-ms-properties": properties,
                },
                expected=(200,),
            )
            snapshot = self.read_view_file(stage)
            self._verify_view_snapshot(snapshot, decoded, encoded, properties)
            if self._request("HEAD", destination_url, expected=(200, 404)).status_code != 404:
                raise StoreError("View destination already exists")
            source_path = "/".join(("", source.workspace, source.item, source.relative))
            publishing = True
            result = self._request(
                "PUT", destination_url,
                headers={
                    "x-ms-rename-source": quote(source_path, safe="/"),
                    "If-None-Match": "*", "x-ms-source-if-match": snapshot.etag,
                },
                expected=(201,),
            )
            if result.headers.get("x-ms-continuation"):
                raise StoreError("View publication returned an incomplete rename")
            final = self.read_view_file(destination)
            self._verify_view_snapshot(final, decoded, encoded, properties)
            if self._request("HEAD", stage_url, expected=(200, 404)).status_code != 404:
                raise StoreError("View stage remained after publication")
            return final
        except Exception as exc:
            if publishing:
                try:
                    final = self.read_view_file(destination)
                    self._verify_view_snapshot(final, decoded, encoded, properties)
                    if self._request("HEAD", stage_url, expected=(200, 404)).status_code == 404:
                        return final
                except Exception:
                    pass
                raise StoreError("View publication is uncertain; inspect its destination before retrying") from exc
            if created:
                try:
                    current = self._request("HEAD", stage_url, expected=(200, 404))
                    if current.status_code == 200:
                        self._request("DELETE", stage_url,
                                      headers={"If-Match": current.headers["ETag"]},
                                      expected=(200, 202, 204))
                except Exception as cleanup_error:
                    raise StoreError("View private stage cleanup failed; inspect it before retrying") from cleanup_error
            raise

    @staticmethod
    def _verify_view_snapshot(
        snapshot: ViewFileSnapshot, decoded: bytes, encoded: bytes, properties: str
    ) -> None:
        if (
            snapshot.content != decoded
            or snapshot.content_type != "application/json"
            or snapshot.content_encoding != "deflate"
            or snapshot.properties != properties
            or snapshot.content_length != len(encoded)
            or not snapshot.etag
        ):
            raise StoreError("View file bytes or headers differ from the private draft")

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
            onelake_url(
                address.username,
                segments[0],
                "/".join(segments[1:]),
                base_url=self.base_url,
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
