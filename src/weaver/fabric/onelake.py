"""OneLake DFS storage for desktop access to Fabric Lakehouses."""

from __future__ import annotations

import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError
from .auth import STORAGE_SCOPE, token_source
from .client import ONELAKE_DFS

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


def parse_onelake(location: Location, *, base_url: str = ONELAKE_DFS) -> OneLakePath:
    prefix = base_url.rstrip("/") + "/"
    if not location.value.startswith(prefix):
        raise CommandError(
            f"{location.value!r} is not a OneLake location. Expected it to start "
            f"with {prefix}"
        )
    parts = [part for part in location.value[len(prefix) :].split("/") if part]
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
        token: str | None = None,
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
            response = requests.request(
                method, url, headers=merged, data=data, timeout=self.timeout
            )
            if response.status_code not in expected:
                raise StoreError(
                    f"{method} {url.split('?')[0]} returned {response.status_code}: "
                    f"{response.text.strip()[:300] or 'no body'}"
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
            raise StoreError(f"cannot list a location that does not exist: {location}")

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


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
