"""OneLake as a :class:`~weaver.store.Store`.

OneLake speaks the ADLS Gen2 DFS API over plain HTTPS, so the same code reaches
it from a laptop and from inside a Fabric session. That is what keeps a desktop
push and folder work inside Fabric one implementation rather than two that
drift.

Two things OneLake does not do, both learned the hard way:

**No directory rename.** ``PUT ?resource=directory`` with ``x-ms-rename-source``
returns ``400 UnsupportedHeader``. :meth:`FabricStore.move_within_store` is
still one operation — the intent has to survive the call — but here it copies
and deletes. Inside a Fabric session ``notebookutils.fs.mv`` can do better, and
that is the implementation's business, not the caller's.

**Writing is three calls, not one.** Create the file, append the bytes, flush at
the final offset.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timezone
from datetime import datetime
from urllib.parse import quote, urlencode

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError
from .auth import STORAGE_SCOPE, get_token
from .client import ONELAKE_DFS

STORAGE_API_VERSION = "2023-11-03"
DEFAULT_TIMEOUT = 120.0


def artifact_segment(item: str) -> str:
    """A OneLake path segment for an item, by id or by name.

    A GUID stands alone; a name needs its item type, as ``Weaver.Lakehouse``.
    """

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
    """A DFS URL beneath one item, e.g. ``…/{ws}/{lh}/Files/repos/x``."""

    parts = [workspace, artifact_segment(item)]
    parts.extend(part for part in relative_path.strip("/").split("/") if part)
    url = f"{base_url.rstrip('/')}/" + "/".join(quote(part, safe="") for part in parts)
    return f"{url}?{urlencode(query)}" if query else url


def abfss_root(workspace_id: str, item_id: str) -> str:
    """The Spark-facing root for an item.

    Proven to list, read and write Lakehouses that are not attached to the
    notebook, which is the whole reason destination roots are explicit.
    """

    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{item_id}"


@dataclass(frozen=True)
class OneLakePath:
    """A OneLake location split back into the parts DFS needs."""

    workspace: str
    item: str
    relative: str


def parse_onelake(location: Location, *, base_url: str = ONELAKE_DFS) -> OneLakePath:
    prefix = base_url.rstrip("/") + "/"
    if not location.value.startswith(prefix):
        raise CommandError(
            f"{location.value!r} is not a OneLake location — expected it to start "
            f"with {prefix}"
        )
    parts = [part for part in location.value[len(prefix):].split("/") if part]
    if len(parts) < 2:
        raise CommandError(f"{location.value!r} names no item beneath its workspace")
    return OneLakePath(workspace=parts[0], item=parts[1], relative="/".join(parts[2:]))


class FabricStore:
    """File transport over OneLake.

    Satisfies the same :class:`~weaver.store.Store` protocol as
    :class:`~weaver.store.LocalStore`, so everything above it is written once.
    """

    def __init__(
        self,
        *,
        base_url: str = ONELAKE_DFS,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._token = token

    @property
    def token(self) -> str:
        if self._token is None:
            self._token = get_token(STORAGE_SCOPE)
        return self._token

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

    # --- the Store protocol ----------------------------------------------

    def exists(self, location: Location) -> bool:
        return self._request("HEAD", self._url(location), expected=(200, 404)).status_code == 200

    def is_directory(self, location: Location) -> bool:
        response = self._request("HEAD", self._url(location), expected=(200, 404))
        if response.status_code != 200:
            return False
        return response.headers.get("x-ms-resource-type") == "directory"

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]:
        parsed = parse_onelake(location, base_url=self.base_url)
        directory = "/".join(
            part for part in (artifact_segment(parsed.item), parsed.relative) if part
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

        entries: list[Entry] = []
        prefix = f"{artifact_segment(parsed.item)}/"
        for path in response.json().get("paths", []):
            name = path.get("name", "")
            relative = name[len(prefix):] if name.startswith(prefix) else name
            entries.append(
                Entry(
                    location=Location(
                        f"{self.base_url}/{parsed.workspace}/"
                        f"{artifact_segment(parsed.item)}/{relative}"
                    ),
                    is_directory=str(path.get("isDirectory", "false")).lower() == "true",
                    size=int(path["contentLength"]) if path.get("contentLength") else None,
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
        self._request("PATCH", f"{url}?action=flush&position={len(data)}", expected=(200,))

    def delete(self, location: Location, *, recursive: bool = False) -> None:
        query = "?recursive=true" if recursive else ""
        self._request(
            "DELETE", f"{self._url(location)}{query}", expected=(200, 202, 204, 404)
        )

    def make_directory(self, location: Location) -> None:
        self._request(
            "PUT", f"{self._url(location)}?resource=directory", expected=(201, 409)
        )

    def move_within_store(self, source: Location, destination: Location) -> None:
        """Copy then delete.

        OneLake refuses ``x-ms-rename-source`` with ``400 UnsupportedHeader``, so
        a move here really does move bytes. The operation stays whole so a
        future implementation inside a Fabric session can do better.
        """

        if not self.exists(source):
            raise StoreError(f"cannot move a location that does not exist: {source}")

        if self.is_directory(source):
            self.make_directory(destination)
            prefix = source.value.rstrip("/") + "/"
            for entry in self.list(source, recursive=True):
                if entry.is_directory:
                    continue
                relative = entry.location.value[len(prefix):]
                self.write(
                    destination.join(*relative.split("/")), self.read(entry.location)
                )
            self.delete(source, recursive=True)
        else:
            self.write(destination, self.read(source))
            self.delete(source)


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
