"""Prove browser sign-in reaches Fabric, with the Azure CLI made unavailable.

Run by hand, like `provision_estate.py`. It is not a pytest test: the marker
vocabulary `@weaver_test` generates is closed, and the suite it belongs to must
never be able to open a browser and wait for somebody who is not there.

The Azure CLI is made unavailable inside this process alone, by replacing what
`AzureCliCredential.get_token` does. Nothing outside it is touched: no
`az logout`, no rename of `~/.azure`, no environment variable that outlives the
run. The developer stays signed in.

What it proves:

    the Azure CLI reports itself unavailable
        → the chain falls through to browser sign-in
            → a browser opens, and a person signs in
                → Fabric REST answers

Usage:
    python -m tests.fabric.interactive_signin [--workspace "My Fabric Workspace"]

The token is cached under `weaverstack`, so a second run signs in silently. Pass
`--forget` to remove that cache first and see the browser again.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weaver.fabric import auth
from weaver.operations.doctor import doctor


def _make_the_azure_cli_unavailable() -> None:
    """Inside this process only, and for this process's lifetime only."""

    from azure.identity import AzureCliCredential, CredentialUnavailableError

    def unavailable(self, *scopes, **kwargs):
        raise CredentialUnavailableError(
            "the Azure CLI is deliberately unavailable for this run"
        )

    AzureCliCredential.get_token = unavailable


def _forget_the_cached_token() -> None:
    """Drop the persisted token, so sign-in is interactive again.

    Where the cache lives is the platform's choice. Linux and Windows keep a
    file, and macOS keeps a Keychain item, which this does not touch: delete
    ``weaverstack`` from the login keychain by hand to see the browser there.
    """

    removed = []
    for path in (Path.home() / ".IdentityService" / auth.TOKEN_CACHE_NAME,):
        for candidate in (path, path.with_suffix(".bin")):
            if candidate.exists():
                candidate.unlink()
                removed.append(str(candidate))
    if removed:
        print("removed " + ", ".join(removed))
    elif sys.platform == "darwin":
        print(
            f"No cache file here. On macOS the token is a Keychain item named "
            f"{auth.TOKEN_CACHE_NAME!r}; remove it there to sign in again."
        )
    else:
        print("No cached token to remove.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace", help="Fabric workspace to resolve as well.")
    parser.add_argument(
        "--forget",
        action="store_true",
        help="Remove the cached token first, so a browser opens.",
    )
    args = parser.parse_args(argv)

    if args.forget:
        _forget_the_cached_token()

    _make_the_azure_cli_unavailable()
    auth._desktop_chain = None
    auth.use_credential(auth.desktop_credential())
    print("The Azure CLI is unavailable in this process. Signing in through a browser.")

    report = doctor(workspace=args.workspace)
    for check in report.checks:
        print(f"  {check.name:<34}{'OK' if check.passed else 'FAILED'}")
        if not check.passed:
            print(f"    {check.detail}")

    if not report.succeeded:
        print("\nBrowser sign-in did not reach Fabric.")
        return 1
    print("\nBrowser sign-in reached Fabric.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
