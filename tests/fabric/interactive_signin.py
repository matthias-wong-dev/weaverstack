"""Prove browser sign-in reaches Fabric, with the Azure CLI made unavailable.

Run by hand, like `provision_estate.py`. It is not a pytest test: the marker
vocabulary `@weaver_test` generates is closed, and the suite it belongs to must
never be able to open a browser and wait for somebody who is not there.

The Azure CLI is made unavailable inside this process alone, by replacing what
`AzureCliCredential.get_token` does. Nothing outside it is touched: no
`az logout`, no rename of `~/.azure`, no environment variable that outlives the
run. The developer stays signed in.

What it proves, in two processes:

    the Azure CLI reports itself unavailable
        → the chain falls through to browser sign-in
            → a browser opens, and a person signs in
                → Fabric REST answers, and the account is remembered

    a second process, the Azure CLI still unavailable
        → the remembered account and the secure token cache are read
            → Fabric REST answers, and nothing opens

Usage:

    python -m tests.fabric.interactive_signin --forget --workspace "..."
    python -m tests.fabric.interactive_signin --expect-cached --workspace "..."

Reuse needs both halves: the encrypted token cache under `weaverstack` holds the
refresh token, and `~/.weaver/authentication-record.json` names the account it
belongs to. `--forget` removes both. `--expect-cached` makes the interactive
sign-in fail on the spot, so the second run proves reuse rather than a browser
window nobody was watching.
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


def _forget_the_sign_in() -> None:
    """Drop both halves, so sign-in is interactive again.

    Where the token cache lives is the platform's choice. Linux and Windows keep
    a file, and macOS keeps a Keychain item, which this does not touch: delete
    ``weaverstack`` from the login keychain by hand to see the browser there.
    The remembered account is Weaver's own file and is removed here either way.
    """

    removed = []
    record = auth._authentication_record_path()
    if record.exists():
        record.unlink()
        removed.append(str(record))
    for path in (Path.home() / ".IdentityService" / auth.TOKEN_CACHE_NAME,):
        for candidate in (path, path.with_suffix(".bin")):
            if candidate.exists():
                candidate.unlink()
                removed.append(str(candidate))
    if removed:
        print("removed " + ", ".join(removed))
    if sys.platform == "darwin":
        print(
            f"On macOS the token is a Keychain item named "
            f"{auth.TOKEN_CACHE_NAME!r}; remove it there to sign in again."
        )
    elif not removed:
        print("Nothing to remove.")


def _refuse_interactive_sign_in() -> None:
    """Make a browser sign-in a failure rather than a window.

    What ``--expect-cached`` is for. Success then means the token cache and the
    remembered account reconstructed the sign-in, which a browser opening for
    somebody who is watching would otherwise hide.
    """

    from azure.identity import InteractiveBrowserCredential

    def refused(self, **kwargs):
        raise SystemExit(
            "A browser sign-in was requested, and --expect-cached forbids one.\n"
            "The token cache and the remembered account did not reconstruct the "
            "sign-in."
        )

    InteractiveBrowserCredential.authenticate = refused


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace", help="Fabric workspace to resolve as well.")
    parser.add_argument(
        "--forget",
        action="store_true",
        help="Remove the token cache and the remembered account, so a browser opens.",
    )
    parser.add_argument(
        "--expect-cached",
        action="store_true",
        help="Fail rather than open a browser, proving the sign-in was reused.",
    )
    args = parser.parse_args(argv)

    if args.forget:
        _forget_the_sign_in()

    _make_the_azure_cli_unavailable()
    if args.expect_cached:
        _refuse_interactive_sign_in()
    auth._desktop_chain = None
    auth.use_credential(auth.desktop_credential())
    if args.expect_cached:
        print(
            "The Azure CLI is unavailable in this process and a browser sign-in "
            "is forbidden. Reusing the remembered sign-in."
        )
    else:
        print(
            "The Azure CLI is unavailable in this process. "
            "Signing in through a browser."
        )

    report = doctor(workspace=args.workspace)
    for check in report.checks:
        print(f"  {check.name:<34}{'OK' if check.passed else 'FAILED'}")
        if not check.passed:
            print(f"    {check.detail}")

    if not report.succeeded:
        print("\nBrowser sign-in did not reach Fabric.")
        return 1
    if args.expect_cached:
        print("\nThe remembered sign-in reached Fabric, and nothing opened.")
        record = auth._authentication_record_path()
        print(f"  token cache   {auth.TOKEN_CACHE_NAME}")
        print(f"  account       {record}")
        return 0
    print("\nBrowser sign-in reached Fabric.")
    print(f"The account is remembered in {auth._authentication_record_path()}.")
    print("Run again with --expect-cached, in a new process, to prove the reuse.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
