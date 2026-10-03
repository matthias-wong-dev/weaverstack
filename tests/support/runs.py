"""Doubles for the seams a run dispatches through."""

from __future__ import annotations


def together(dispatch):
    """Answer Python nodes dispatched together the way ``dispatch`` answers one."""

    def dispatch_many(nodes, **asked):
        asked.pop("resolved", None)
        outcomes = []
        for node in nodes:
            try:
                outcomes.append(dispatch(node, **asked))
            except Exception as exc:  # noqa: BLE001 - the node's own outcome
                outcomes.append(exc)
        return outcomes

    return dispatch_many
