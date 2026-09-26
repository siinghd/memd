"""Hosted mode: tenancy (org -> namespaces -> API keys), usage metering,
entitlements and Stripe billing.

OFF by default. Only `memd serve --http --hosted` (or MEMD_HOSTED=1) loads
this package; embedded and self-hosted memd never import it, and nothing here
imports `stripe` at module level (it is the optional `memd[billing]` extra,
imported lazily by memd.hosted.billing when a Stripe key is configured).
"""
from __future__ import annotations

import os

_TRUE = ("1", "true", "yes", "on")


def hosted_enabled(flag: bool | None = None) -> bool:
    """An explicit flag wins; otherwise MEMD_HOSTED decides (default off)."""
    if flag is not None:
        return bool(flag)
    return os.environ.get("MEMD_HOSTED", "").strip().lower() in _TRUE
