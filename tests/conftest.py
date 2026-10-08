"""Suite-wide fixtures."""
import os

import pytest

# settings that make memd send data to an outside provider: a developer's
# environment must never make a test do that (a test that wants one sets it)
_PROVIDER_ENV_PREFIXES = ("MEMD_EXTRACTION_", "MEMD_EMBEDDING_")
_PROVIDER_ENV = ("TYPESAFE_API_KEY",)


@pytest.fixture(autouse=True)
def _no_provider_settings_from_the_environment(monkeypatch):
    for k in list(os.environ):
        if k.startswith(_PROVIDER_ENV_PREFIXES) or k in _PROVIDER_ENV:
            monkeypatch.delenv(k)
