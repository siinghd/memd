"""Pass 48: an S3 or KMS error without a parsed reply is raised as itself.

Every place the S3 store (and the aws-kms key provider) read an error code
did `getattr(ex, "response", {}).get(...)`. A transport error - botocore's
ReadTimeoutError, a connection reset - carries no parsed reply, and one
whose `response` is None made that line raise AttributeError: the caller
saw "'NoneType' object has no attribute 'get'" instead of the timeout, and
the aws-kms provider raised it instead of KeyUnavailableError. An error
without a reply now reads as having no code, and the real error is raised.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.storage.crypto import AwsKmsProvider, KeyUnavailableError, WrappedKey  # noqa: E402
from memd.storage.s3store import S3ObjectStore  # noqa: E402


class _ReadTimeout(Exception):
    """What a transport error looks like: no parsed reply."""

    response = None


class _NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}}


class _Client:
    def __init__(self, ex: Exception):
        self.ex = ex

    def __getattr__(self, name):
        def call(**kw):
            raise self.ex
        return call


def _store(ex: Exception) -> S3ObjectStore:
    """A store whose every client call raises `ex` (nothing else is needed
    by the raw calls exercised here)."""
    st = object.__new__(S3ObjectStore)
    st._client = st._ctl = _Client(ex)
    st.bucket = "b"
    return st


@pytest.mark.parametrize("ex", [_ReadTimeout("Read timeout on endpoint URL"),
                                RuntimeError("no response attribute at all")])
def test_an_error_without_a_reply_has_no_code(ex):
    assert S3ObjectStore._code(ex) == ""
    assert _store(ex)._is_missing(ex) is False


def test_an_error_with_a_reply_keeps_its_code():
    ex = _NoSuchKey()
    assert S3ObjectStore._code(ex) == "NoSuchKey"
    assert _store(ex)._is_missing(ex) is True


@pytest.mark.parametrize("call", [
    lambda st: st._raw_get_meta("ns/a/manifest.json"),
    lambda st: st._raw_head("ns/a/manifest.json"),
    lambda st: st._raw_cas_put("ns/a/manifest.json", b"x", if_match='"etag"'),
    lambda st: st._raw_cas_put("ns/a/manifest.json", b"x", if_none_match=True),
], ids=["get", "head", "cas-if-match", "cas-if-none-match"])
def test_a_timeout_is_raised_as_itself(call):
    ex = _ReadTimeout("Read timeout on endpoint URL")
    with pytest.raises(_ReadTimeout):
        call(_store(ex))


def test_a_missing_object_still_reads_as_absent():
    st = _store(_NoSuchKey())
    assert st._raw_get_meta("ns/a/manifest.json") is None
    assert st._raw_head("ns/a/manifest.json") is None


def test_a_kms_timeout_is_a_key_unavailable_error():
    kms = AwsKmsProvider("arn:aws:kms:eu-west-1:1:key/k", client=_Client(_ReadTimeout("timed out")))
    with pytest.raises(KeyUnavailableError, match="_ReadTimeout"):
        kms.unwrap("n", WrappedKey("aws-kms", "k", "", b"ct"))
    with pytest.raises(KeyUnavailableError):
        kms.generate("n")
