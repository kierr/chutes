import asyncio
import io
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

# Ensure tests import the local workspace package instead of a globally installed one.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import chutes.entrypoint.warmup as warmup


class FakeContent:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_any(self):
        for chunk in self._chunks:
            yield chunk


class FakeResponse:
    def __init__(self, status=200, json_data=None, text_data="", chunks=None):
        self.status = status
        self._json_data = json_data or {}
        self._text_data = text_data
        self.content = FakeContent(chunks or [])
        self.request_info = None
        self.history = ()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._json_data

    async def text(self):
        return self._text_data


class FakeSession:
    def __init__(self, responses, captured_kwargs=None, **kwargs):
        self._responses = responses
        if captured_kwargs is not None:
            captured_kwargs.append(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *args, **kwargs):
        if not self._responses:
            raise AssertionError("No fake responses queued")
        return self._responses.pop(0)


def _dummy_config():
    return SimpleNamespace(generic=SimpleNamespace(api_base_url="https://example.com"))


@pytest.mark.asyncio
async def test_poll_for_instances_fails_fast_on_unauthorized(monkeypatch):
    responses = [FakeResponse(status=401, text_data="unauthorized")]

    monkeypatch.setattr(warmup, "sign_request", lambda purpose=None: ({}, None))
    monkeypatch.setattr(
        warmup.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: FakeSession(responses, **kwargs),
    )

    with pytest.raises(PermissionError, match="status=401"):
        await warmup.poll_for_instances("mychute", _dummy_config(), poll_interval=0, max_wait=1)


@pytest.mark.asyncio
async def test_poll_for_instances_resigns_each_attempt(monkeypatch):
    responses = [
        FakeResponse(status=500, text_data="server error"),
        FakeResponse(status=200, json_data={"instances": [{"instance_id": "inst-1"}]}),
    ]
    sign_calls = {"count": 0}

    def fake_sign_request(purpose=None):
        sign_calls["count"] += 1
        return {"X-Signature-Nonce": str(sign_calls["count"])}, None

    monkeypatch.setattr(warmup, "sign_request", fake_sign_request)
    monkeypatch.setattr(
        warmup.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: FakeSession(responses, **kwargs),
    )

    instances = await warmup.poll_for_instances("mychute", _dummy_config(), poll_interval=0, max_wait=1)
    assert instances[0]["instance_id"] == "inst-1"
    assert sign_calls["count"] == 2


@pytest.mark.asyncio
async def test_stream_instance_logs_keeps_single_character_lines(monkeypatch):
    responses = [
        FakeResponse(
            status=200,
            chunks=[
                b"data: {\"log\":\"x\"}\n\n",
                b"data: {\"log\":\"ready\"}\n\n",
            ],
        )
    ]
    session_kwargs = []

    monkeypatch.setattr(warmup, "sign_request", lambda purpose=None: ({}, None))
    monkeypatch.setattr(
        warmup.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: FakeSession(responses, captured_kwargs=session_kwargs, **kwargs),
    )

    class DummyStdout:
        def __init__(self):
            self.buffer = io.BytesIO()

    stdout = DummyStdout()
    monkeypatch.setattr(warmup.sys, "stdout", stdout)

    await warmup.stream_instance_logs("inst-1", _dummy_config(), backfill=1)

    assert stdout.buffer.getvalue() == b"x\nready\n"
    assert session_kwargs[0]["timeout"].total is None


@pytest.mark.asyncio
async def test_monitor_warmup_raises_if_stream_ends_before_hot(monkeypatch):
    responses = [
        FakeResponse(
            status=200,
            chunks=[b"data: {\"status\":\"warming\",\"log\":\"booting\"}\n\n"],
        )
    ]

    monkeypatch.setattr(
        warmup.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: FakeSession(responses, **kwargs),
    )

    with pytest.raises(RuntimeError, match="reached hot status"):
        await warmup.monitor_warmup("mychute", _dummy_config(), headers={})


def test_warmup_chute_propagates_monitor_failure_and_cancels_poll_task(monkeypatch):
    state = {"cancelled": False}

    async def fake_monitor(*args, **kwargs):
        raise RuntimeError("warmup failed")

    async def fake_poll(*args, **kwargs):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    monkeypatch.setattr(warmup, "monitor_warmup", fake_monitor)
    monkeypatch.setattr(warmup, "poll_and_stream_logs", fake_poll)
    monkeypatch.setattr(warmup, "get_config", _dummy_config)
    monkeypatch.setattr(warmup, "sign_request", lambda purpose=None: ({}, None))

    with pytest.raises(RuntimeError, match="warmup failed"):
        warmup.warmup_chute("mychute", config_path=None, debug=False, stream_logs=True)

    assert state["cancelled"] is True
