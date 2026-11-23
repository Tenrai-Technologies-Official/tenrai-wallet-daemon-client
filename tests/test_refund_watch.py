"""Transport/serialization checks, not a substitute for real daemon lifecycle tests."""

import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiohttp import web
from fake_daemon import TOKEN, asynchronous

from tenrai_daemon import (
    CONNECTED,
    DaemonClient,
    DaemonError,
    ErrorCode,
    Event,
    EventStream,
    RefundWatchJob,
    RefundWatchOutpoint,
)

OUTPOINT: RefundWatchOutpoint = {
    "transaction_id": "ab" * 48,
    "output_index": (1 << 32) - 1,
}
FEE_CAP = str(1 << 100)
JOB: RefundWatchJob = {
    "job_id": "cd" * 48,
    "state": "public-state-from-daemon",
    "refund_address": "ttrai1refundowner",
    "refund_height": (1 << 64) - 1,
    "max_fee_atoms": FEE_CAP,
    "amount_atoms": str((1 << 128) - 1),
    "attempt": 0,
}


@contextlib.asynccontextmanager
async def endpoint(answer: Any, calls: list[dict[str, Any]], *, error: bool = False) -> AsyncIterator[str]:
    async def rpc(request: web.Request) -> web.Response:
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        body = await request.json()
        calls.append(body)
        return web.json_response({"jsonrpc": "2.0", "id": body["id"], "error" if error else "result": answer})

    app = web.Application()
    app.router.add_post("/v1/rpc", rpc)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


@asynchronous
async def test_registration_preserves_exact_outpoint_path_and_large_integer_fee_text():
    calls: list[dict[str, Any]] = []
    async with endpoint(JOB, calls) as url, DaemonClient(url, TOKEN) as client:
        job = await client.refund_watch_register(
            "0103" + "00" * 122,
            OUTPOINT,
            max_fee_atoms=FEE_CAP,
            key_branch="change",
            index=(1 << 31) - 1,
        )
    assert job == JOB
    assert calls[0]["method"] == "refund_watch_register"
    assert calls[0]["params"] == {
        "descriptor_hex": "0103" + "00" * 122,
        "outpoint": OUTPOINT,
        "key_branch": "change",
        "index": (1 << 31) - 1,
        "max_fee_atoms": FEE_CAP,
    }
    assert job["refund_height"] == (1 << 64) - 1
    assert "to" not in calls[0]["params"] and "preimage_hex" not in calls[0]["params"]


@asynchronous
async def test_registration_defaults_and_status_lifecycle_helpers_use_exact_job_id():
    calls: list[dict[str, Any]] = []
    async with endpoint(JOB, calls) as url, DaemonClient(url, TOKEN) as client:
        await client.refund_watch_register("public-descriptor", OUTPOINT, max_fee_atoms="1")
        await client.refund_watch_status(JOB["job_id"])
        await client.refund_watch_cancel(JOB["job_id"])
        await client.refund_watch_forget(JOB["job_id"])
    assert calls[0]["params"]["key_branch"] == "receive"
    assert calls[0]["params"]["index"] == 0
    assert [(call["method"], call["params"]) for call in calls[1:]] == [
        ("refund_watch_status", {"job_id": JOB["job_id"]}),
        ("refund_watch_cancel", {"job_id": JOB["job_id"]}),
        ("refund_watch_forget", {"job_id": JOB["job_id"]}),
    ]


@asynchronous
async def test_list_exposes_auto_refund_enablement_and_bounded_jobs():
    calls: list[dict[str, Any]] = []
    answer = {"jobs": [JOB], "limit": 64, "auto_refund_enabled": False}
    async with endpoint(answer, calls) as url, DaemonClient(url, TOKEN) as client:
        assert await client.refund_watch_list() == answer
    assert calls[0]["method"] == "refund_watch_list"
    assert calls[0]["params"] == {}


@pytest.mark.parametrize(
    "value",
    ["", "0", "01", "-1", "+1", "1.0", " 1", "\u0661", str(1 << 128), 1, 1.5, True, None],
)
@asynchronous
async def test_bad_fee_caps_are_rejected_before_any_rpc(value: Any):
    calls: list[dict[str, Any]] = []
    async with endpoint(JOB, calls) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(ValueError, match="positive u128"):
            await client.refund_watch_register("descriptor", OUTPOINT, max_fee_atoms=value)
    assert calls == []


@asynchronous
async def test_lock_unlock_omit_unlimited_duration_and_keep_explicit_bounded_duration():
    calls: list[dict[str, Any]] = []
    async with (
        endpoint({"locked": False}, calls) as url,
        DaemonClient(url, TOKEN) as client,
    ):
        await client.wallet_lock()
        await client.wallet_unlock("test-only-password")
        await client.wallet_unlock(
            "test-only-password",
            passphrase="test-only-passphrase",
            unlock_for_seconds=60,
        )
    assert [(call["method"], call["params"]) for call in calls] == [
        ("wallet_lock", {}),
        ("wallet_unlock", {"password": "test-only-password", "passphrase": ""}),
        (
            "wallet_unlock",
            {
                "password": "test-only-password",
                "passphrase": "test-only-passphrase",
                "unlock_for_seconds": 60,
            },
        ),
    ]


@pytest.mark.parametrize(
    "answer",
    [
        {"locked": True, "unlock_remaining_seconds": 0},
        {"locked": False, "unlock_remaining_seconds": None},
        {"locked": False, "unlock_remaining_seconds": 59},
    ],
)
@asynchronous
async def test_wallet_state_only_queries_and_preserves_nullable_integer_lease(answer: dict[str, Any]):
    calls: list[dict[str, Any]] = []
    async with endpoint(answer, calls) as url, DaemonClient(url, TOKEN) as client:
        assert await client.wallet_state() == answer
        assert await client.wallet_state() == answer
    assert [(call["method"], call["params"]) for call in calls] == [("wallet_state", {}), ("wallet_state", {})]


@asynchronous
async def test_locked_wallet_is_an_explicit_nonretryable_error():
    calls: list[dict[str, Any]] = []
    answer = {
        "code": -32005,
        "message": "wallet is locked",
        "data": {"retryable": False},
    }
    async with (
        endpoint(answer, calls, error=True) as url,
        DaemonClient(url, TOKEN) as client,
    ):
        with pytest.raises(DaemonError) as refused:
            await client.refund_watch_cancel(JOB["job_id"])
    assert refused.value.code == ErrorCode.WALLET_LOCKED
    assert not refused.value.retryable
    assert not refused.value.outcome_unknown


@asynchronous
async def test_forget_propagates_pending_restriction_instead_of_assuming_success():
    calls: list[dict[str, Any]] = []
    answer = {"code": -32000, "message": "refund outcome is unresolved", "data": {"retryable": False}}
    async with endpoint(answer, calls, error=True) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as refused:
            await client.refund_watch_forget(JOB["job_id"])
    assert refused.value.code == ErrorCode.REFUSED
    assert not refused.value.retryable
    assert len(calls) == 1
    assert calls[0]["method"] == "refund_watch_forget"


@asynchronous
async def test_existing_event_stream_preserves_refund_and_lock_notifications():
    async def socket_handler(request: web.Request) -> web.WebSocketResponse:
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        subscription = await socket.receive_json()
        assert subscription["params"] == {"events": ["refund_watch", "wallet_lock"]}
        await socket.send_json({"jsonrpc": "2.0", "id": subscription["id"], "result": {}})
        for kind, data in [("refund_watch", {"job_id": JOB["job_id"]}), ("wallet_lock", {"locked": True})]:
            await socket.send_json({"jsonrpc": "2.0", "method": "event", "params": {"type": kind, "data": data}})
        await socket.receive()
        await socket.close()
        return socket

    app = web.Application()
    app.router.add_get("/v1/ws", socket_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        url = f"http://127.0.0.1:{runner.addresses[0][1]}"
        async with EventStream(url, TOKEN, events=["refund_watch", "wallet_lock"]) as stream:
            events = aiter(stream)
            assert (await anext(events)).type == CONNECTED
            assert await anext(events) == Event("refund_watch", {"job_id": JOB["job_id"]})
            assert await anext(events) == Event("wallet_lock", {"locked": True})
            await events.aclose()
    finally:
        await runner.cleanup()
