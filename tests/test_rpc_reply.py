import asyncio

import pytest
from aiohttp import web
from fake_daemon import TOKEN, asynchronous
from test_client import serving

from tenrai_daemon import DaemonClient, DaemonError, ErrorCode, EventStream
from tenrai_daemon.rpc_reply import rpc_result


@pytest.mark.parametrize(
    "reply",
    [
        {},
        {"id": 1, "result": {}},
        {"jsonrpc": "1.0", "id": 1, "result": {}},
        {"jsonrpc": "2.0", "result": {}},
        {"jsonrpc": "2.0", "id": True, "result": {}},
        {"jsonrpc": "2.0", "id": 1.0, "result": {}},
        {"jsonrpc": "2.0", "id": "1", "result": {}},
        {"jsonrpc": "2.0", "id": 2, "result": {}},
        {"jsonrpc": "2.0", "id": 1},
        {"jsonrpc": "2.0", "id": 1, "result": {}, "error": None},
        {"jsonrpc": "2.0", "id": 1, "error": None},
        {"jsonrpc": "2.0", "id": 1, "error": {"code": True, "message": "bad"}},
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32002}},
    ],
)
def test_mismatched_and_ambiguous_replies_have_no_authoritative_outcome(reply):
    with pytest.raises(DaemonError) as failed:
        rpc_result(reply, 1)
    assert failed.value.code is None
    assert failed.value.retryable
    assert failed.value.reason is None


def test_valid_null_results_and_typed_daemon_errors_keep_their_meaning():
    assert rpc_result({"jsonrpc": "2.0", "id": 1, "result": None}, 1) is None
    with pytest.raises(DaemonError) as failed:
        rpc_result(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32002, "message": "missing receipt", "data": {"reason": "request_not_archived"}},
            },
            1,
        )
    assert failed.value.code == ErrorCode.OUTCOME_UNKNOWN
    assert failed.value.reason == "request_not_archived"


@asynchronous
async def test_http_ack_from_another_call_is_refused_without_resending_payment():
    calls = []

    async def answer(request):
        body = await request.json()
        calls.append(body)
        return web.json_response({"jsonrpc": "2.0", "id": body["id"] + 1, "result": {"transaction_id": "ab" * 48}})

    async with serving(web.post("/v1/rpc", answer)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as failed:
            await client.send("ttrai1recipient", "1", request_id="payout-1")
    assert failed.value.outcome_unknown
    assert not failed.value.retryable
    assert len(calls) == 1
    assert calls[0]["params"]["request_id"] == "payout-1"


@asynchronous
async def test_concurrent_out_of_order_http_replies_still_bind_to_each_call():
    async def answer(request):
        body = await request.json()
        await asyncio.sleep(0.02 if body["id"] == 1 else 0)
        return web.json_response({"jsonrpc": "2.0", "id": body["id"], "result": body["params"]})

    async with serving(web.post("/v1/rpc", answer)) as url, DaemonClient(url, TOKEN) as client:
        results = await asyncio.gather(client.call("read", {"key": "a"}), client.call("read", {"key": "b"}))
    assert results == [{"key": "a"}, {"key": "b"}]


@asynchronous
async def test_wrong_subscription_ack_reconnects_before_connected_is_exposed():
    subscriptions = 0

    async def socket(request):
        nonlocal subscriptions
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        body = await ws.receive_json()
        subscriptions += 1
        await ws.send_json(
            {"jsonrpc": "2.0", "id": "another" if subscriptions == 1 else body["id"], "result": {"subscription": 1}}
        )
        async for _ in ws:
            pass
        return ws

    async with serving(web.get("/v1/ws", socket)) as url, EventStream(url, TOKEN, backoff=(0.01,)) as stream:
        events = aiter(stream)
        connected = await asyncio.wait_for(anext(events), 5)
        assert connected.type == "connected"
        assert subscriptions == 2
        await events.aclose()


@pytest.mark.parametrize(
    "method",
    [
        "send",
        "send_many",
        "sweep",
        "burn",
        "compound",
        "stake",
        "unstake",
        "withdraw",
        "native_sweep",
        "private_register",
        "private_enter",
        "private_transfer",
        "private_exit",
        "private_retry",
    ],
)
@asynchronous
async def test_spending_transport_failure_is_uncertain_and_never_an_unscoped_retry(method):
    calls = 0

    async def accepted_but_lost_reply(request):
        nonlocal calls
        await request.json()
        calls += 1
        return web.Response(text="not a receipt", content_type="application/json")

    async with serving(web.post("/v1/rpc", accepted_but_lost_reply)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as failed:
            await client.call(method, {})
    assert failed.value.outcome_unknown
    assert not failed.value.retryable
    assert failed.value.reason is None
    assert calls == 1


@pytest.mark.parametrize("method,params", [("status", {}), ("quote_send_many", {})])
@asynchronous
async def test_reads_and_unsigned_quotes_can_retry_malformed_replies(method, params):
    async def malformed(request):
        await request.json()
        return web.Response(text="not JSON", content_type="application/json")

    async with serving(web.post("/v1/rpc", malformed)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as failed:
            await client.call(method, params)
    assert failed.value.code is None
    assert failed.value.retryable


@pytest.mark.parametrize("method", ["send_many", "send", "private_register", "native_sweep"])
@asynchronous
async def test_lost_signed_export_responses_are_unknown_and_never_retryable_previews(method):
    async def malformed(request):
        await request.json()
        return web.Response(text="not JSON", content_type="application/json")

    async with serving(web.post("/v1/rpc", malformed)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as failed:
            await client.call(method, {"dry_run": True})
    assert failed.value.code == ErrorCode.OUTCOME_UNKNOWN
    assert not failed.value.retryable
