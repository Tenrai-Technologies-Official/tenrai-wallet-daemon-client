import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from aiohttp import web
from fake_daemon import TOKEN, FakeDaemon, asynchronous

from tenrai_daemon import DaemonClient, DaemonError, ErrorCode, Recipient
from tenrai_daemon import client as client_module


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextlib.asynccontextmanager
async def serving(*routes: web.RouteDef) -> AsyncIterator[str]:
    """A server that answers like no real daemon does."""
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def test_plain_http_goes_only_to_this_machine():
    for url in ("http://127.0.0.1:28703", "http://localhost:28703", "http://[::1]:28703", "https://wallet.example"):
        DaemonClient(url, TOKEN)
    for url in ("http://192.0.2.7:28703", "ftp://127.0.0.1", "127.0.0.1:28703"):
        with pytest.raises(ValueError):
            DaemonClient(url, TOKEN)
    assert DaemonClient("http://192.0.2.7:28703/", TOKEN, allow_insecure=True).url == "http://192.0.2.7:28703"


@asynchronous
async def test_a_redirect_is_not_followed():
    reached: list[str | None] = []

    async def elsewhere(request: web.Request) -> web.Response:
        reached.append(request.headers.get("Authorization"))
        return web.json_response({"jsonrpc": "2.0", "id": 1, "result": {}})

    async with serving(web.post("/v1/rpc", elsewhere)) as target:

        async def redirect(_: web.Request) -> web.Response:
            raise web.HTTPTemporaryRedirect(f"{target}/v1/rpc")

        async with serving(web.post("/v1/rpc", redirect)) as url, DaemonClient(url, TOKEN) as client:
            with pytest.raises(DaemonError) as refused:
                await client.status()
    assert "HTTP 307" in refused.value.message
    assert reached == []


@asynchronous
async def test_answers_that_are_no_reply_object_are_errors():
    async def odd(_: web.Request) -> web.Response:
        return web.json_response([1, 2, 3])

    async def undescribed(_: web.Request) -> web.Response:
        return web.json_response({"jsonrpc": "2.0", "id": 1, "error": "boom"})

    async with serving(web.post("/v1/rpc", odd), web.get("/health", odd)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as rpc:
            await client.status()
        with pytest.raises(DaemonError) as health:
            await client.health()
    async with serving(web.post("/v1/rpc", undescribed)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as error:
            await client.status()
    assert rpc.value.retryable
    assert health.value.retryable
    assert "boom" in error.value.message
    assert error.value.code is None


@asynchronous
async def test_an_oversized_answer_is_refused(monkeypatch):
    monkeypatch.setattr(client_module, "MAX_REPLY_BYTES", 1024)

    async def huge(_: web.Request) -> web.Response:
        return web.json_response({"jsonrpc": "2.0", "id": 1, "result": "x" * 4096})

    async with serving(web.post("/v1/rpc", huge)) as url, DaemonClient(url, TOKEN) as client:
        with pytest.raises(DaemonError) as refused:
            await client.status()
    assert "larger than" in refused.value.message


@asynchronous
async def test_calls_carry_the_token_and_return_the_result():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        assert (await client.status())["finalized_height"] == 100
        assert (await client.health())["live"] is True
        assert daemon.calls == [("status", {})]


@asynchronous
async def test_a_wrong_token_is_refused_for_good():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, "wrong") as client:
        with pytest.raises(DaemonError) as refused:
            await client.status()
    assert refused.value.code is None
    assert not refused.value.retryable


@asynchronous
async def test_daemon_errors_keep_their_code_and_retry_hint():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        daemon.fail_next_send(ErrorCode.SERVICE_UNAVAILABLE, "the service could not be reached", retryable=True)
        with pytest.raises(DaemonError) as busy:
            await client.send("ttrai1x", "1")
        daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "the outcome is unknown")
        with pytest.raises(DaemonError) as unknown:
            await client.send("ttrai1x", "1", request_id="payout-9")
    assert busy.value.code == ErrorCode.SERVICE_UNAVAILABLE
    assert busy.value.retryable
    assert unknown.value.outcome_unknown
    assert not unknown.value.retryable


@asynchronous
async def test_an_unreachable_daemon_is_worth_another_try():
    async with DaemonClient(f"http://127.0.0.1:{closed_port()}", TOKEN, timeout=5) as client:
        with pytest.raises(DaemonError) as down:
            await client.status()
    assert down.value.retryable
    assert down.value.code is None


@asynchronous
async def test_payments_go_out_as_daemon_strings_without_empty_fields():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        await client.send("ttrai1x", Decimal("1.5"), request_id="payout-1", subtract_fee=True)
        await client.send_many({"ttrai1a": "0.25", "ttrai1b": Decimal("2")}, dry_run=True)
    assert daemon.calls[0] == (
        "send",
        {
            "to": "ttrai1x",
            "amount": "1.50000000",
            "fee": "normal",
            "request_id": "payout-1",
            "subtract_fee": True,
            "dry_run": False,
        },
    )
    assert daemon.calls[1][1]["recipients"] == [
        {"address": "ttrai1a", "amount": "0.25000000"},
        {"address": "ttrai1b", "amount": "2.00000000"},
    ]
    assert "compound_first" not in daemon.calls[1][1]


@asynchronous
async def test_a_batch_can_carry_per_recipient_payloads_and_an_explicit_compound_choice():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        await client.send_many(
            [
                Recipient("ttrai1first", "0.25", payload_text="invoice 41"),
                Recipient("ttrai1second", Decimal("2")),
            ],
            compound_first=False,
        )

    assert daemon.calls == [
        (
            "send_many",
            {
                "recipients": [
                    {"address": "ttrai1first", "amount": "0.25000000", "payload_text": "invoice 41"},
                    {"address": "ttrai1second", "amount": "2.00000000"},
                ],
                "fee": "normal",
                "subtract_fee": False,
                "compound_first": False,
                "dry_run": False,
            },
        )
    ]


def test_a_float_never_reaches_the_daemon():
    client = DaemonClient("http://127.0.0.1:1", TOKEN)
    with pytest.raises(TypeError):
        asyncio.run(client.send("ttrai1x", 1.5))


@asynchronous
async def test_private_statements_pass_their_amounts_as_text_and_wait_only_when_told():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        assert (await client.private_status())["active"] is True
        assert (await client.private_register("1", wait=True))["registered"] is True
        await client.private_enter(Decimal("100"))
        await client.private_enter("5.5", to="dtrai1other", wait=True)
        await client.private_transfer("dtrai1other", "1.25")
        await client.private_exit("dtrai1plain", [3, 9], wait=True)
        assert daemon.calls == [
            ("private_status", {}),
            ("private_register", {"max_fee": "1.00000000", "wait": True}),
            ("private_enter", {"amount": "100.00000000"}),
            ("private_enter", {"amount": "5.50000000", "to": "dtrai1other", "wait": True}),
            ("private_transfer", {"to": "dtrai1other", "amount": "1.25000000"}),
            ("private_exit", {"to": "dtrai1plain", "notes": [3, 9], "wait": True}),
        ]


@asynchronous
async def test_private_balance_asks_for_a_rescan_only_when_told():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        assert (await client.private_balance())["private_balance"] == "12.5"
        await client.private_balance(rescan=True)
        assert daemon.calls == [("private_balance", {}), ("private_balance", {"rescan": True})]


@asynchronous
async def test_a_rescan_sends_its_depth_only_when_given():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        assert (await client.rescan())["depth"] == 1000
        assert (await client.rescan(5000))["depth"] == 5000
        assert daemon.calls == [("rescan", {}), ("rescan", {"depth": 5000})]


@asynchronous
async def test_outputs_ask_for_the_largest_only_when_told():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        assert (await client.outputs())["count"] == 3
        largest = await client.outputs(largest=1)
        assert [row["amount"] for row in largest["outputs"]] == ["5.00000000"]
        assert daemon.calls == [("outputs", {}), ("outputs", {"largest": 1})]


@asynchronous
async def test_an_address_proof_goes_from_request_to_verdict():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        mine = (await client.new_address("customer-7"))["address"]
        words = (await client.validate_address(mine))["verification_words"]
        assert words == "windmill talc frosty fjord frothy sorghum"
        request = await client.proof_request(mine, valid_for=60)
        assert request["expires_at"] == 1_900_000_060
        proof = await client.prove_address(request)
        assert (await client.verify_address_proof(proof, request))["valid"] is True
        other = {**request, "challenge": "cd" * 32}
        assert (await client.verify_address_proof(proof, other))["valid"] is False
        with pytest.raises(DaemonError) as refused:
            await client.prove_address({**request, "address": "ttrai1stranger"})
        assert refused.value.code == -32602
        assert [name for name, _ in daemon.calls[-5:]] == [
            "proof_request",
            "prove_address",
            "verify_address_proof",
            "verify_address_proof",
            "prove_address",
        ]
        assert daemon.calls[-5] == ("proof_request", {"address": mine, "valid_for": 60})
