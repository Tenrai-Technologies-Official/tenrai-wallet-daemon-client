import asyncio

import pytest
from fake_daemon import TOKEN, FakeDaemon, asynchronous

from tenrai_daemon import CONNECTED, DaemonError, Event, EventStream


async def next_event(events) -> Event:
    return await asyncio.wait_for(anext(events), 5)


@asynchronous
async def test_events_arrive_after_the_connected_marker_and_are_filtered():
    async with FakeDaemon() as daemon, EventStream(daemon.url, TOKEN, events=["finalized"]) as stream:
        events = aiter(stream)
        assert (await next_event(events)).type == CONNECTED
        await daemon.push("balance", {"balance": "1.00000000"})
        await daemon.push("finalized", {"address": "ttrai1a", "height": 7})
        assert await next_event(events) == Event("finalized", {"address": "ttrai1a", "height": 7})
        await events.aclose()


@asynchronous
async def test_a_dropped_socket_comes_back_and_says_so():
    async with FakeDaemon() as daemon, EventStream(daemon.url, TOKEN, backoff=(0.05,)) as stream:
        events = aiter(stream)
        assert (await next_event(events)).type == CONNECTED
        await daemon.drop_sockets()
        assert (await next_event(events)).type == CONNECTED
        await daemon.push("finalized", {"height": 8})
        assert (await next_event(events)).data["height"] == 8
        await events.aclose()


@asynchronous
async def test_a_refused_token_ends_the_stream():
    async with FakeDaemon() as daemon, EventStream(daemon.url, "wrong") as stream:
        with pytest.raises(DaemonError):
            await next_event(aiter(stream))


@asynchronous
async def test_an_unknown_event_name_fails_loudly():
    async with FakeDaemon() as daemon, EventStream(daemon.url, TOKEN, events=["nonsense"]) as stream:
        with pytest.raises(DaemonError) as refused:
            await next_event(aiter(stream))
    assert refused.value.code == -32602


def test_a_socket_to_another_machine_needs_tls():
    assert EventStream("http://127.0.0.1:28703").url == "ws://127.0.0.1:28703/v1/ws"
    assert EventStream("https://wallet.example").url == "wss://wallet.example/v1/ws"
    with pytest.raises(ValueError):
        EventStream("ws://192.0.2.7:28703")
    assert EventStream("ws://192.0.2.7:28703", allow_insecure=True).url == "ws://192.0.2.7:28703/v1/ws"


@asynchronous
async def test_malformed_frames_are_skipped():
    async with FakeDaemon() as daemon, EventStream(daemon.url, TOKEN) as stream:
        events = aiter(stream)
        assert (await next_event(events)).type == CONNECTED
        for text in ("not json", "[1, 2]", '{"method": "event", "params": [1]}'):
            await daemon.push_raw(text)
        await daemon.push_raw('{"method": "event", "params": {"type": "odd", "data": 5}}')
        await daemon.push("finalized", {"height": 9})
        assert await next_event(events) == Event("odd", {})
        assert await next_event(events) == Event("finalized", {"height": 9})
        await events.aclose()


@asynchronous
async def test_a_garbled_subscription_reply_means_another_try():
    async with FakeDaemon() as daemon, EventStream(daemon.url, TOKEN, backoff=(0.05,)) as stream:
        daemon.garbled_subscriptions = 1
        events = aiter(stream)
        assert (await next_event(events)).type == CONNECTED
        assert daemon.garbled_subscriptions == 0
        await events.aclose()
