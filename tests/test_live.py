"""Opt-in checks against a daemon with a dedicated test wallet."""

import asyncio
import os

import pytest
from fake_daemon import asynchronous

from tenrai_daemon import CONNECTED, DaemonClient, DaemonError, ErrorCode, EventStream

URL = os.environ.get("TENRAI_DAEMON_URL", "")
TOKEN_FILE = os.environ.get("TENRAI_DAEMON_TOKEN_FILE", "")

pytestmark = pytest.mark.skipif(not (URL and TOKEN_FILE), reason="set TENRAI_DAEMON_URL and TENRAI_DAEMON_TOKEN_FILE")


@asynchronous
async def test_the_daemon_answers_and_hands_out_addresses():
    async with DaemonClient.from_token_file(URL, TOKEN_FILE) as client:
        status = await client.status()
        assert status["finalized_height"] > 0
        issued = await client.new_address(label="live test")
        checked = await client.validate_address(issued["address"])
        assert checked["valid"] and checked["mine"]
        received = await client.received(since_height=status["finalized_height"])
        assert received["indexed_height"] >= status["indexed_height"]


@asynchronous
async def test_a_payment_can_be_reviewed_without_sending_it():
    async with DaemonClient.from_token_file(URL, TOKEN_FILE) as client:
        address = (await client.receive())["address"]
        try:
            review = await client.send(address, "0.001", payload_text="live test", dry_run=True)
        except DaemonError as error:
            if error.code == ErrorCode.REFUSED:
                pytest.skip(f"the wallet cannot pay: {error.message}")
            raise
        assert review["submitted"] is False
        assert review["transaction"]


@asynchronous
async def test_the_event_socket_connects():
    async with EventStream.from_token_file(URL, TOKEN_FILE) as stream:
        events = aiter(stream)
        assert (await asyncio.wait_for(anext(events), 15)).type == CONNECTED
        await events.aclose()
