import asyncio
from decimal import Decimal

from fake_daemon import TOKEN, FakeDaemon, asynchronous

from examples.merchant import take_payment
from tenrai_daemon import DaemonClient, EventStream


@asynchronous
async def test_an_order_is_paid_once_its_address_holds_the_amount():
    async with (
        FakeDaemon() as daemon,
        DaemonClient(daemon.url, TOKEN) as client,
        EventStream(daemon.url, TOKEN, backoff=(0.05,)) as stream,
    ):
        checkout = asyncio.create_task(take_payment(client, stream, "4711", Decimal("5")))
        while "order 4711" not in daemon.labels.values() or not daemon.subscribed:
            await asyncio.sleep(0.01)
        address = next(address for address, label in daemon.labels.items() if label == "order 4711")
        daemon.height += 1
        daemon.pay(address, "2")
        await daemon.push("finalized", {"address": address})
        await asyncio.sleep(0.2)
        assert not checkout.done()
        last = daemon.pay(address, "3")
        await daemon.push("finalized", {"address": address})
        assert await asyncio.wait_for(checkout, 5) == last["transaction"]
