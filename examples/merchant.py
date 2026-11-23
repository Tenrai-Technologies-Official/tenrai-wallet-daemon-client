"""Wait for confirmed payments to a fresh order address.

Usage:
    python examples/merchant.py --token-file wallet.vault.token --order 4711 --amount 12.50
"""

import argparse
import asyncio
import sys
from decimal import Decimal

from tenrai_daemon import CONNECTED, DEFAULT_URL, DaemonClient, EventStream, from_atoms, to_atoms

# Recheck confirmed outputs after these events.
CHECK_ON = frozenset({CONNECTED, "finalized", "confirmations", "resync", "resync_required"})


async def take_payment(
    client: DaemonClient,
    stream: EventStream,
    order: str,
    amount: Decimal,
    *,
    confirmations: int = 1,
) -> str:
    """Return the final contributing transaction once confirmed payments cover the order."""
    invoice = await client.new_address(label=f"order {order}")
    since = (await client.status())["finalized_height"]
    wanted = to_atoms(amount)
    print(f"Order {order}: please pay {from_atoms(wanted)} TRAI to {invoice['address']}")
    print(f"Payment link: {invoice['payment_uri']}")
    async for event in stream:
        if event.type == "unconfirmed" and event.data.get("address") == invoice["address"]:
            print(f"Seen {event.data['amount']} TRAI, waiting until it is final")
        if event.type not in CHECK_ON:
            continue
        paid = await client.received(since_height=since)
        rows = [
            row
            for row in paid["received"]
            if row["address"] == invoice["address"] and row["confirmations"] >= confirmations
        ]
        total = sum(to_atoms(row["amount"]) for row in rows)
        if total >= wanted:
            print(f"Order {order} is paid: {from_atoms(total)} TRAI")
            return rows[-1]["transaction"]
        if total:
            print(f"Order {order}: {from_atoms(total)} of {from_atoms(wanted)} TRAI paid so far")
    raise RuntimeError("the event stream ended")


async def main(args: argparse.Namespace) -> int:
    async with (
        DaemonClient.from_token_file(args.daemon, args.token_file) as client,
        EventStream.from_token_file(args.daemon, args.token_file) as stream,
    ):
        try:
            async with asyncio.timeout(args.expires_after):
                transaction = await take_payment(
                    client, stream, args.order, Decimal(args.amount), confirmations=args.confirmations
                )
        except TimeoutError:
            print(f"Order {args.order} was not paid in time", file=sys.stderr)
            return 1
    print(f"Paid with transaction {transaction}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", default=DEFAULT_URL)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--order", required=True)
    parser.add_argument("--amount", required=True, help="in TRAI, e.g. 12.50")
    parser.add_argument("--confirmations", type=int, default=1)
    parser.add_argument("--expires-after", type=float, default=1800, help="seconds, default 30 minutes")
    sys.stdout.reconfigure(line_buffering=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
