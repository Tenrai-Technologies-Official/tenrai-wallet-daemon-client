"""Read wallet status, issue an address and request an unsigned fee quote.

Usage:
    python examples/quickstart.py --token-file wallet.vault.token
"""

import argparse
import asyncio
import contextlib
import sys

from tenrai_daemon import DEFAULT_URL, DaemonClient, DaemonError, ErrorCode, EventStream


async def main(args: argparse.Namespace) -> None:
    async with DaemonClient.from_token_file(args.daemon, args.token_file) as client:
        status = await client.status()
        health = await client.health()
        print(f"daemon {status['version']} on {status['network']} at height {status['finalized_height']}")
        print(f"connected to {status['service']}, live feed {'up' if health['live'] else 'down'}")

        balance = await client.balance()
        print(f"balance {balance['balance']} TRAI, {balance['available']} available")

        address = await client.new_address(label="quickstart")
        print(f"new address {address['address']}")

        try:
            quote = await client.quote_send_many({address["address"]: "0.001"})
            print(f"estimated fee for 0.001 TRAI: {quote['fee']} TRAI")
        except DaemonError as error:
            if error.code != ErrorCode.REFUSED:
                raise
            print(f"fee quote unavailable: {error.message}")

    print("listening to events for 20 seconds")
    async with EventStream.from_token_file(args.daemon, args.token_file) as stream:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(20):
                async for event in stream:
                    print(event.type, event.data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", default=DEFAULT_URL)
    parser.add_argument("--token-file", required=True)
    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(main(parser.parse_args()))
