"""Command line for the example exchange.

Usage:
    python -m examples.exchange --token-file wallet.vault.token run
    python -m examples.exchange --token-file wallet.vault.token assign alice
    python -m examples.exchange --token-file wallet.vault.token withdraw alice <address> 1.5
"""

import argparse
import asyncio
import contextlib
import json
import logging
import sys

from tenrai_daemon import DEFAULT_URL, DaemonClient, DaemonError, EventStream, from_atoms

from .deposits import DepositWatcher
from .ledger import Deposit, Ledger
from .withdrawals import WithdrawalError, Withdrawals

log = logging.getLogger("exchange")

CHECK_EVERY = 15.0


async def run(stream: EventStream, deposits: DepositWatcher, withdrawals: Withdrawals) -> None:
    """Process events and periodically reconcile pending work."""
    checker = asyncio.create_task(_check_regularly(deposits, withdrawals))
    try:
        async for event in stream:
            try:
                await deposits.handle(event)
                await withdrawals.handle(event)
            except DaemonError as error:
                # Resume from the last committed cursor on the next catch-up.
                log.warning("could not handle %s: %s", event.type, error.message)
    finally:
        checker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await checker


async def _check_regularly(deposits: DepositWatcher, withdrawals: Withdrawals) -> None:
    while True:
        await asyncio.sleep(CHECK_EVERY)
        try:
            if deposits.ledger.open_height() is not None:
                await deposits.catch_up()
            await withdrawals.retry()
            await withdrawals.reconcile()
        except DaemonError as error:
            log.warning("regular check failed: %s", error.message)


async def _credited(deposit: Deposit) -> None:
    # Notify downstream systems after crediting the customer's deposit.
    log.info("deposit of %s TRAI for %s is available", from_atoms(deposit.amount), deposit.customer)


async def _main(args: argparse.Namespace) -> int:
    ledger = Ledger(args.db)
    try:
        async with DaemonClient.from_token_file(args.daemon, args.token_file) as client:
            deposits = DepositWatcher(client, ledger, confirmations=args.confirmations, on_credit=_credited)
            withdrawals = Withdrawals(client, ledger)
            match args.command:
                case "run":
                    async with EventStream.from_token_file(args.daemon, args.token_file) as stream:
                        await run(stream, deposits, withdrawals)
                case "assign":
                    print(await deposits.assign_address(args.customer))
                case "reference":
                    address, reference = await deposits.assign_reference(args.customer)
                    print(f"address {address}\nreference {reference}")
                case "withdraw":
                    withdrawal = await withdrawals.request(
                        args.customer, args.address, args.amount, subtract_fee=args.subtract_fee
                    )
                    print(f"{withdrawal.state} {withdrawal.transaction or withdrawal.reason or ''}".strip())
                case "review-withdrawals":
                    after = ""
                    while rows := withdrawals.needs_review(after=after):
                        for withdrawal in rows:
                            print(
                                json.dumps(
                                    {
                                        "request_id": withdrawal.request_id,
                                        "customer": withdrawal.customer,
                                        "address": withdrawal.address,
                                        "amount_atoms": str(withdrawal.amount),
                                        "state": withdrawal.state,
                                        "reason": withdrawal.reason,
                                    },
                                    ensure_ascii=True,
                                )
                            )
                        after = rows[-1].request_id
                case "retry-withdrawal":
                    withdrawal = await withdrawals.retry_review(args.request_id)
                    print(
                        json.dumps(
                            {
                                "request_id": withdrawal.request_id,
                                "state": withdrawal.state,
                                "transaction": withdrawal.transaction,
                                "reason": withdrawal.reason,
                            },
                            ensure_ascii=True,
                        )
                    )
                case "balance":
                    print(
                        f"{from_atoms(ledger.balance(args.customer))} TRAI available, "
                        f"{from_atoms(ledger.pending(args.customer))} TRAI incoming"
                    )
                case "unassigned":
                    for deposit in ledger.unassigned():
                        amount = from_atoms(deposit.amount)
                        print(f"{deposit.transaction}:{deposit.output} {amount} TRAI {deposit.payload!r}")
    except (DaemonError, WithdrawalError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    finally:
        ledger.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m examples.exchange", description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", default=DEFAULT_URL, help=f"wallet daemon URL (default {DEFAULT_URL})")
    parser.add_argument("--token-file", required=True, help="the daemon's <wallet file>.token")
    parser.add_argument("--db", default="exchange.db", help="SQLite ledger (default exchange.db)")
    parser.add_argument("--confirmations", type=int, default=3, help="cut confirmations before crediting (default 3)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="watch deposits and pay withdrawals until stopped")
    commands.add_parser("assign", help="print a customer's deposit address").add_argument("customer")
    commands.add_parser("reference", help="shared address and payment reference").add_argument("customer")
    withdraw = commands.add_parser("withdraw", help="pay a customer's withdrawal")
    withdraw.add_argument("customer")
    withdraw.add_argument("address")
    withdraw.add_argument("amount")
    withdraw.add_argument("--subtract-fee", action="store_true", help="the customer carries the network fee")
    commands.add_parser("balance", help="a customer's balance").add_argument("customer")
    commands.add_parser("unassigned", help="payments nobody could be matched to")
    commands.add_parser("review-withdrawals", help="list paused withdrawals and their original request IDs")
    commands.add_parser(
        "retry-withdrawal", help="retry a reviewed withdrawal under its original request ID"
    ).add_argument("request_id")
    parser.set_defaults(subtract_fee=False)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        return asyncio.run(_main(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
