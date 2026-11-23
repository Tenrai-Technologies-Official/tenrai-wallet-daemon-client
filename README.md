# Tenrai Wallet Daemon Client

Async Python client for the Tenrai wallet daemon. It provides JSON-RPC calls, a reconnecting event stream and examples for merchant payments and exchange accounting.

## Install

Use Python 3.12 or later. Install from this checkout.

```sh
python -m pip install .
```

Start the daemon against a synced Tenrai node.

```sh
tenrai-wallet daemon --file exchange.vault --password-file exchange.pass --service node --confirmations 3
```

The default endpoint is `http://127.0.0.1:28703`. The daemon writes its bearer token to `exchange.vault.token`. Restrict the token and wallet files to the service account. Keep the daemon on loopback or use TLS for remote access.

## Read wallet status

```python
import asyncio

from tenrai_daemon import DEFAULT_URL, DaemonClient


async def main():
    async with DaemonClient.from_token_file(DEFAULT_URL, "exchange.vault.token") as daemon:
        status = await daemon.status()
        print(status["network"], status["finalized_height"])


asyncio.run(main())
```

Pass TRAI amounts as strings or `Decimal`. Use `to_atoms` and `from_atoms` for integer accounting. Treat payment payloads as sender input and escape them when rendering.

## Payments and recovery

Use `quote_send_many` for an unsigned fee preview. `send_many(max_fee=...)` binds the fee ceiling before signing. A signed `dry_run` export reserves its inputs.

For automated payouts, derive `RequestScope` from fresh daemon status. Persist the scope, request ID and payment arguments with the first-attempt marker before sending. Submit once with `recovery_only=False` and use `recovery_only=True` with the saved scope and arguments for every later attempt. Keep ambiguous payments reserved for reconciliation using the original request. A daemon-owned `never_signed` outcome closes that request without a transaction.

Private payments require an activated network and the native prover. Store the scope and request ID for `private_enter`, `private_transfer` and `private_exit`. Use `private_retry` and `private_receipts` to recover the original frame and its outcome.

For a backup, stop processes using the vault. Keep its `.pending.json`, `.pending/`, `.requests.json` and `.requests.json.sqlite3` with the vault and any SQLite journal files. Preserve owner-only access. Keep the matching application ledger with the backup.

## Events and refunds

Use `EventStream` for notifications. On `connected`, `resync` and `resync_required`, catch up with `received(since_height=...)` from the saved cursor. Credit deposits from confirmed outputs keyed by transaction ID and output index.

With the daemon running unlocked and `--auto-refund` enabled, use `refund_watch_register` to monitor an exact HTLC funding outpoint. The refund uses the committed owner and a fee cap in integer atoms. Use `refund_watch_status` to inspect the job. After a reconnect or an ambiguous reply, refresh the same job or list before acting.

`wallet_lock` stops new signing. `wallet_unlock` accepts a bounded `unlock_for_seconds` lease. Use `wallet_state` to read the remaining lease.

## Examples

- [Quickstart](examples/quickstart.py) reads status and balance, issues an address and requests an unsigned fee quote.
- [Merchant checkout](examples/merchant.py) waits for confirmed payments to an order address.
- [Exchange ledger](examples/exchange) tracks deposits and withdrawals in SQLite. Use `python -m examples.exchange --help` for commands.

## Checks

```sh
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m pytest
```

Use a dedicated test wallet for the optional live suite. Set `TENRAI_DAEMON_URL` and `TENRAI_DAEMON_TOKEN_FILE` before running `python -m pytest tests/test_live.py`. The live suite issues an address and can reserve inputs for a signed export.

## License

Apache-2.0.
