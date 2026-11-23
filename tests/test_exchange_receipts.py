import sqlite3

import pytest
from fake_daemon import TOKEN, FakeDaemon, asynchronous
from test_exchange_terminal import TRAI, credit

from examples.exchange import Ledger, Withdrawals
from tenrai_daemon import DaemonClient, Event, RequestScope


@asynchronous
async def test_malformed_settlement_prompts_neither_query_the_daemon_nor_release_credit(tmp_path):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            calls = list(daemon.calls)
            for transaction in [None, True, {}, [], "", "abc", "AB" * 48]:
                await withdrawals.handle(Event("settled", {"outcome": "expired", "transaction": transaction}))
            await withdrawals.handle(Event("settled", {"outcome": "lost"}))
            assert daemon.calls == calls
            assert ledger.withdrawal(paid.request_id).state == "submitted"
            assert ledger.balance("alice") == 6 * TRAI


def test_one_transaction_receipt_cannot_finalize_multiple_withdrawals_and_is_indexed(tmp_path):
    path = tmp_path / "exchange.db"
    scope = RequestScope("aa" * 32, 0, "bb" * 48)
    with Ledger(path) as ledger:
        credit(ledger)
        for request in ["first", "second"]:
            assert ledger.add_withdrawal(request, "alice", "ttrai1external", TRAI, subtract_fee=False)
            assert ledger.begin_attempt(request, scope) is False
        assert ledger.record_receipt("first", "cc" * 48, 0)
        with pytest.raises(sqlite3.IntegrityError):
            ledger.record_receipt("second", "cc" * 48, 0)
        plan = ledger._db.execute(
            "EXPLAIN QUERY PLAN SELECT request_id FROM withdrawals WHERE transaction_id = ?", ("cc" * 48,)
        ).fetchall()
        assert any("withdrawals_transaction" in row[3] for row in plan)
    with Ledger(path) as reopened:
        assert reopened.withdrawal_for("cc" * 48).request_id == "first"
        assert reopened.withdrawal("second").transaction is None
        assert reopened.balance("alice") == 8 * TRAI


@asynchronous
async def test_duplicate_receipt_keeps_second_debit_reserved_without_an_automatic_resend(tmp_path, monkeypatch):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            first = await withdrawals.request("alice", "ttrai1external", "2")
            send = client.send_many

            async def wrong_receipt(*args, **kwargs):
                answer = await send(*args, **kwargs)
                return {**answer, "transaction": first.transaction}

            monkeypatch.setattr(client, "send_many", wrong_receipt)
            second = await withdrawals.request("alice", "ttrai1external", "2")
            assert second.state == "needs_review"
            assert second.transaction is None
            assert ledger.balance("alice") == 6 * TRAI
            assert daemon.payments == 2
