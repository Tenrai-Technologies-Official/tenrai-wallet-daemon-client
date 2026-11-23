import asyncio
import contextlib
import sqlite3
from collections.abc import AsyncIterator

import pytest
from fake_daemon import TOKEN, FakeDaemon, asynchronous

from examples.exchange import Deposit, DepositWatcher, Ledger, WithdrawalError, Withdrawals
from tenrai_daemon import CONNECTED, DaemonClient, ErrorCode, Event

TRAI = 100_000_000


class Exchange:
    """The example exchange on a fake daemon, with every credit recorded."""

    def __init__(self, daemon: FakeDaemon, client: DaemonClient, *, confirmations: int = 1) -> None:
        self.daemon = daemon
        self.ledger = Ledger()
        self.credited: list[Deposit] = []
        self.deposits = DepositWatcher(client, self.ledger, confirmations=confirmations, on_credit=self._credit)
        self.withdrawals = Withdrawals(client, self.ledger)

    async def _credit(self, deposit: Deposit) -> None:
        self.credited.append(deposit)

    async def event(self, kind: str, data: dict | None = None) -> None:
        await self.deposits.handle(Event(kind, data or {}))
        await self.withdrawals.handle(Event(kind, data or {}))

    async def fund(self, customer: str, amount: str) -> None:
        address = await self.deposits.assign_address(customer)
        await self.event("finalized", finalized(self.daemon.pay(address, amount)))


@contextlib.asynccontextmanager
async def running(*, confirmations: int = 1) -> AsyncIterator[Exchange]:
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        exchange = Exchange(daemon, client, confirmations=confirmations)
        try:
            yield exchange
        finally:
            exchange.ledger.close()


def finalized(payment: dict) -> dict:
    """The daemon's `finalized` event for one payment."""
    return {"address": payment["address"], "height": payment["height"], "payments": [{**payment, "own": False}]}


# Deposits


@asynchronous
async def test_each_customer_keeps_one_deposit_address():
    async with running() as exchange:
        daemon = exchange.daemon
        alice = await exchange.deposits.assign_address("alice")
        assert await exchange.deposits.assign_address("alice") == alice
        assert await exchange.deposits.assign_address("bob") != alice
        assert daemon.labels[alice] == "alice"


@asynchronous
async def test_a_payment_is_credited_once_however_often_it_is_seen():
    async with running() as exchange:
        daemon = exchange.daemon
        payment = daemon.pay(await exchange.deposits.assign_address("alice"), "12.5")
        await exchange.event("finalized", finalized(payment))
        await exchange.event("finalized", finalized(payment))
        await exchange.event(CONNECTED)
        assert [deposit.customer for deposit in exchange.credited] == ["alice"]
        assert exchange.ledger.balance("alice") == 1_250_000_000


@asynchronous
async def test_credit_waits_for_the_house_confirmations():
    async with running(confirmations=3) as exchange:
        daemon = exchange.daemon
        payment = daemon.pay(await exchange.deposits.assign_address("alice"), "2")
        await exchange.event("finalized", finalized(payment))
        assert exchange.credited == []
        await exchange.event("confirmations", {**payment, "own": False, "confirmations": 3})
        assert len(exchange.credited) == 1


@asynchronous
async def test_a_restart_catches_up_and_keeps_counting_confirmations():
    async with running(confirmations=3) as exchange:
        daemon = exchange.daemon
        address = await exchange.deposits.assign_address("alice")
        old = daemon.pay(address, "1", height=90)
        new = daemon.pay(address, "2", height=100)
        await exchange.event(CONNECTED)
        assert [deposit.transaction for deposit in exchange.credited] == [old["transaction"]]
        assert exchange.ledger.cursor() == 100
        daemon.height = 102
        await exchange.deposits.catch_up()
        assert [deposit.transaction for deposit in exchange.credited] == [old["transaction"], new["transaction"]]
        assert exchange.ledger.open_height() is None


@asynchronous
async def test_own_transfers_rewards_and_foreign_addresses_are_no_deposits():
    async with running() as exchange:
        daemon = exchange.daemon
        alice = await exchange.deposits.assign_address("alice")
        bob = await exchange.deposits.assign_address("bob")
        daemon.pay(alice, "5", senders=(bob,))
        daemon.pay(alice, "1", source="stake_reward")
        daemon.pay(daemon.address("never handed out"), "3")
        await exchange.event(CONNECTED)
        change = daemon.pay(alice, "7", senders=(alice,))
        await exchange.event(
            "finalized", {"address": alice, "height": change["height"], "payments": [{**change, "own": True}]}
        )
        assert exchange.credited == []
        assert exchange.ledger.deposits() == []


@asynchronous
async def test_one_shared_address_tells_customers_apart_by_reference():
    async with running() as exchange:
        daemon = exchange.daemon
        shared, alice = await exchange.deposits.assign_reference("alice")
        again, bob = await exchange.deposits.assign_reference("bob")
        assert again == shared and alice != bob
        daemon.pay(shared, "4", payload=f" {alice} ")
        daemon.pay(shared, "6", payload="no such reference")
        unread = daemon.pay(shared, "9", payload=bob, payload_read=False)
        await exchange.event(CONNECTED)
        assert [(deposit.customer, deposit.amount) for deposit in exchange.credited] == [("alice", 4 * TRAI)]
        assert [deposit.payload for deposit in exchange.ledger.unassigned()] == ["no such reference"]

        # The node could not serve that payload yet; the next catch-up reads it again.
        daemon.received.remove(unread)
        daemon.received.append({**unread, "payload_text": bob, "senders": ["ttrai1customer"], "payload_read": True})
        await exchange.deposits.catch_up()
        assert exchange.ledger.balance("bob") == 9 * TRAI


@asynchronous
async def test_unconfirmed_payments_show_as_incoming_until_final_or_gone():
    async with running() as exchange:
        daemon = exchange.daemon
        address = await exchange.deposits.assign_address("alice")
        payment = daemon.pay(address, "3")
        seen = {key: payment[key] for key in ("transaction", "address", "amount", "senders", "payload_read")}
        await exchange.event("unconfirmed", seen)
        assert exchange.ledger.pending("alice") == 3 * TRAI
        await exchange.event("removed", {"transaction": payment["transaction"], "reason": "expired"})
        assert exchange.ledger.pending("alice") == 0
        await exchange.event("unconfirmed", seen)
        await exchange.event("finalized", finalized(payment))
        assert exchange.ledger.pending("alice") == 0
        assert exchange.ledger.balance("alice") == 3 * TRAI


@asynchronous
async def test_a_reconnect_forgets_incoming_payments_that_vanished_meanwhile():
    async with running() as exchange:
        daemon = exchange.daemon
        address = await exchange.deposits.assign_address("alice")
        kept, vanished = "aa" * 48, "bb" * 48
        for transaction, amount in [(kept, "3"), (vanished, "4")]:
            unconfirmed = {"transaction": transaction, "address": address, "amount": amount, "senders": ["ttrai1x"]}
            await exchange.event("unconfirmed", unconfirmed)
        assert exchange.ledger.pending("alice") == 7 * TRAI

        # The `removed` for the second one went out while the socket was down.
        daemon.incoming = [{"transaction": kept, "status": "unconfirmed", "amount": "3.00000000"}]
        await exchange.event(CONNECTED)
        assert exchange.ledger.pending("alice") == 3 * TRAI


# Withdrawals


@asynchronous
async def test_a_withdrawal_is_checked_then_paid():
    async with running() as exchange:
        daemon = exchange.daemon
        await exchange.fund("alice", "10")
        withdrawal = await exchange.withdrawals.request("alice", "ttrai1external", "4")
        assert withdrawal.state == "submitted"
        assert withdrawal.transaction in daemon.transactions
        assert exchange.ledger.balance("alice") == 6 * TRAI
        for address, amount in [
            ("ttrai1external", "7"),
            ("not-an-address", "1"),
            (await exchange.deposits.assign_address("bob"), "1"),
        ]:
            with pytest.raises(WithdrawalError):
                await exchange.withdrawals.request("alice", address, amount)
        assert daemon.payments == 1


@asynchronous
async def test_a_lost_answer_is_repaired_under_the_same_request_id():
    async with running() as exchange:
        daemon = exchange.daemon
        await exchange.fund("alice", "10")
        daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "the outcome is unknown", sent=True)
        withdrawal = await exchange.withdrawals.request("alice", "ttrai1external", "4")
        assert withdrawal.state == "requested"
        await exchange.withdrawals.retry()
        paid = exchange.ledger.withdrawal(withdrawal.request_id)
        assert paid.state == "submitted"
        assert paid.transaction == daemon.sent[withdrawal.request_id]["transaction"]
        assert daemon.payments == 1


@asynchronous
async def test_a_busy_daemon_gets_the_withdrawal_again_later():
    async with running() as exchange:
        daemon = exchange.daemon
        await exchange.fund("alice", "10")
        daemon.fail_next_send(ErrorCode.SERVICE_UNAVAILABLE, "the node is down", retryable=True)
        withdrawal = await exchange.withdrawals.request("alice", "ttrai1external", "4", subtract_fee=True)
        assert (withdrawal.state, daemon.payments) == ("requested", 0)
        await exchange.withdrawals.retry()
        assert exchange.ledger.withdrawal(withdrawal.request_id).state == "needs_review"
        assert daemon.calls[-1][1]["subtract_fee"] is True
        assert daemon.calls[-1][1]["recovery_only"] is True
        assert daemon.payments == 0


@asynchronous
async def test_a_refused_withdrawal_pauses_for_review_and_keeps_the_debit():
    async with running() as exchange:
        daemon = exchange.daemon
        await exchange.fund("alice", "10")
        daemon.fail_next_send(ErrorCode.REFUSED, "not enough spendable funds")
        withdrawal = await exchange.withdrawals.request("alice", "ttrai1external", "4")
        assert withdrawal.state == "needs_review"
        assert withdrawal.reason == "not enough spendable funds"
        assert exchange.ledger.balance("alice") == 6 * TRAI
        assert [row.request_id for row in exchange.withdrawals.needs_review()] == [withdrawal.request_id]
        before = len(daemon.calls)
        await exchange.withdrawals.retry()
        assert len(daemon.calls) == before
        resumed = await exchange.withdrawals.retry_review(withdrawal.request_id)
        assert resumed.state == "needs_review"
        assert daemon.payments == 0
        assert exchange.ledger.balance("alice") == 6 * TRAI


@asynchronous
async def test_settlement_closes_withdrawals():
    async with running() as exchange:
        await exchange.fund("alice", "10")
        paid = await exchange.withdrawals.request("alice", "ttrai1external", "3")
        late = await exchange.withdrawals.request("alice", "ttrai1external", "2")
        beaten = await exchange.withdrawals.request("alice", "ttrai1external", "1")
        exchange.daemon.transactions[paid.transaction] |= {"lifecycle": "finalized", "finalized_cut": 100}
        exchange.daemon.transactions[late.transaction] |= {
            "lifecycle": "failed",
            "finalized_cut": 100,
            "failure": "no_effect",
        }
        exchange.daemon.transactions[beaten.transaction] |= {
            "lifecycle": "failed",
            "finalized_cut": 100,
            "failure": "conflict",
        }
        await exchange.event("settled", {"transaction": paid.transaction, "outcome": "final"})
        await exchange.event("settled", {"transaction": late.transaction, "outcome": "expired"})
        await exchange.event("settled", {"transaction": beaten.transaction, "outcome": "lost"})
        assert exchange.ledger.withdrawal(paid.request_id).state == "final"
        assert exchange.ledger.withdrawal(late.request_id).state == "expired"
        assert exchange.ledger.withdrawal(beaten.request_id).state == "expired"
        assert exchange.ledger.withdrawal(beaten.request_id).reason == "conflict"
        assert exchange.ledger.balance("alice") == 7 * TRAI


@asynchronous
async def test_a_restart_learns_what_became_of_sent_withdrawals():
    async with running() as exchange:
        daemon = exchange.daemon
        await exchange.fund("alice", "10")
        withdrawal = await exchange.withdrawals.request("alice", "ttrai1external", "3")
        daemon.transactions[withdrawal.transaction] |= {
            "lifecycle": "finalized",
            "finalized_cut": 100,
            "confirmations": 1,
        }
        await exchange.event(CONNECTED)
        assert exchange.ledger.withdrawal(withdrawal.request_id).state == "final"


def credit(ledger: Ledger, customer: str, atoms: int) -> None:
    transaction = f"{len(customer):096x}"
    ledger.add_address(f"ttrai1{customer}", customer)
    ledger.save_deposit(Deposit(transaction, 0, f"ttrai1{customer}", atoms, customer, None, None, True, 100, 1))
    assert ledger.credit(transaction, 0)


def test_two_processes_cannot_spend_one_balance(tmp_path):
    first, second = Ledger(tmp_path / "exchange.db"), Ledger(tmp_path / "exchange.db")
    try:
        credit(first, "alice", 10 * TRAI)
        assert first.add_withdrawal("one", "alice", "ttrai1external", 8 * TRAI) is not None
        assert second.add_withdrawal("two", "alice", "ttrai1external", 8 * TRAI) is None
        assert second.balance("alice") == 2 * TRAI
    finally:
        first.close()
        second.close()


def test_the_balance_check_waits_for_another_writer(tmp_path):
    ledger, other = Ledger(tmp_path / "exchange.db"), Ledger(tmp_path / "exchange.db")
    try:
        credit(ledger, "alice", 10 * TRAI)
        ledger._db.execute("PRAGMA busy_timeout = 100")
        other._db.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError):
            ledger.add_withdrawal("one", "alice", "ttrai1external", 1 * TRAI)
        other._db.execute("ROLLBACK")
        assert ledger.add_withdrawal("one", "alice", "ttrai1external", 1 * TRAI) is not None
    finally:
        ledger.close()
        other.close()


@asynchronous
async def test_lost_ack_then_restart_and_refusal_never_refunds_customer_credit(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger, "alice", 10 * TRAI)
            withdrawals = Withdrawals(client, ledger)
            daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "answer lost after payment", sent=True)
            withdrawal = await withdrawals.request("alice", "ttrai1external", "4")
            assert withdrawal.state == "requested"
            transaction = daemon.sent[withdrawal.request_id]["transaction"]
            assert daemon.payments == 1

        with Ledger(path) as ledger:
            # The restarted operator has the wrong token. This later refusal
            # says nothing about the successful payment whose answer was lost.
            async with DaemonClient(daemon.url, "wrong-token") as refused:
                await Withdrawals(refused, ledger).retry()
            held = ledger.withdrawal(withdrawal.request_id)
            assert held.state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            withdrawals = Withdrawals(client, ledger)
            before = len(daemon.calls)
            await withdrawals.retry()
            assert len(daemon.calls) == before
            with pytest.raises(WithdrawalError):
                await withdrawals.request("alice", "ttrai1external", "7")
            recovered = await withdrawals.retry_review(withdrawal.request_id)
            assert recovered.state == "submitted"
            assert recovered.transaction == transaction
            assert daemon.payments == 1
            assert ledger.balance("alice") == 6 * TRAI
            daemon.transactions[transaction] |= {"lifecycle": "finalized", "finalized_cut": 100}
            await withdrawals.reconcile()
            assert ledger.withdrawal(withdrawal.request_id).state == "final"
            assert ledger.balance("alice") == 6 * TRAI


@asynchronous
@pytest.mark.parametrize("capability", [None, False, 0, "true"])
async def test_automatic_withdrawals_require_the_literal_durable_capability(capability):
    async with running() as exchange:
        await exchange.fund("alice", "10")
        exchange.daemon.durable_request_ids = capability
        held = await exchange.withdrawals.request("alice", "ttrai1external", "4")
        assert held.state == "needs_review"
        assert exchange.daemon.payments == 0
        assert exchange.ledger.balance("alice") == 6 * TRAI
        exchange.daemon.durable_request_ids = True
        paid = await exchange.withdrawals.retry_review(held.request_id)
        assert paid.request_id == held.request_id
        assert paid.state == "submitted"
        assert exchange.daemon.payments == 1


def test_a_late_send_error_cannot_regress_a_known_receipt(tmp_path):
    with Ledger(tmp_path / "exchange.db") as ledger:
        credit(ledger, "alice", 10 * TRAI)
        assert ledger.add_withdrawal("original", "alice", "ttrai1external", 4 * TRAI)
        transaction = "ab" * 48
        ledger.update_withdrawal("original", "submitted", transaction=transaction)
        ledger.hold_withdrawal("original", "needs_review", "late refusal")
        assert ledger.withdrawal("original").state == "submitted"
        assert ledger.withdrawal("original").transaction == transaction
        ledger.update_withdrawal("original", "final")
        ledger.hold_withdrawal("original", "requested", "late timeout")
        assert ledger.withdrawal("original").state == "final"
        assert ledger.balance("alice") == 6 * TRAI


@asynchronous
async def test_an_unknown_settlement_event_cannot_refund_a_paid_withdrawal():
    async with running() as exchange:
        await exchange.fund("alice", "10")
        paid = await exchange.withdrawals.request("alice", "ttrai1external", "4")
        await exchange.event("settled", {"transaction": paid.transaction, "outcome": "unknown"})
        assert exchange.ledger.withdrawal(paid.request_id).state == "submitted"
        assert exchange.ledger.balance("alice") == 6 * TRAI


@asynchronous
@pytest.mark.parametrize("outcome", ["final", "expired"])
async def test_a_late_duplicate_http_receipt_cannot_regress_settlement(outcome):
    async with running() as exchange:
        await exchange.fund("alice", "10")
        daemon = exchange.daemon
        paused, resume = asyncio.Event(), asyncio.Event()
        daemon.pause_send_reply, daemon.resume_send_reply = paused, resume
        first = asyncio.create_task(exchange.withdrawals.request("alice", "ttrai1external", "4"))
        try:
            await asyncio.wait_for(paused.wait(), 3)
            # A concurrent retry recovers the same daemon receipt before the
            # first actual HTTP answer arrives. That receipt can then settle.
            await exchange.withdrawals.retry()
            submitted = exchange.ledger.withdrawals("submitted")[0]
            assert daemon.payments == 1
            daemon.transactions[submitted.transaction] |= {
                "lifecycle": "finalized" if outcome == "final" else "failed",
                "finalized_cut": 100,
                "failure": None if outcome == "final" else "no_effect",
            }
            await exchange.event("settled", {"transaction": submitted.transaction, "outcome": outcome})
            expected = "final" if outcome == "final" else "expired"
            balance = 6 * TRAI if outcome == "final" else 10 * TRAI
            assert exchange.ledger.withdrawal(submitted.request_id).state == expected
            resume.set()
            recovered = await asyncio.wait_for(first, 3)
            assert recovered.state == expected
            assert recovered.transaction == submitted.transaction
            assert exchange.ledger.balance("alice") == balance
            assert daemon.payments == 1
        finally:
            resume.set()
            if not first.done():
                await first


def test_a_receipt_cannot_replace_another_known_transaction(tmp_path):
    with Ledger(tmp_path / "exchange.db") as ledger:
        credit(ledger, "alice", 10 * TRAI)
        ledger.add_withdrawal("original", "alice", "ttrai1external", 4 * TRAI)
        assert ledger.record_receipt("original", "ab" * 48, 7)
        with pytest.raises(ValueError, match="changed its transaction ID"):
            ledger.record_receipt("original", "cd" * 48, 9)
        known = ledger.withdrawal("original")
        assert (known.state, known.transaction, known.fee) == ("submitted", "ab" * 48, 7)
        assert ledger.balance("alice") == 6 * TRAI


@pytest.mark.parametrize("net", [-7, 0, 7])
def test_lifetime_turnover_above_i64_keeps_exact_positive_and_negative_net(tmp_path, net):
    path = tmp_path / "exchange.db"
    with Ledger(path) as ledger:
        # Each row is at most the 200-million-TRAI supply cap. Repeated
        # circulation can make both gross totals exceed i64 independently.
        atoms = 20_000_000_000_000_000
        assert 512 * atoms > (1 << 63) - 1
        with contextlib.closing(sqlite3.connect(path)) as database, database:
            database.executemany(
                "INSERT INTO deposits(transaction_id,output,address,customer,amount,"
                "payload_read,height,confirmations,credited_at) "
                "VALUES(?,0,'ttrai1alice','alice',?,1,1,1,1)",
                ((f"{index:096x}", atoms) for index in range(512)),
            )
            database.executemany(
                "INSERT INTO withdrawals(request_id,customer,address,amount,subtract_fee,state,updated_at) "
                "VALUES(?,'alice','ttrai1external',?,0,'final',1)",
                ((str(index), atoms) for index in range(512)),
            )
            if net >= 0:
                database.execute(
                    "INSERT INTO deposits(transaction_id,output,address,customer,amount,"
                    "payload_read,height,confirmations,credited_at) "
                    "VALUES(?,0,'ttrai1alice','alice',?,1,1,1,1)",
                    ("ef" * 48, net),
                )
            else:
                database.execute(
                    "INSERT INTO withdrawals(request_id,customer,address,amount,subtract_fee,state,updated_at) "
                    "VALUES('extra','alice','ttrai1external',?,0,'final',1)",
                    (-net,),
                )
        assert ledger.balance("alice") == net
        if net > 0:
            assert ledger.add_withdrawal("spend-net", "alice", "ttrai1external", net)
            assert ledger.balance("alice") == 0
        assert ledger.add_withdrawal("overdraw", "alice", "ttrai1external", 1) is None
