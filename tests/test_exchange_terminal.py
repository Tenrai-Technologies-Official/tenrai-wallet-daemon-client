"""Scoped terminal observations over real HTTP with explicit daemon/finality stand-ins."""

import asyncio

import pytest
from fake_daemon import TOKEN, FakeDaemon, asynchronous

from examples.exchange import Deposit, Ledger, Withdrawals
from tenrai_daemon import DaemonClient, Event

TRAI = 100_000_000


def credit(ledger):
    ledger.save_deposit(Deposit("ab" * 48, 0, "ttrai1alice", 10 * TRAI, "alice", None, None, True, 100, 1))
    assert ledger.credit("ab" * 48, 0)


def terminal(daemon, transaction, state):
    daemon.transactions[transaction] |= {
        "lifecycle": "finalized" if state == "final" else "failed",
        "finalized_cut": 100,
        "failure": None if state == "final" else "no_effect",
    }


@asynchronous
@pytest.mark.parametrize("state", ["final", "expired"])
async def test_same_archive_restart_before_bracket_can_settle_under_the_current_process(tmp_path, state):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            paid = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
        daemon.started_at_millis = 2
        daemon.height = 101
        terminal(daemon, paid.transaction, state)
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).reconcile()
            assert ledger.withdrawal(paid.request_id).state == state
            assert ledger.balance("alice") == (6 if state == "final" else 10) * TRAI
            assert daemon.payments == 1


@asynchronous
@pytest.mark.parametrize("outcome", ["expired", "lost", "final"])
async def test_raw_settled_event_does_not_release_processing_or_missing_original(tmp_path, outcome):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            await withdrawals.handle(Event("settled", {"transaction": paid.transaction, "outcome": outcome}))
            assert ledger.withdrawal(paid.request_id).state == "submitted"
            assert ledger.balance("alice") == 6 * TRAI
            daemon.transactions.pop(paid.transaction)
            await withdrawals.handle(Event("settled", {"transaction": paid.transaction, "outcome": outcome}))
            assert ledger.withdrawal(paid.request_id).state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            # Missing history is temporary: restore the same exact known receipt and reconcile it later.
            daemon.transactions[paid.transaction] = {"transaction": paid.transaction}
            terminal(daemon, paid.transaction, "final")
            await withdrawals.reconcile()
            assert ledger.withdrawal(paid.request_id).state == "final"
            assert daemon.payments == 1


@asynchronous
@pytest.mark.parametrize(
    "field,value",
    [
        ("request_archive_id", "bb" * 32),
        ("account", 1),
        ("chain_id", "cc" * 48),
        ("started_at_millis", 2),
        ("service", "http://127.0.0.1:19001"),
        ("identity", "ab" * 48),
        ("height", 99),
    ],
)
async def test_context_change_during_terminal_query_retains_original_receipt_and_debit(tmp_path, field, value):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, "expired")

            def switch():
                setattr(daemon, field, value)
                daemon.before_transaction = None

            daemon.before_transaction = switch
            await withdrawals.reconcile()
            retained = ledger.withdrawal(paid.request_id)
            assert retained.state == "needs_review"
            assert retained.transaction == paid.transaction
            assert ledger.balance("alice") == 6 * TRAI
            assert daemon.payments == 1


@asynchronous
@pytest.mark.parametrize("cut", [None, False, 0, -1, 101, "100", {"height": 100}])
async def test_failed_status_requires_the_actual_scalar_finalized_cut_inside_the_fresh_tip(tmp_path, cut):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, "expired")
            daemon.transactions[paid.transaction]["finalized_cut"] = cut
            await withdrawals.reconcile()
            assert ledger.withdrawal(paid.request_id).state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI


@asynchronous
@pytest.mark.parametrize(
    "mutate",
    [
        {"transaction": "ee" * 48},
        {"failure": "made_up"},
        {"lifecycle": "finalized", "failure": "conflict"},
    ],
)
async def test_wrong_or_incoherent_terminal_transaction_cannot_release_credit(tmp_path, mutate):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, "expired")
            daemon.transactions[paid.transaction] |= mutate
            await withdrawals.reconcile()
            assert ledger.withdrawal(paid.request_id).state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI


@asynchronous
@pytest.mark.parametrize("first_state,second_state", [("expired", "final"), ("final", "expired")])
async def test_late_actual_http_terminal_reply_cannot_overwrite_a_concurrent_terminal_observation(
    tmp_path, first_state, second_state
):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, first_state)
            paused, resume = asyncio.Event(), asyncio.Event()
            daemon.pause_transaction_reply, daemon.resume_transaction_reply = paused, resume
            earlier = asyncio.create_task(withdrawals.reconcile())
            try:
                await asyncio.wait_for(paused.wait(), 3)
                terminal(daemon, paid.transaction, second_state)
                # The second real HTTP query commits first while the old captured answer is deferred.
                with Ledger(path) as independent:
                    await Withdrawals(client, independent).reconcile()
                assert ledger.withdrawal(paid.request_id).state == second_state
                resume.set()
                await asyncio.wait_for(earlier, 3)
                assert ledger.withdrawal(paid.request_id).state == second_state
                assert ledger.balance("alice") == (6 if second_state == "final" else 10) * TRAI
                assert daemon.payments == 1
            finally:
                resume.set()
                if not earlier.done():
                    await earlier


@asynchronous
async def test_earlier_unbound_receipt_is_not_implicitly_reassigned_on_restart(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            paid = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            ledger._db.execute("DELETE FROM withdrawal_attempts WHERE request_id=?", (paid.request_id,))
        terminal(daemon, paid.transaction, "expired")
        before = len(daemon.calls)
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).reconcile()
            assert ledger.withdrawal(paid.request_id).state == "needs_review"
            assert ledger.withdrawal(paid.request_id).transaction == paid.transaction
            assert ledger.balance("alice") == 6 * TRAI
            assert len(daemon.calls) == before
            assert ledger._db.execute("SELECT COUNT(*) FROM withdrawal_attempts").fetchone()[0] == 0


@asynchronous
async def test_changed_persisted_binding_after_http_observation_cannot_commit_a_terminal_transition(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, "expired")
            paused, resume = asyncio.Event(), asyncio.Event()
            daemon.pause_transaction_reply, daemon.resume_transaction_reply = paused, resume
            reading = asyncio.create_task(withdrawals.reconcile())
            try:
                await asyncio.wait_for(paused.wait(), 3)
                with Ledger(path) as writer:
                    writer._db.execute(
                        "UPDATE withdrawal_attempts SET request_archive_id=? WHERE request_id=?",
                        ("bb" * 32, paid.request_id),
                    )
                resume.set()
                await asyncio.wait_for(reading, 3)
                assert ledger.withdrawal(paid.request_id).state == "needs_review"
                assert ledger.balance("alice") == 6 * TRAI
            finally:
                resume.set()
                if not reading.done():
                    await reading


@asynchronous
async def test_forward_progress_within_one_coherent_bracket_remains_usable(tmp_path):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            withdrawals = Withdrawals(client, ledger)
            paid = await withdrawals.request("alice", "ttrai1external", "4")
            terminal(daemon, paid.transaction, "expired")

            def advance():
                daemon.height = 101
                daemon.transactions[paid.transaction]["finalized_cut"] = 101
                daemon.before_transaction = None

            daemon.before_transaction = advance
            await withdrawals.reconcile()
            assert ledger.withdrawal(paid.request_id).state == "expired"
            assert ledger.balance("alice") == 10 * TRAI
            assert daemon.payments == 1
