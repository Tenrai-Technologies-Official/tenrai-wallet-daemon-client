"""Archive-bound payment attempts over real HTTP and a reopened exchange SQLite book.

The daemon is an explicit independent effects/receipt stand-in. These tests do not
claim native signing, chain execution, browser durability or power-loss coverage.
"""

import sqlite3
from contextlib import closing

import pytest
from fake_daemon import TOKEN, FakeDaemon, asynchronous

from examples.exchange import Deposit, Ledger, Withdrawals
from tenrai_daemon import DaemonClient, DaemonError, ErrorCode, RequestScope

TRAI = 100_000_000


def credit(ledger):
    ledger.save_deposit(Deposit("ab" * 48, 0, "ttrai1alice", 10 * TRAI, "alice", None, None, True, 100, 1))
    assert ledger.credit("ab" * 48, 0)


def send_calls(daemon):
    return [params for method, params in daemon.calls if method == "send_many"]


@pytest.mark.parametrize(
    "changes",
    [
        {"durable_request_ids": False},
        {"durable_request_ids": 1},
        {"durable_request_ids": "true"},
        {"request_archive_id": "AB" * 32},
        {"request_archive_id": "a" * 63},
        {"account": True},
        {"account": -1},
        {"account": 1 << 32},
        {"account": "0"},
        {"chain_id": "AB" * 48},
        {"chain_id": "a" * 95},
    ],
)
def test_scope_requires_literal_capability_and_canonical_complete_identity(changes):
    status = {"durable_request_ids": True, "request_archive_id": "ab" * 32, "account": 0, "chain_id": "cd" * 48}
    with pytest.raises(ValueError):
        RequestScope.from_status({**status, **changes})


def test_scope_is_immutable_and_accepts_the_complete_u32_range():
    scope = RequestScope("ab" * 32, 0xFFFFFFFF, "cd" * 48)
    assert scope.account == 0xFFFFFFFF
    with pytest.raises(AttributeError):
        scope.account = 0


@pytest.mark.parametrize("field", ["durable_request_ids", "request_archive_id", "account", "chain_id"])
def test_partial_status_never_becomes_a_scope(field):
    status = {"durable_request_ids": True, "request_archive_id": "ab" * 32, "account": 0, "chain_id": "cd" * 48}
    del status[field]
    with pytest.raises(ValueError):
        RequestScope.from_status(status)


def test_error_reasons_are_bounded_optional_codes_and_never_boolean_rpc_codes():
    missing = DaemonError.from_reply(
        {
            "code": -32002,
            "message": "missing original",
            "data": {"retryable": False, "reason": "request_not_archived"},
        }
    )
    assert missing.outcome_unknown and missing.reason == "request_not_archived"
    assert not missing.retryable
    for reason in (None, True, 7, {}, [], "x" * 65, "has space", "\n", "\u2603"):
        error = DaemonError.from_reply({"code": True, "data": {"reason": reason, "retryable": "true"}})
        assert error.code is None
        assert error.reason is None
        assert not error.retryable
    other = DaemonError.from_reply({"code": -32002, "data": {"reason": "another_reason"}})
    assert other.outcome_unknown and other.reason == "another_reason"


@asynchronous
async def test_scoped_client_serializes_every_guard_and_exact_recipient():
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        first = await client.send_many(
            [("ttrai1external", "4")], request_id="scoped:1", scope=scope, recovery_only=False
        )
        retry = await client.send_many(
            [("ttrai1external", "4")], request_id="scoped:1", scope=scope, recovery_only=True
        )
        assert first["transaction"] == retry["transaction"]
        assert daemon.payments == 1
        sent = send_calls(daemon)
        assert [call["recovery_only"] for call in sent] == [False, True]
        for call in sent:
            assert call["expected_request_archive_id"] == scope.request_archive_id
            assert call["expected_account"] == scope.account
            assert call["expected_chain_id"] == scope.chain_id
            assert call["recipients"] == [{"address": "ttrai1external", "amount": "4.00000000"}]
        before = len(daemon.calls)
        for kwargs in (
            {"recovery_only": False, "request_id": "id"},
            {"scope": scope},
            {"scope": scope, "request_id": "has space"},
            {"scope": scope, "request_id": "id", "recovery_only": 1},
            {"scope": object(), "request_id": "id"},
        ):
            with pytest.raises(ValueError):
                await client.send_many([("ttrai1external", "4")], **kwargs)
        assert len(daemon.calls) == before


@asynchronous
@pytest.mark.parametrize("reason", [None, "request_not_archived"])
async def test_status_unknown_before_intent_cannot_be_misread_as_a_recovery_verdict(tmp_path, reason):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            daemon.fail_next_status(ErrorCode.OUTCOME_UNKNOWN, "status failed before payment", reason=reason)
            held = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            assert held.state == "requested"
            assert ledger.balance("alice") == 6 * TRAI
            assert ledger._db.execute("SELECT attempted FROM withdrawal_attempts").fetchone()[0] == 0
            assert send_calls(daemon) == []
            assert daemon.payments == 0
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            assert ledger.withdrawal(held.request_id).state == "submitted"
            assert ledger.balance("alice") == 6 * TRAI
            assert len(send_calls(daemon)) == 1
            assert send_calls(daemon)[0]["recovery_only"] is False
            assert send_calls(daemon)[0]["request_id"] == held.request_id
            assert daemon.payments == 1


@asynchronous
async def test_disk_intent_is_visible_before_http_effect_and_reopen_recovers_same_receipt(tmp_path):
    path = tmp_path / "exchange.db"
    observed = []
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:

        def inspect_intent(params):
            with closing(sqlite3.connect(path)) as independent:
                row = independent.execute(
                    "SELECT attempted,request_archive_id,account,chain_id FROM withdrawal_attempts WHERE request_id=?",
                    (params["request_id"],),
                ).fetchone()
            assert row == (1, daemon.request_archive_id, daemon.account, daemon.chain_id)
            observed.append(params["recovery_only"])

        daemon.before_send = inspect_intent
        with Ledger(path) as ledger:
            assert ledger._db.execute("PRAGMA synchronous").fetchone()[0] >= 2
            credit(ledger)
            daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "answer lost after effect", sent=True)
            first = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4", subtract_fee=True)
            assert first.state == "requested"
            transaction = daemon.sent[first.request_id]["transaction"]
            assert daemon.payments == 1
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            recovered = ledger.withdrawal(first.request_id)
            assert recovered.state == "submitted"
            assert recovered.transaction == transaction
            assert ledger.balance("alice") == 6 * TRAI
            assert daemon.payments == 1
            assert [call["request_id"] for call in send_calls(daemon)] == [first.request_id, first.request_id]
            assert all(call["subtract_fee"] is True for call in send_calls(daemon))
        assert observed == [False, True]


@asynchronous
@pytest.mark.parametrize(
    "field,value",
    [
        ("request_archive_id", "bb" * 32),
        ("account", 1),
        ("chain_id", "cc" * 48),
    ],
)
async def test_fresh_true_capability_on_a_different_backend_never_relabels_old_attempt(tmp_path, field, value):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "answer lost", sent=True)
            first = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            original = daemon.sent[first.request_id]["transaction"]
        setattr(daemon, field, value)
        daemon.sent.clear()  # Fresh backend receipt database; the independent paid effect remains.
        count = len(send_calls(daemon))
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            held = ledger.withdrawal(first.request_id)
            assert held.state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            assert len(send_calls(daemon)) == count
            assert original in daemon.transactions
            assert daemon.payments == 1


@asynchronous
async def test_same_archive_rollback_with_missing_receipt_is_recovery_only_and_never_pays_again(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "answer lost", sent=True)
            first = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
        daemon.sent.clear()  # Whole daemon pair restored to before the request, retaining the same archive ID.
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            assert send_calls(daemon)[-1]["recovery_only"] is True
            assert send_calls(daemon)[-1]["request_id"] == first.request_id
            assert ledger.withdrawal(first.request_id).state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            assert daemon.payments == 1


@asynchronous
@pytest.mark.parametrize("reason", [None, "another_reason"])
async def test_archived_transient_unknown_keeps_safe_retry_progress_instead_of_pausing(tmp_path, reason):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            daemon.fail_next_send(ErrorCode.OUTCOME_UNKNOWN, "first answer lost", sent=True)
            first = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            original = daemon.sent[first.request_id]["transaction"]
        daemon.recovery_unknowns = 1
        daemon.recovery_reason = reason
        with Ledger(path) as ledger:
            withdrawals = Withdrawals(client, ledger)
            await withdrawals.retry()
            assert ledger.withdrawal(first.request_id).state == "requested"
            assert ledger.balance("alice") == 6 * TRAI
            await withdrawals.retry()
            assert ledger.withdrawal(first.request_id).state == "submitted"
            assert ledger.withdrawal(first.request_id).transaction == original
            assert daemon.payments == 1
            assert [call["recovery_only"] for call in send_calls(daemon)] == [False, True, True]
            assert all(call["request_id"] == first.request_id for call in send_calls(daemon))


@asynchronous
async def test_daemon_scope_check_closes_the_status_to_post_switch_window(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:

        def swap_before_effect(_params):
            daemon.request_archive_id = "bb" * 32
            daemon.before_send = None

        daemon.before_send = swap_before_effect
        with Ledger(path) as ledger:
            credit(ledger)
            first = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            assert first.state == "needs_review"
            assert daemon.payments == 0
            assert ledger.balance("alice") == 6 * TRAI
            original_scope = ledger._db.execute("SELECT request_archive_id FROM withdrawal_attempts").fetchone()[0]
            assert original_scope != daemon.request_archive_id
            await Withdrawals(client, ledger).retry_review(first.request_id)
            assert len(send_calls(daemon)) == 1
            assert daemon.payments == 0


@asynchronous
async def test_crash_after_committed_intent_before_post_cannot_create_a_later_payment(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        with Ledger(path) as ledger:
            credit(ledger)
            original = ledger.add_withdrawal("crash-intent", "alice", "ttrai1external", 4 * TRAI)
            assert ledger.begin_attempt(original.request_id, scope) is False
            assert send_calls(daemon) == []
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            assert ledger.withdrawal(original.request_id).state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            assert send_calls(daemon)[0]["recovery_only"] is True
            assert daemon.payments == 0


@asynchronous
async def test_earlier_unbound_withdrawal_is_readable_but_never_guessed_safe_to_send(tmp_path):
    path = tmp_path / "exchange.db"
    with Ledger(path) as ledger:
        credit(ledger)
        ledger._db.execute(
            "INSERT INTO withdrawals(request_id,customer,address,amount,subtract_fee,state,updated_at) "
            "VALUES('earlier','alice','ttrai1external',?,0,'requested',1)",
            (4 * TRAI,),
        )
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            assert ledger.withdrawal("earlier") is not None
            await Withdrawals(client, ledger).retry()
            assert ledger.withdrawal("earlier").state == "needs_review"
            assert ledger._db.execute("SELECT COUNT(*) FROM withdrawal_attempts").fetchone()[0] == 0
            assert ledger.balance("alice") == 6 * TRAI
            assert send_calls(daemon) == []


@asynchronous
@pytest.mark.parametrize("action", ["IGNORE", "ABORT,'forced intent failure'"])
async def test_failed_or_ignored_attempt_commit_cannot_post(tmp_path, action):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(path) as ledger:
            credit(ledger)
            ledger._db.execute(
                "CREATE TRIGGER reject_attempt BEFORE UPDATE ON withdrawal_attempts "
                "BEGIN SELECT RAISE(" + action + "); END"
            )
            held = await Withdrawals(client, ledger).request("alice", "ttrai1external", "4")
            assert held.state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            assert ledger._db.execute("SELECT attempted FROM withdrawal_attempts").fetchone()[0] == 0
            assert send_calls(daemon) == []
            assert daemon.payments == 0


@pytest.mark.parametrize("action", ["IGNORE", "ABORT,'forced marker failure'"])
def test_new_withdrawal_and_unused_marker_are_atomic_even_when_insert_is_ignored(tmp_path, action):
    with Ledger(tmp_path / "exchange.db") as ledger:
        credit(ledger)
        ledger._db.execute(
            "CREATE TRIGGER reject_marker BEFORE INSERT ON withdrawal_attempts BEGIN SELECT RAISE(" + action + "); END"
        )
        with pytest.raises((ValueError, sqlite3.Error)):
            ledger.add_withdrawal("no-marker", "alice", "ttrai1external", 4 * TRAI)
        assert ledger.withdrawal("no-marker") is None
        assert ledger.balance("alice") == 10 * TRAI


def test_downgraded_durability_and_future_schema_fail_closed_without_relabeling(tmp_path):
    path = tmp_path / "exchange.db"
    with Ledger(path) as ledger:
        credit(ledger)
        ledger.add_withdrawal("intent", "alice", "ttrai1external", 4 * TRAI)
        ledger._db.execute("PRAGMA synchronous = NORMAL")
        with pytest.raises(ValueError, match="synchronous FULL"):
            ledger.begin_attempt("intent", RequestScope("ab" * 32, 0, "cd" * 48))
        assert ledger._db.execute("SELECT attempted FROM withdrawal_attempts").fetchone()[0] == 0
        ledger._db.execute("PRAGMA user_version = 2")
    with pytest.raises(ValueError, match="unsupported exchange ledger schema"):
        Ledger(path)
    with closing(sqlite3.connect(path)) as check:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 2


@asynchronous
async def test_damaged_existing_scope_stays_reserved_and_does_not_post(tmp_path):
    path = tmp_path / "exchange.db"
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        with Ledger(path) as ledger:
            credit(ledger)
            ledger.add_withdrawal("damaged", "alice", "ttrai1external", 4 * TRAI)
            assert ledger.begin_attempt("damaged", scope) is False
            ledger._db.execute("UPDATE withdrawal_attempts SET request_archive_id = 'bad' WHERE request_id='damaged'")
        with Ledger(path) as ledger:
            await Withdrawals(client, ledger).retry()
            assert ledger.withdrawal("damaged").state == "needs_review"
            assert ledger.balance("alice") == 6 * TRAI
            assert send_calls(daemon) == []
            assert daemon.payments == 0
