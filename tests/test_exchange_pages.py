"""Finite active-row pages and real HTTP retry progress under continuous arrivals."""

from fake_daemon import TOKEN, FakeDaemon, asynchronous

from examples.exchange import Deposit, Ledger, Withdrawals
from tenrai_daemon import DaemonClient


def credit(ledger):
    ledger.save_deposit(Deposit("ab" * 48, 0, "ttrai1alice", 1_000_000_000, "alice", None, None, True, 100, 1))
    assert ledger.credit("ab" * 48, 0)


def test_indexed_pages_bound_rows_and_freeze_the_arrival_set(tmp_path):
    with Ledger(tmp_path / "exchange.db") as ledger:
        credit(ledger)
        original = [f"old-{index:04d}" for index in range(130)]
        for request in original:
            assert ledger.add_withdrawal(request, "alice", "ttrai1external", 1)
        traces = []
        ledger._db.set_trace_callback(traces.append)
        after, through, first = ledger.active_page(("requested",), "", 0)
        ledger._db.set_trace_callback(None)
        assert len(first) == 64
        assert after == ""
        assert through == 130
        query = next(sql for sql in traces if "INDEXED BY withdrawals_active_request" in sql)
        plan = [row[3] for row in ledger._db.execute("EXPLAIN QUERY PLAN " + query)]
        assert any("withdrawals_active_request" in description for description in plan)
        assert not any("TEMP B-TREE" in description for description in plan)
        after = first[-1].request_id
        inserted = after + ":arrival"
        assert ledger.add_withdrawal(inserted, "alice", "ttrai1external", 1)
        _, same_upper, second = ledger.active_page(("requested",), after, through)
        assert same_upper == through
        assert len(second) == 64
        assert inserted not in {row.request_id for row in second}
        _, _, third = ledger.active_page(("requested",), second[-1].request_id, through)
        assert [row.request_id for row in first + second + third] == original
        _, next_upper, _ = ledger.active_page(("requested",), third[-1].request_id, through)
        assert next_upper == 131


@asynchronous
async def test_real_http_retry_cursor_finishes_old_work_before_new_arrivals(tmp_path):
    async with FakeDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        with Ledger(tmp_path / "exchange.db") as ledger:
            credit(ledger)
            original = {f"old-{index:04d}" for index in range(130)}
            for request in sorted(original):
                assert ledger.add_withdrawal(request, "alice", "ttrai1external", 1)
            withdrawals = Withdrawals(client, ledger)
            for turn in range(130):
                before = set(daemon.sent)
                await withdrawals.retry()
                newly_sent = set(daemon.sent) - before
                assert 1 <= len(newly_sent) <= 64
                assert newly_sent <= original
                done = set(daemon.sent) & original
                if done == original:
                    break
                # Sort immediately after current progress: an unfenced query would pick this late arrival.
                arrival = max(done) + ":arrival-" + str(turn)
                assert ledger.add_withdrawal(arrival, "alice", "ttrai1external", 1)
            else:
                raise AssertionError("continuous arrivals starved older withdrawals")
            assert daemon.payments == 130
            await withdrawals.retry()
            assert any(":arrival-" in request for request in daemon.sent)
            assert len(daemon.sent) == daemon.payments


def test_operator_review_pagination_preserves_every_reserved_row_without_fetching_all(tmp_path):
    with Ledger(tmp_path / "exchange.db") as ledger:
        credit(ledger)
        expected = {f"review-{index:04d}" for index in range(145)}
        for request in sorted(expected):
            assert ledger.add_withdrawal(request, "alice", "ttrai1external", 1)
            ledger.hold_withdrawal(request, "needs_review", "retained original")
        withdrawals = Withdrawals(None, ledger)  # This read-only operator path does not use a daemon.
        seen, after, sizes = [], "", []
        while rows := withdrawals.needs_review(after=after):
            assert len(rows) <= 64
            sizes.append(len(rows))
            seen.extend(row.request_id for row in rows)
            after = rows[-1].request_id
        assert sizes == [64, 64, 17]
        assert len(seen) == len(set(seen)) == 145
        assert set(seen) == expected
        assert len(ledger.withdrawals("needs_review")) == 64
        assert ledger.balance("alice") == 1_000_000_000 - 145
