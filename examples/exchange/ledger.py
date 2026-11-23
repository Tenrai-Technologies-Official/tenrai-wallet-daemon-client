"""SQLite ledger for deposit addresses, credits and withdrawals.

Store amounts in atoms and credit each transaction output once.
"""

import contextlib
import sqlite3
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from tenrai_daemon import RequestScope

SCHEMA = """
CREATE TABLE IF NOT EXISTS addresses (
    address TEXT PRIMARY KEY,
    customer TEXT UNIQUE,            -- NULL for the shared address
    shared INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS payment_references (
    reference TEXT PRIMARY KEY,
    customer TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS pending (
    transaction_id TEXT NOT NULL,
    address TEXT NOT NULL,
    customer TEXT,
    amount INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, address)
);
CREATE TABLE IF NOT EXISTS deposits (
    transaction_id TEXT NOT NULL,
    output INTEGER NOT NULL,
    address TEXT NOT NULL,
    customer TEXT,                   -- NULL when nobody matched, see unassigned()
    amount INTEGER NOT NULL,
    payload TEXT,
    sender TEXT,
    payload_read INTEGER NOT NULL,
    height INTEGER NOT NULL,
    confirmations INTEGER NOT NULL,
    credited_at INTEGER,
    PRIMARY KEY (transaction_id, output)
);
CREATE TABLE IF NOT EXISTS withdrawals (
    request_id TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    address TEXT NOT NULL,
    amount INTEGER NOT NULL,
    subtract_fee INTEGER NOT NULL,   -- a retry must pay exactly what was asked for
    state TEXT NOT NULL,             -- requested, needs_review, submitted, final, expired, failed
    transaction_id TEXT,
    fee INTEGER,
    reason TEXT,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS withdrawals_active_request ON withdrawals(request_id)
    WHERE state IN ('requested', 'submitted', 'needs_review');
CREATE UNIQUE INDEX IF NOT EXISTS withdrawals_transaction ON withdrawals(transaction_id)
    WHERE transaction_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS withdrawal_attempts (
    request_id TEXT PRIMARY KEY REFERENCES withdrawals(request_id),
    attempted INTEGER NOT NULL CHECK (attempted IN (0, 1)),
    request_archive_id TEXT,
    account INTEGER,
    chain_id TEXT,
    CHECK ((attempted = 0 AND request_archive_id IS NULL AND account IS NULL AND chain_id IS NULL)
        OR (attempted = 1 AND request_archive_id IS NOT NULL AND account IS NOT NULL AND chain_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS settings (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


@dataclass(frozen=True)
class Deposit:
    transaction: str
    output: int
    address: str
    amount: int
    customer: str | None
    payload: str | None
    sender: str | None
    payload_read: bool
    height: int
    confirmations: int


@dataclass(frozen=True)
class Withdrawal:
    request_id: str
    customer: str
    address: str
    amount: int
    subtract_fee: bool
    state: str
    transaction: str | None
    fee: int | None
    reason: str | None


class Ledger:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self._db = sqlite3.connect(path, isolation_level=None)
        try:
            self._db.execute("PRAGMA synchronous = FULL")
            self._require_full_sync()
            version = self._db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("unsupported exchange ledger schema")
            self._db.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nPRAGMA user_version = 1;\nCOMMIT;")
        except BaseException:
            self._db.close()
            raise

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    # Addresses and payment references

    def add_address(self, address: str, customer: str | None, *, shared: bool = False) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO addresses (address, customer, shared) VALUES (?, ?, ?)",
            (address, customer, int(shared)),
        )

    def address_of(self, customer: str) -> str | None:
        return self._one("SELECT address FROM addresses WHERE customer = ?", customer)

    def customer_of(self, address: str) -> str | None:
        return self._one("SELECT customer FROM addresses WHERE address = ?", address)

    def is_ours(self, address: str) -> bool:
        return self._one("SELECT 1 FROM addresses WHERE address = ?", address) is not None

    def is_shared(self, address: str) -> bool:
        return self._one("SELECT shared FROM addresses WHERE address = ?", address) == 1

    def shared_address(self) -> str | None:
        return self._one("SELECT address FROM addresses WHERE shared = 1")

    def add_reference(self, customer: str, reference: str) -> str:
        self._db.execute("INSERT INTO payment_references (reference, customer) VALUES (?, ?)", (reference, customer))
        return reference

    def reference_of(self, customer: str) -> str | None:
        return self._one("SELECT reference FROM payment_references WHERE customer = ?", customer)

    def customer_of_reference(self, reference: str) -> str | None:
        return self._one("SELECT customer FROM payment_references WHERE reference = ?", reference)

    # Payments still in the mempool, for an "incoming" display only

    def add_pending(self, transaction: str, address: str, customer: str | None, amount: int) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO pending (transaction_id, address, customer, amount) VALUES (?, ?, ?, ?)",
            (transaction, address, customer, amount),
        )

    def clear_pending(self, transaction: str) -> None:
        self._db.execute("DELETE FROM pending WHERE transaction_id = ?", (transaction,))

    def keep_pending(self, transactions: set[str]) -> None:
        """Keep only the current incoming payments."""
        known = {row[0] for row in self._db.execute("SELECT DISTINCT transaction_id FROM pending")}
        for transaction in known - transactions:
            self.clear_pending(transaction)

    def pending(self, customer: str) -> int:
        return self._one("SELECT COALESCE(SUM(amount), 0) FROM pending WHERE customer = ?", customer)

    # Deposits

    def save_deposit(self, deposit: Deposit) -> None:
        """Upsert a deposit with monotonic confirmations."""
        self._db.execute(
            """
            INSERT INTO deposits (transaction_id, output, address, customer, amount, payload, sender,
                                  payload_read, height, confirmations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id, output) DO UPDATE SET
                customer = COALESCE(deposits.customer, excluded.customer),
                payload = COALESCE(excluded.payload, deposits.payload),
                sender = COALESCE(excluded.sender, deposits.sender),
                payload_read = MAX(deposits.payload_read, excluded.payload_read),
                confirmations = MAX(deposits.confirmations, excluded.confirmations)
            """,
            (
                deposit.transaction,
                deposit.output,
                deposit.address,
                deposit.customer,
                deposit.amount,
                deposit.payload,
                deposit.sender,
                int(deposit.payload_read),
                deposit.height,
                deposit.confirmations,
            ),
        )

    def credit(self, transaction: str, output: int) -> bool:
        """Return true only when crediting this output for the first time."""
        cursor = self._db.execute(
            """
            UPDATE deposits SET credited_at = ?
            WHERE transaction_id = ? AND output = ? AND credited_at IS NULL AND customer IS NOT NULL
            """,
            (int(time.time()), transaction, output),
        )
        return cursor.rowcount == 1

    def deposit(self, transaction: str, output: int) -> Deposit | None:
        row = self._db.execute(
            f"SELECT {_DEPOSIT} FROM deposits WHERE transaction_id = ? AND output = ?", (transaction, output)
        ).fetchone()
        return None if row is None else _deposit(row)

    def deposits(self, customer: str | None = None) -> list[Deposit]:
        rows = self._db.execute(
            f"SELECT {_DEPOSIT} FROM deposits WHERE ? IS NULL OR customer = ? ORDER BY height, transaction_id",
            (customer, customer),
        ).fetchall()
        return [_deposit(row) for row in rows]

    def credited(self, transaction: str, output: int) -> bool:
        return (
            self._one("SELECT credited_at FROM deposits WHERE transaction_id = ? AND output = ?", transaction, output)
            is not None
        )

    def open_height(self) -> int | None:
        """Oldest deposit awaiting confirmations or a payload."""
        return self._one(
            """
            SELECT MIN(height) FROM deposits
            WHERE credited_at IS NULL AND (payload_read = 0 OR customer IS NOT NULL)
            """
        )

    def unassigned(self) -> list[Deposit]:
        """Deposits without a matched customer."""
        rows = self._db.execute(
            f"SELECT {_DEPOSIT} FROM deposits WHERE customer IS NULL AND payload_read = 1 ORDER BY height"
        ).fetchall()
        return [_deposit(row) for row in rows]

    # Withdrawals

    def add_withdrawal(
        self, request_id: str, customer: str, address: str, amount: int, *, subtract_fee: bool = False
    ) -> Withdrawal | None:
        """Check the balance and book the withdrawal atomically."""
        with self._write():
            if self.balance(customer) < amount:
                return None
            inserted = self._db.execute(
                """
                INSERT INTO withdrawals (request_id, customer, address, amount, subtract_fee, state, updated_at)
                VALUES (?, ?, ?, ?, ?, 'requested', ?)
                """,
                (request_id, customer, address, amount, int(subtract_fee), int(time.time())),
            )
            if inserted.rowcount != 1:
                raise ValueError("the withdrawal was not booked")
            marker = self._db.execute(
                "INSERT INTO withdrawal_attempts(request_id, attempted) VALUES (?, 0)", (request_id,)
            )
            if marker.rowcount != 1:
                raise ValueError("the withdrawal attempt marker was not booked")
        withdrawal = self.withdrawal(request_id)
        assert withdrawal is not None
        return withdrawal

    def begin_attempt(self, request_id: str, scope: RequestScope) -> bool:
        """Commit the attempt before POST. Later calls return true for recovery-only."""
        if not isinstance(scope, RequestScope):
            raise ValueError("a complete daemon request scope is required")
        with self._write():
            withdrawal = self.withdrawal(request_id)
            if withdrawal is None or withdrawal.state not in ("requested", "needs_review"):
                raise ValueError("the withdrawal is no longer awaiting submission")
            row = self._db.execute(
                "SELECT attempted, request_archive_id, account, chain_id FROM withdrawal_attempts WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValueError("the earlier withdrawal has no durable attempt binding; retain it for review")
            attempted, archive, account, chain = row
            if type(attempted) is not int or attempted not in (0, 1):
                raise ValueError("the withdrawal attempt marker is damaged")
            if attempted == 1:
                if RequestScope(archive, account, chain) != scope:
                    raise ValueError("the daemon archive, account or chain changed; retain the original request")
                return True
            if any(value is not None for value in (archive, account, chain)):
                raise ValueError("the unused withdrawal attempt marker is damaged")
            updated = self._db.execute(
                """
                UPDATE withdrawal_attempts SET attempted = 1, request_archive_id = ?, account = ?, chain_id = ?
                WHERE request_id = ? AND attempted = 0
                  AND request_archive_id IS NULL AND account IS NULL AND chain_id IS NULL
                """,
                (scope.request_archive_id, scope.account, scope.chain_id, request_id),
            )
            if updated.rowcount != 1:
                raise ValueError("the first withdrawal attempt was not durably marked")
        return False

    def update_withdrawal(
        self,
        request_id: str,
        state: str,
        *,
        transaction: str | None = None,
        fee: int | None = None,
        reason: str | None = None,
    ) -> None:
        self._db.execute(
            """
            UPDATE withdrawals
            SET state = ?, transaction_id = COALESCE(?, transaction_id), fee = COALESCE(?, fee),
                reason = ?, updated_at = ?
            WHERE request_id = ?
            """,
            (state, transaction, fee, reason, int(time.time()), request_id),
        )

    def hold_withdrawal(self, request_id: str, state: str, reason: str) -> None:
        """Keep the debit reserved and preserve any receipt committed concurrently."""
        if state not in ("requested", "needs_review"):
            raise ValueError("a send error must keep the withdrawal reserved")
        self._db.execute(
            """
            UPDATE withdrawals SET state = ?, reason = ?, updated_at = ?
            WHERE request_id = ? AND transaction_id IS NULL AND state IN ('requested', 'needs_review')
            """,
            (state, reason, int(time.time()), request_id),
        )

    def attempt_scope(self, request_id: str, transaction: str) -> RequestScope:
        """Read the saved daemon binding for this exact withdrawal receipt."""
        row = self._db.execute(
            """
            SELECT a.attempted, a.request_archive_id, a.account, a.chain_id
            FROM withdrawal_attempts a JOIN withdrawals w ON w.request_id = a.request_id
            WHERE w.request_id = ? AND w.transaction_id = ? AND w.state IN ('submitted', 'needs_review')
            """,
            (request_id, transaction),
        ).fetchone()
        if row is None or type(row[0]) is not int or row[0] != 1:
            raise ValueError("the receipted withdrawal has no durable attempted scope")
        return RequestScope(row[1], row[2], row[3])

    def review_receipt(self, request_id: str, transaction: str, reason: str) -> None:
        """Pause reconciliation while retaining the receipt and debit."""
        self._db.execute(
            """
            UPDATE withdrawals SET state = 'needs_review', reason = ?, updated_at = ?
            WHERE request_id = ? AND transaction_id = ? AND state IN ('submitted', 'needs_review')
            """,
            (reason[:512], int(time.time()), request_id, transaction),
        )

    def finish_withdrawal(
        self, request_id: str, transaction: str, scope: RequestScope, state: str, reason: str | None = None
    ) -> bool:
        """Record a terminal observation once under its original scope."""
        if state not in ("final", "expired"):
            raise ValueError("unsupported withdrawal settlement")
        with self._write():
            current = self.withdrawal(request_id)
            if (
                current is None
                or current.transaction != transaction
                or current.state not in ("submitted", "needs_review")
            ):
                return False
            if self.attempt_scope(request_id, transaction) != scope:
                raise ValueError("the withdrawal attempt changed during settlement")
            changed = self._db.execute(
                """
                UPDATE withdrawals SET state = ?, reason = ?, updated_at = ?
                WHERE request_id = ? AND transaction_id = ? AND state IN ('submitted', 'needs_review')
                  AND EXISTS (SELECT 1 FROM withdrawal_attempts a
                    WHERE a.request_id = withdrawals.request_id AND a.attempted = 1
                      AND a.request_archive_id = ? AND a.account = ? AND a.chain_id = ?)
                """,
                (
                    state,
                    reason,
                    int(time.time()),
                    request_id,
                    transaction,
                    scope.request_archive_id,
                    scope.account,
                    scope.chain_id,
                ),
            )
            if changed.rowcount not in (0, 1):
                raise ValueError("the terminal withdrawal update changed multiple rows")
            return changed.rowcount == 1

    def record_receipt(self, request_id: str, transaction: str, fee: int) -> bool:
        """Save the immutable receipt while preserving settlement."""
        if (
            not isinstance(transaction, str)
            or len(transaction) != 96
            or any(char not in "0123456789abcdef" for char in transaction)
        ):
            raise ValueError("the withdrawal receipt has a noncanonical transaction ID")
        if type(fee) is not int or not 0 <= fee <= (1 << 63) - 1:
            raise ValueError("the withdrawal receipt fee exceeds ledger bounds")
        cursor = self._db.execute(
            """
            UPDATE withdrawals
            SET state = 'submitted', transaction_id = ?, fee = COALESCE(fee, ?), reason = NULL, updated_at = ?
            WHERE request_id = ? AND state IN ('requested', 'needs_review', 'submitted')
              AND (transaction_id IS NULL OR transaction_id = ?)
            """,
            (transaction, fee, int(time.time()), request_id, transaction),
        )
        row = self.withdrawal(request_id)
        if row is None:
            raise ValueError("the withdrawal receipt has no stored request")
        if row.transaction is not None and row.transaction != transaction:
            raise ValueError("the withdrawal receipt changed its transaction ID")
        return cursor.rowcount == 1

    def withdrawal(self, request_id: str) -> Withdrawal | None:
        row = self._db.execute(f"SELECT {_WITHDRAWAL} FROM withdrawals WHERE request_id = ?", (request_id,)).fetchone()
        return None if row is None else _withdrawal(row)

    def withdrawal_for(self, transaction: str) -> Withdrawal | None:
        row = self._db.execute(
            f"SELECT {_WITHDRAWAL} FROM withdrawals WHERE transaction_id = ?", (transaction,)
        ).fetchone()
        return None if row is None else _withdrawal(row)

    def withdrawals(self, *states: str, after: str = "", limit: int = 64) -> list[Withdrawal]:
        """Page review rows by request ID."""
        if not states or not isinstance(after, str) or type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("invalid withdrawal page")
        marks = ", ".join("?" for _ in states)
        active = all(state in ("requested", "submitted", "needs_review") for state in states)
        index = " INDEXED BY withdrawals_active_request" if active else ""
        predicate = "state IN ('requested', 'submitted', 'needs_review') AND " if active else ""
        rows = self._db.execute(
            f"SELECT {_WITHDRAWAL} FROM withdrawals{index} WHERE {predicate}state IN ({marks}) "
            "AND request_id > ? ORDER BY request_id LIMIT ?",
            (*states, after, limit),
        )
        return [_withdrawal(row) for row in rows]

    def active_page(self, states: tuple[str, ...], after: str, through: int) -> tuple[str, int, list[Withdrawal]]:
        """Page up to the cycle's fixed insertion bound."""
        if (
            not states
            or any(state not in ("requested", "submitted", "needs_review") for state in states)
            or not isinstance(after, str)
            or type(through) is not int
            or not 0 <= through <= (1 << 63) - 1
        ):
            raise ValueError("invalid active withdrawal page")

        def page() -> list[Withdrawal]:
            marks = ", ".join("?" for _ in states)
            rows = self._db.execute(
                f"SELECT {_WITHDRAWAL} FROM withdrawals INDEXED BY withdrawals_active_request "
                "WHERE state IN ('requested', 'submitted', 'needs_review') "
                f"AND state IN ({marks}) AND request_id > ? AND rowid <= ? ORDER BY request_id LIMIT 64",
                (*states, after, through),
            )
            return [_withdrawal(row) for row in rows]

        if through == 0:
            after, through = "", self._one("SELECT COALESCE(MAX(rowid), 0) FROM withdrawals")
        rows = page()
        if not rows and through > 0:
            after, through = "", self._one("SELECT COALESCE(MAX(rowid), 0) FROM withdrawals")
            rows = page()
        return after, through, rows

    # Balances and the catch-up cursor

    def balance(self, customer: str) -> int:
        """Credited deposits less every withdrawal that has not expired or failed."""
        # Sum in Python to support gross totals beyond SQLite's signed integer range.
        credited = sum(
            row[0]
            for row in self._db.execute(
                "SELECT amount FROM deposits WHERE customer = ? AND credited_at IS NOT NULL", (customer,)
            )
        )
        withdrawn = sum(
            row[0]
            for row in self._db.execute(
                "SELECT amount FROM withdrawals WHERE customer = ? AND state NOT IN ('expired', 'failed')",
                (customer,),
            )
        )
        return credited - withdrawn

    def cursor(self) -> int:
        return self._one("SELECT value FROM settings WHERE name = 'received_after'") or 0

    def set_cursor(self, height: int) -> None:
        self._db.execute(
            "INSERT INTO settings (name, value) VALUES ('received_after', ?) "
            "ON CONFLICT (name) DO UPDATE SET value = MAX(settings.value, excluded.value)",
            (height,),
        )

    def _require_full_sync(self) -> None:
        row = self._db.execute("PRAGMA synchronous").fetchone()
        if row is None or type(row[0]) is not int or row[0] < 2:
            raise ValueError("exchange monetary writes require SQLite synchronous FULL")

    @contextlib.contextmanager
    def _write(self) -> Iterator[None]:
        self._require_full_sync()
        # Acquire the write lock before checking the balance.
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _one(self, query: str, *params: object):
        row = self._db.execute(query, params).fetchone()
        return None if row is None else row[0]


_DEPOSIT = "transaction_id, output, address, amount, customer, payload, sender, payload_read, height, confirmations"
_WITHDRAWAL = "request_id, customer, address, amount, subtract_fee, state, transaction_id, fee, reason"


def _deposit(row: tuple) -> Deposit:
    *fields, payload_read, height, confirmations = row
    return Deposit(*fields, bool(payload_read), height, confirmations)


def _withdrawal(row: tuple) -> Withdrawal:
    request_id, customer, address, amount, subtract_fee, *rest = row
    return Withdrawal(request_id, customer, address, amount, bool(subtract_fee), *rest)
