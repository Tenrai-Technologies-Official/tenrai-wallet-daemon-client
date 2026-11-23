"""Customer withdrawals with durable daemon request IDs."""

import asyncio
import logging
import sqlite3
import uuid
from decimal import Decimal

from tenrai_daemon import CONNECTED, DaemonClient, DaemonError, Event, RequestScope, from_atoms, to_atoms

from .ledger import Ledger, Withdrawal

log = logging.getLogger(__name__)


class WithdrawalError(Exception):
    """Withdrawal rejected before daemon submission."""


class Withdrawals:
    """Stores each withdrawal and request ID before asking the hot-wallet daemon."""

    def __init__(self, client: DaemonClient, ledger: Ledger, *, fee: str = "normal") -> None:
        self.client = client
        self.ledger = ledger
        self.fee = fee
        self._retry_after = self._reconcile_after = ""
        self._retry_through = self._reconcile_through = 0
        self._retrying = self._reconciling = False

    async def request(
        self, customer: str, address: str, amount: str | Decimal, *, subtract_fee: bool = False
    ) -> Withdrawal:
        """Validate and book a withdrawal before submitting it."""
        atoms = to_atoms(amount)
        if atoms == 0:
            raise WithdrawalError("the amount must be above zero")
        checked = await self.client.validate_address(address)
        if not checked["valid"]:
            raise WithdrawalError(f"{address} is not an address of this network")
        if checked["mine"]:
            raise WithdrawalError("that address belongs to the exchange's own wallet")
        withdrawal = self.ledger.add_withdrawal(
            uuid.uuid4().hex, customer, checked["address"], atoms, subtract_fee=subtract_fee
        )
        if withdrawal is None:
            raise WithdrawalError(f"{customer} holds less than {from_atoms(atoms)} TRAI")
        return await self._send(withdrawal)

    async def retry(self) -> None:
        """Retry a bounded page with its original daemon bindings."""
        if self._retrying:
            return
        self._retrying = True
        try:
            self._retry_after, self._retry_through, rows = self.ledger.active_page(
                ("requested",), self._retry_after, self._retry_through
            )
            started = asyncio.get_running_loop().time()
            for withdrawal in rows:
                await self._send(withdrawal)
                self._retry_after = withdrawal.request_id
                if asyncio.get_running_loop().time() - started >= 10:
                    break
        finally:
            self._retrying = False

    def needs_review(self, *, after: str = "") -> list[Withdrawal]:
        """Page up to 64 reserved withdrawals needing review."""
        return self.ledger.withdrawals("needs_review", after=after)

    async def retry_review(self, request_id: str) -> Withdrawal:
        """Retry under the stored request ID after operator review."""
        withdrawal = self.ledger.withdrawal(request_id)
        if withdrawal is None or withdrawal.state != "needs_review":
            raise WithdrawalError("the request is not a withdrawal awaiting review")
        return await self._send(withdrawal)

    async def handle(self, event: Event) -> None:
        if event.type in (CONNECTED, "resync", "resync_required"):
            await self.reconcile()
        elif event.type == "settled":
            if event.data.get("outcome") not in ("final", "expired", "lost"):
                log.error("ignored an unknown withdrawal settlement outcome")
                return
            transaction = event.data.get("transaction")
            if (
                not isinstance(transaction, str)
                or len(transaction) != 96
                or any(char not in "0123456789abcdef" for char in transaction)
            ):
                log.error("ignored a withdrawal settlement with an invalid transaction ID")
                return
            withdrawal = self.ledger.withdrawal_for(transaction)
            if withdrawal is not None and withdrawal.state in ("submitted", "needs_review"):
                # Confirm socket notifications with a scoped RPC lookup.
                await self._reconcile_one(withdrawal)

    async def reconcile(self) -> None:
        """Reconcile one page of retained receipts."""
        if self._reconciling:
            return
        self._reconciling = True
        try:
            self._reconcile_after, self._reconcile_through, rows = self.ledger.active_page(
                ("submitted", "needs_review"), self._reconcile_after, self._reconcile_through
            )
            started = asyncio.get_running_loop().time()
            for withdrawal in rows:
                if withdrawal.transaction is not None:
                    await self._reconcile_one(withdrawal)
                self._reconcile_after = withdrawal.request_id
                if asyncio.get_running_loop().time() - started >= 10:
                    break
        finally:
            self._reconciling = False

    async def _reconcile_one(self, withdrawal: Withdrawal) -> None:
        transaction = withdrawal.transaction
        if transaction is None:
            return
        current = self.ledger.withdrawal(withdrawal.request_id)
        if current is None or current.transaction != transaction or current.state not in ("submitted", "needs_review"):
            return
        try:
            scope = self.ledger.attempt_scope(withdrawal.request_id, transaction)
            before_scope, before_tip, before_indexed, before_context = _snapshot(await self.client.status())
            if before_scope != scope:
                raise ValueError("the receipt belongs to another daemon archive, account or chain")
            view = await self.client.transaction(transaction)
            after_scope, after_tip, after_indexed, after_context = _snapshot(await self.client.status())
            if (
                after_scope != scope
                or before_context != after_context
                or after_tip < before_tip
                or after_indexed < before_indexed
            ):
                raise ValueError("the daemon context changed during receipt reconciliation")
            if not isinstance(view, dict) or view.get("transaction") != transaction:
                raise ValueError("the daemon answered for another withdrawal transaction")
            lifecycle = view.get("lifecycle")
            if lifecycle not in ("finalized", "failed"):
                return
            cut = view.get("finalized_cut")
            if type(cut) is not int or not 0 < cut <= after_tip:
                raise ValueError("the terminal withdrawal has no consistent finalized cut")
            failure = view.get("failure")
            if lifecycle == "finalized":
                if failure is not None:
                    raise ValueError("the finalized withdrawal also reports a failure")
                state, reason = "final", None
            else:
                if failure not in ("conflict", "invalid", "expired", "dropped", "no_effect"):
                    raise ValueError("the failed withdrawal has no recognized finalized failure")
                state, reason = "expired", failure
            if self.ledger.finish_withdrawal(withdrawal.request_id, transaction, scope, state, reason):
                log.info("withdrawal %s is %s", withdrawal.request_id, state)
        except (DaemonError, ValueError, sqlite3.Error) as error:
            log.warning("withdrawal %s retains its receipt for review: %s", withdrawal.request_id, error)
            self.ledger.review_receipt(withdrawal.request_id, transaction, str(error))

    async def _send(self, withdrawal: Withdrawal) -> Withdrawal:
        recovery_only = False
        try:
            scope = RequestScope.from_status(await self.client.status())
            recovery_only = self.ledger.begin_attempt(withdrawal.request_id, scope)
            sent = await self.client.send_many(
                [(withdrawal.address, from_atoms(withdrawal.amount))],
                fee=self.fee,
                request_id=withdrawal.request_id,
                subtract_fee=withdrawal.subtract_fee,
                scope=scope,
                recovery_only=recovery_only,
            )
        except (ValueError, sqlite3.Error) as error:
            # Require the original daemon binding and a committed attempt before POST.
            log.error("withdrawal %s needs review before submission: %s", withdrawal.request_id, error)
            self.ledger.hold_withdrawal(withdrawal.request_id, "needs_review", str(error))
        except DaemonError as error:
            if error.outcome_unknown and recovery_only and error.reason == "request_not_archived":
                # Keep the original debit reserved when its archived request is absent.
                log.error("withdrawal %s needs recovery review: %s", withdrawal.request_id, error.message)
                self.ledger.hold_withdrawal(withdrawal.request_id, "needs_review", error.message)
            elif error.retryable or error.outcome_unknown:
                log.warning("withdrawal %s waits for a retry: %s", withdrawal.request_id, error.message)
                self.ledger.hold_withdrawal(withdrawal.request_id, "requested", error.message)
            else:
                # Retain the debit while an earlier attempt may still settle.
                log.error("withdrawal %s needs review: %s", withdrawal.request_id, error.message)
                self.ledger.hold_withdrawal(withdrawal.request_id, "needs_review", error.message)
        else:
            try:
                installed = self.ledger.record_receipt(
                    withdrawal.request_id, sent["transaction"], to_atoms(sent["fee"])
                )
            except (KeyError, TypeError, ValueError, sqlite3.Error) as error:
                log.error("withdrawal %s has a conflicting receipt: %s", withdrawal.request_id, error)
                self.ledger.hold_withdrawal(withdrawal.request_id, "needs_review", str(error))
            else:
                if installed:
                    log.info("withdrawal %s sent as %s", withdrawal.request_id, sent["transaction"])
        updated = self.ledger.withdrawal(withdrawal.request_id)
        assert updated is not None
        return updated


def _snapshot(status: dict) -> tuple[RequestScope, int, int, tuple]:
    scope = RequestScope.from_status(status)
    tip = status.get("finalized_height")
    indexed = status.get("indexed_height")
    started = status.get("started_at_millis")
    if any(type(value) is not int or not 0 <= value <= (1 << 64) - 1 for value in (tip, indexed, started)):
        raise ValueError("the daemon status has an invalid finalized/indexed height or process start")
    if indexed > tip:
        raise ValueError("the daemon index is ahead of its finalized head")
    network, service, identity = status.get("network"), status.get("service"), status.get("identity")
    if network not in ("mainnet", "testnet", "testnet100", "development"):
        raise ValueError("the daemon status has an invalid network")
    if not isinstance(service, str) or not 0 < len(service) <= 8192:
        raise ValueError("the daemon status has no bounded service identity")
    if identity is not None and (
        not isinstance(identity, str)
        or not 0 < len(identity) <= 8192
        or any(char not in "0123456789abcdef" for char in identity)
    ):
        raise ValueError("the daemon status has an invalid optional node identity")
    return scope, tip, indexed, (network, started, service, identity)
