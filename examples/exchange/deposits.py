"""Deposit addresses and confirmed credits."""

import logging
import secrets
from collections.abc import Awaitable, Callable
from typing import Any

from tenrai_daemon import CONNECTED, DaemonClient, Event, from_atoms, to_atoms

from .ledger import Deposit, Ledger

log = logging.getLogger(__name__)

OnCredit = Callable[[Deposit], Awaitable[None]]

# Catch up over RPC after a gap in the event stream.
CATCH_UP = frozenset({CONNECTED, "resync", "resync_required"})


class DepositWatcher:
    """Books incoming payments and credits them after `confirmations` cut confirmations; one is already final.

    Customers pay to their own address or to the shared one with their reference as payload text.
    """

    def __init__(
        self,
        client: DaemonClient,
        ledger: Ledger,
        *,
        confirmations: int = 1,
        on_credit: OnCredit | None = None,
    ) -> None:
        if confirmations < 1:
            raise ValueError("confirmations start at 1, the finalizing cut itself")
        self.client = client
        self.ledger = ledger
        self.confirmations = confirmations
        self._on_credit = on_credit

    async def assign_address(self, customer: str) -> str:
        """Return the customer's assigned deposit address."""
        if (known := self.ledger.address_of(customer)) is not None:
            return known
        issued = await self.client.new_address(label=customer)
        self.ledger.add_address(issued["address"], customer)
        return issued["address"]

    async def assign_reference(self, customer: str) -> tuple[str, str]:
        """Return the shared address and the customer's payment reference."""
        address = self.ledger.shared_address()
        if address is None:
            address = (await self.client.new_address(label="shared deposits"))["address"]
            self.ledger.add_address(address, None, shared=True)
        reference = self.ledger.reference_of(customer) or self.ledger.add_reference(customer, _new_reference())
        return address, reference

    async def handle(self, event: Event) -> None:
        data = event.data
        if event.type in CATCH_UP:
            await self.catch_up()
            await self._drop_stale_incoming()
        elif event.type == "unconfirmed":
            if self._is_deposit(data["address"], data):
                customer = self._customer(data["address"], data)
                self.ledger.add_pending(data["transaction"], data["address"], customer, to_atoms(data["amount"]))
        elif event.type == "removed":
            self.ledger.clear_pending(data["transaction"])
        elif event.type == "finalized":
            for payment in data.get("payments", []):
                await self._book(data["address"], payment, height=data["height"], confirmations=1)
        elif event.type == "confirmations":
            await self._book(data["address"], data, height=data["height"], confirmations=data["confirmations"])

    async def catch_up(self) -> None:
        """Catch up from the cursor and recheck pending confirmations."""
        start = self.ledger.cursor()
        if (open_height := self.ledger.open_height()) is not None:
            start = min(start, open_height - 1)
        answer = await self.client.received(since_height=start)
        for row in answer["received"]:
            await self._book(row["address"], row, height=row["height"], confirmations=row["confirmations"])
        if answer.get("complete", True):
            self.ledger.set_cursor(answer["indexed_height"])

    async def _drop_stale_incoming(self) -> None:
        # Refresh mempool entries after reconnecting.
        answer = await self.client.incoming()
        self.ledger.keep_pending({payment["transaction"] for payment in answer["payments"]})

    async def _book(self, address: str, payment: dict[str, Any], *, height: int, confirmations: int) -> None:
        if not self._is_deposit(address, payment):
            return
        senders = payment.get("senders") or []
        self.ledger.clear_pending(payment["transaction"])
        self.ledger.save_deposit(
            Deposit(
                transaction=payment["transaction"],
                output=payment["output"],
                address=address,
                amount=to_atoms(payment["amount"]),
                customer=self._customer(address, payment),
                payload=payment.get("payload_text"),
                sender=senders[0] if senders else None,
                payload_read=bool(payment.get("payload_read", True)),
                height=height,
                confirmations=confirmations,
            )
        )
        deposit = self.ledger.deposit(payment["transaction"], payment["output"])
        if deposit is None or deposit.customer is None or not deposit.payload_read:
            return
        if deposit.confirmations >= self.confirmations and self.ledger.credit(deposit.transaction, deposit.output):
            log.info("credited %s TRAI to %s, %s", from_atoms(deposit.amount), deposit.customer, deposit.transaction)
            if self._on_credit is not None:
                await self._on_credit(deposit)

    def _is_deposit(self, address: str, payment: dict[str, Any]) -> bool:
        # Exclude wallet-generated outputs from customer deposits.
        if not self.ledger.is_ours(address) or payment.get("own"):
            return False
        if payment.get("source", "regular") != "regular":
            return False
        return not any(self.ledger.is_ours(sender) for sender in payment.get("senders") or [])

    def _customer(self, address: str, payment: dict[str, Any]) -> str | None:
        if not self.ledger.is_shared(address):
            return self.ledger.customer_of(address)
        reference = (payment.get("payload_text") or "").strip()
        return self.ledger.customer_of_reference(reference) if reference else None


def _new_reference() -> str:
    return "TR-" + secrets.token_hex(6).upper()
