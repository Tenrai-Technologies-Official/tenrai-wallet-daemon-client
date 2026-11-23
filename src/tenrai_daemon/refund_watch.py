"""Types for wallet lock state and refund monitoring."""

from typing import NotRequired, TypedDict


class RefundWatchOutpoint(TypedDict):
    """Funded HTLC output with a 96-character lowercase transaction ID."""

    transaction_id: str
    output_index: int


class RefundWatchJob(TypedDict):
    """Current state of a daemon refund job."""

    job_id: str
    state: str
    refund_address: str
    refund_height: int
    max_fee_atoms: str
    amount_atoms: str
    attempt: int
    transaction: NotRequired[str | None]
    reason: NotRequired[str | None]
    account: NotRequired[int]
    outpoint: NotRequired[RefundWatchOutpoint]
    genesis_cut_id: NotRequired[str]


class RefundWatchList(TypedDict):
    jobs: list[RefundWatchJob]
    limit: int
    auto_refund_enabled: bool


class RefundWatchForgotten(TypedDict):
    forgotten: bool
    job_id: str


class WalletLockStatus(TypedDict):
    locked: bool
    unlock_for_seconds: NotRequired[int | None]


class WalletState(TypedDict):
    """Signing state and remaining unlock lease."""

    locked: bool
    unlock_remaining_seconds: int | None


def positive_atoms(value: str) -> str:
    """Validate a positive unsigned 128-bit atom amount."""
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isdigit()
        or value.startswith("0")
        or len(value) > 39
        or int(value) > (1 << 128) - 1
    ):
        raise ValueError("max_fee_atoms must be a canonical positive u128 decimal string")
    return value
