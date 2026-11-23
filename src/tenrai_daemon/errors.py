"""Daemon errors and JSON-RPC error codes."""

from enum import IntEnum
from typing import Any


class ErrorCode(IntEnum):
    """JSON-RPC error codes of the wallet daemon."""

    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    UNKNOWN_METHOD = -32601
    INVALID_PARAMS = -32602
    REFUSED = -32000
    SERVICE_UNAVAILABLE = -32001
    OUTCOME_UNKNOWN = -32002
    WALLET_BUSY = -32003
    REJECTED = -32004
    WALLET_LOCKED = -32005


class DaemonError(Exception):
    """A failed call with an optional RPC code and retry classification."""

    def __init__(
        self, message: str, code: int | None = None, retryable: bool = False, *, reason: str | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code if isinstance(code, int) and not isinstance(code, bool) else None
        self.retryable = retryable
        self.reason = (
            reason
            if (isinstance(reason, str) and 0 < len(reason) <= 64 and all(0x21 <= ord(char) <= 0x7E for char in reason))
            else None
        )

    @classmethod
    def from_reply(cls, error: Any) -> "DaemonError":
        if not isinstance(error, dict):
            return cls(f"the daemon answered with an error it did not describe: {str(error)[:200]}")
        data = error.get("data")
        code = error.get("code")
        return cls(
            str(error.get("message", "the daemon gave no reason")),
            code=code if type(code) is int else None,
            retryable=isinstance(data, dict) and data.get("retryable") is True,
            reason=data.get("reason") if isinstance(data, dict) else None,
        )

    @property
    def outcome_unknown(self) -> bool:
        """Recover an ambiguous payment under its original request ID."""
        return self.code == ErrorCode.OUTCOME_UNKNOWN

    def __repr__(self) -> str:
        return f"DaemonError({self.message!r}, code={self.code}, retryable={self.retryable})"
