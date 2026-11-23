"""Bind a JSON-RPC reply to its request before exposing any outcome."""

from typing import Any

from .errors import DaemonError


def rpc_result(reply: Any, request_id: int | str) -> Any:
    if (
        not isinstance(reply, dict)
        or reply.get("jsonrpc") != "2.0"
        or type(reply.get("id")) is not type(request_id)
        or reply.get("id") != request_id
        or ("result" in reply) == ("error" in reply)
    ):
        raise DaemonError("the daemon's answer does not match its JSON-RPC request", retryable=True)
    if "error" in reply:
        error = reply["error"]
        if (
            not isinstance(error, dict)
            or type(error.get("code")) is not int
            or not isinstance(error.get("message"), str)
        ):
            raise DaemonError(f"the daemon's JSON-RPC error is malformed: {str(error)[:200]}", retryable=True)
        raise DaemonError.from_reply(error)
    return reply["result"]
