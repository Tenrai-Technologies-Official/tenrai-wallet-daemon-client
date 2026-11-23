"""A stand-in for the wallet daemon: its JSON-RPC and socket protocol over a scripted wallet."""

import asyncio
import functools
import itertools
import json
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import WSMsgType, web

from tenrai_daemon import from_atoms, to_atoms

TOKEN = "test-token"
FEE = "0.00010000"
WORDS = "windmill talc frosty fjord frothy sorghum"
EVENTS = {
    "live",
    "reconnecting",
    "resync",
    "watching",
    "unconfirmed",
    "removed",
    "finalized",
    "confirmed",
    "confirmations",
    "balance",
    "status",
    "settled",
    "sent",
    "address",
}


def asynchronous(test: Callable[..., Awaitable[None]]) -> Callable[..., None]:
    """Runs an async test in its own event loop, no pytest plugin needed."""

    @functools.wraps(test)
    def run(*args: Any, **kwargs: Any) -> None:
        asyncio.run(test(*args, **kwargs))

    return run


class FakeDaemon:
    def __init__(self) -> None:
        self.height = 100
        self.labels: dict[str, str | None] = {}
        self.received: list[dict[str, Any]] = []
        self.incoming: list[dict[str, Any]] = []
        self.transactions: dict[str, dict[str, Any]] = {}
        self.sent: dict[str, dict[str, Any]] = {}
        self.payments = 0
        self.durable_request_ids = True
        self.request_archive_id = "a1" * 32
        self.account = 0
        self.chain_id = "de" * 48
        self.before_send: Callable[[dict[str, Any]], None] | None = None
        self.recovery_unknowns = 0
        self.recovery_reason: str | None = None
        self.started_at_millis = 1
        self.service = "http://127.0.0.1:19000"
        self.identity: str | None = None
        self.before_transaction: Callable[[], None] | None = None
        self.pause_transaction_reply: asyncio.Event | None = None
        self.resume_transaction_reply: asyncio.Event | None = None
        self.pause_send_reply: asyncio.Event | None = None
        self.resume_send_reply: asyncio.Event | None = None
        self.garbled_subscriptions = 0
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._failures: list[tuple[dict[str, Any], bool]] = []
        self._status_failures: list[dict[str, Any]] = []
        self._sockets: dict[web.WebSocketResponse, set[str] | None] = {}
        self._ids = itertools.count(1)
        self._runner: web.AppRunner | None = None
        self.url = ""

    async def __aenter__(self) -> "FakeDaemon":
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_post("/v1/rpc", self._rpc)
        app.router.add_get("/v1/ws", self._socket)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.drop_sockets()
        assert self._runner is not None
        await self._runner.cleanup()

    # Scripting the wallet

    def address(self, label: str | None = None) -> str:
        address = f"ttrai1{next(self._ids):060d}"
        self.labels[address] = label
        return address

    def pay(
        self,
        address: str,
        amount: str,
        *,
        payload: str | None = None,
        senders: tuple[str, ...] = ("ttrai1customer",),
        source: str = "regular",
        payload_read: bool = True,
        height: int | None = None,
    ) -> dict[str, Any]:
        """A finalized payment to `address`, as `received` and the events report it."""
        payment = {
            "address": address,
            "label": self.labels.get(address),
            "transaction": f"{next(self._ids):096x}",
            "output": 0,
            "amount": from_atoms(to_atoms(amount)),
            "source": source,
            "height": self.height if height is None else height,
            "payload_hex": payload.encode().hex() if payload and payload_read else None,
            "payload_text": payload if payload_read else None,
            "senders": list(senders) if payload_read else [],
            "payload_read": payload_read,
        }
        self.received.append(payment)
        return payment

    def fail_next_send(self, code: int, message: str, *, retryable: bool = False, sent: bool = False) -> None:
        """`sent` pays anyway before failing, like an answer lost on its way back."""
        self._failures.append(({"code": code, "message": message, "data": {"retryable": retryable}}, sent))

    def fail_next_status(self, code: int, message: str, *, reason: str | None = None) -> None:
        data = {"retryable": False, **({"reason": reason} if reason is not None else {})}
        self._status_failures.append({"code": code, "message": message, "data": data})

    async def push(self, kind: str, data: dict[str, Any]) -> None:
        for socket, wanted in list(self._sockets.items()):
            if wanted is None or kind in wanted:
                params = {"subscription": 1, "type": kind, "data": data}
                await socket.send_str(json.dumps({"jsonrpc": "2.0", "method": "event", "params": params}))

    async def push_raw(self, text: str) -> None:
        for socket in list(self._sockets):
            await socket.send_str(text)

    async def drop_sockets(self) -> None:
        for socket in list(self._sockets):
            await socket.close()
        self._sockets.clear()

    @property
    def subscribed(self) -> int:
        return len(self._sockets)

    # The daemon's side

    async def _health(self, _: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "live": True})

    async def _rpc(self, request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return web.json_response({"error": "a valid bearer token is required"}, status=401)
        call = await request.json()
        params = call.get("params") or {}
        self.calls.append((call["method"], params))
        try:
            result = self._answer(call["method"], params)
        except _Refused as refused:
            return web.json_response({"jsonrpc": "2.0", "id": call["id"], "error": refused.error})
        if call["method"] == "transaction" and self.pause_transaction_reply is not None:
            result = json.loads(json.dumps(result))
            paused, resume = self.pause_transaction_reply, self.resume_transaction_reply
            self.pause_transaction_reply = self.resume_transaction_reply = None
            paused.set()
            assert resume is not None
            await resume.wait()
        if call["method"] in ("send", "send_many") and self.pause_send_reply is not None:
            paused, resume = self.pause_send_reply, self.resume_send_reply
            self.pause_send_reply = self.resume_send_reply = None
            paused.set()
            assert resume is not None
            await resume.wait()
        return web.json_response({"jsonrpc": "2.0", "id": call["id"], "result": result})

    async def _socket(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return web.json_response({"error": "a valid bearer token is required"}, status=401)
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        async for message in socket:
            if message.type is not WSMsgType.TEXT:
                continue
            call = json.loads(message.data)
            names = (call.get("params") or {}).get("events")
            if self.garbled_subscriptions > 0:
                self.garbled_subscriptions -= 1
                await socket.send_str("not a reply")
            elif call.get("method") != "subscribe":
                await socket.send_json({"jsonrpc": "2.0", "id": call.get("id"), "error": _error(-32601, "unknown")})
            elif names is not None and not EVENTS.issuperset(names):
                await socket.send_json({"jsonrpc": "2.0", "id": call["id"], "error": _error(-32602, "unknown event")})
            else:
                self._sockets[socket] = None if names is None else set(names)
                await socket.send_json({"jsonrpc": "2.0", "id": call["id"], "result": {"subscription": 1}})
        self._sockets.pop(socket, None)
        return socket

    def _answer(self, method: str, params: dict[str, Any]) -> Any:
        if method == "status" and self._status_failures:
            raise _Refused(**self._status_failures.pop(0))
        match method:
            case "status":
                return {
                    "version": "test",
                    "network": "testnet",
                    "finalized_height": self.height,
                    "indexed_height": self.height,
                    "started_at_millis": self.started_at_millis,
                    "service": self.service,
                    "identity": self.identity,
                    "live": True,
                    "pending_sends": 0,
                    "pending_compounds": 0,
                    "request_archive_id": self.request_archive_id,
                    "account": self.account,
                    "chain_id": self.chain_id,
                    **(
                        {"durable_request_ids": self.durable_request_ids}
                        if self.durable_request_ids is not None
                        else {}
                    ),
                }
            case "outputs":
                rows = [
                    {"amount": "5.00000000", "reserved": False},
                    {"amount": "9.00000000", "reserved": True},
                    {"amount": "1.00000000", "reserved": False},
                ]
                if "largest" in params:
                    free = [row for row in rows if not row["reserved"]]
                    rows = sorted(free, key=lambda row: to_atoms(row["amount"]), reverse=True)[: params["largest"]]
                return {"finalized_height": self.height, "count": len(rows), "complete": True, "outputs": rows}
            case "balance":
                return {"balance": "0.00000000", "available": "0.00000000", "finalized_height": self.height}
            case "new_address":
                address = self.address(params.get("label"))
                return {"address": address, "payment_uri": f"trai-test:{address}", "label": params.get("label")}
            case "validate_address":
                address = params["address"]
                if not address.startswith("ttrai1"):
                    return {"valid": False}
                return {"valid": True, "address": address, "verification_words": WORDS, "mine": address in self.labels}
            case "received":
                since = params.get("since_height", 0)
                rows = [
                    {**row, "confirmations": self.height - row["height"] + 1}
                    for row in self.received
                    if row["height"] > since
                ]
                return {"since_height": since, "indexed_height": self.height, "complete": True, "received": rows}
            case "incoming":
                total = sum(to_atoms(payment["amount"]) for payment in self.incoming)
                return {"unconfirmed_incoming": from_atoms(total), "payments": self.incoming}
            case "transaction":
                if self.before_transaction is not None:
                    self.before_transaction()
                if params["transaction"] not in self.transactions:
                    raise _Refused(-32000, "the service answered 404: unknown transaction")
                return self.transactions[params["transaction"]]
            case "send":
                return self._send(params)
            case "private_balance":
                return {"private_balance": "12.5", "height": 90, "notes": [{"note": 4, "value": "12.5"}]}
            case "private_status":
                return {"active": True, "fee": "0.33831080"}
            case "private_register":
                return {"private_address": "dtrai1private", "registered": params.get("wait", False)}
            case "private_enter" | "private_transfer":
                return {"statement": "ab" * 48, "amount": params["amount"], "fee": "0.33831080"}
            case "private_exit":
                return {"statement": "ab" * 48, "paid": "1", "fee": "0.33831080"}
            case "rescan":
                depth = params.get("depth", 1000)
                return {"depth": depth, "added": 0, "addresses": 40}
            case "proof_request":
                return {
                    "type": "tenrai-address-proof-request",
                    "version": 1,
                    "address": params["address"],
                    "challenge": "ab" * 32,
                    "expires_at": 1_900_000_000 + params.get("valid_for", 600),
                }
            case "prove_address":
                if params["request"]["address"] not in self.labels:
                    raise _Refused(-32602, "the address is not one this wallet tracks")
                return {
                    **params["request"],
                    "type": "tenrai-address-proof",
                    "public_key": "11" * 1952,
                    "signature": "22" * 3309,
                }
            case "verify_address_proof":
                proof, request = params["proof"], params.get("request")
                if request is not None and request["challenge"] != proof["challenge"]:
                    return {"valid": False, "reason": "the proof answers another request"}
                return {
                    "valid": True,
                    "address": proof["address"],
                    "verification_words": WORDS,
                    "expires_at": proof["expires_at"],
                }
            case "send_many":
                total = sum(to_atoms(recipient["amount"]) for recipient in params["recipients"])
                return self._send({**params, "to": params["recipients"][0]["address"], "amount": from_atoms(total)})
            case _:
                raise _Refused(-32601, f"unknown method {method}")

    def _send(self, params: dict[str, Any]) -> dict[str, Any]:
        request_id = params.get("request_id")
        if self.before_send is not None:
            self.before_send(params)
        guarded = any(
            name in params
            for name in ("expected_request_archive_id", "expected_account", "expected_chain_id", "recovery_only")
        )
        if guarded:
            if (
                not request_id
                or params.get("expected_request_archive_id") != self.request_archive_id
                or type(params.get("expected_account")) is not int
                or params["expected_account"] != self.account
                or params.get("expected_chain_id") != self.chain_id
                or type(params.get("recovery_only", False)) is not bool
            ):
                raise _Refused(-32602, "the request archive, account or chain does not match")
            if params.get("recovery_only") and request_id not in self.sent:
                raise _Refused(
                    -32002,
                    "the original request is absent from this archive",
                    {"retryable": False, "reason": "request_not_archived"},
                )
            if params.get("recovery_only") and self.recovery_unknowns:
                self.recovery_unknowns -= 1
                raise _Refused(
                    -32002,
                    "the archived transaction could not reach its node",
                    {"retryable": False, **({"reason": self.recovery_reason} if self.recovery_reason else {})},
                )
        if request_id in self.sent:
            return {**self.sent[request_id], "duplicate": True}
        error, pays_anyway = self._failures.pop(0) if self._failures else (None, True)
        if error is not None and not pays_anyway:
            raise _Refused(**error)
        receipt = self._pay_out(params)
        if error is not None:
            raise _Refused(**error)
        return receipt

    def _pay_out(self, params: dict[str, Any]) -> dict[str, Any]:
        transaction = f"{next(self._ids):096x}"
        amount = to_atoms(params["amount"]) - (to_atoms(FEE) if params.get("subtract_fee") else 0)
        receipt = {
            "kind": "payment",
            "to": params["to"],
            "amount": from_atoms(amount),
            "fee": FEE,
            "transaction": transaction,
            "request_id": params.get("request_id"),
            "submitted": not params.get("dry_run", False),
        }
        if receipt["submitted"]:
            self.payments += 1
            if params.get("request_id"):
                self.sent[params["request_id"]] = receipt
            self.transactions[transaction] = {
                "transaction": transaction,
                "lifecycle": "processing",
                "finalized_cut": None,
                "failure": None,
                "confirmations": 0,
                "pending_send": True,
            }
        return receipt


class _Refused(Exception):
    def __init__(self, code: int, message: str, data: dict[str, Any] | None = None) -> None:
        self.error = {"code": code, "message": message, "data": data or {"retryable": False}}


def _error(code: int, message: str) -> dict[str, Any]:
    return {"code": code, "message": message, "data": {"retryable": False}}
