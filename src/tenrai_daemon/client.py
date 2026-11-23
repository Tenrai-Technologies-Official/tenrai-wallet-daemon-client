"""JSON-RPC calls to the wallet daemon over HTTP."""

import ipaddress
import itertools
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import aiohttp

from .amounts import amount_text
from .budget import fee_cap_text
from .errors import DaemonError, ErrorCode
from .refund_watch import (
    RefundWatchForgotten,
    RefundWatchJob,
    RefundWatchList,
    RefundWatchOutpoint,
    WalletLockStatus,
    WalletState,
    positive_atoms,
)
from .rpc_reply import rpc_result

DEFAULT_URL = "http://127.0.0.1:28703"
MAX_REPLY_BYTES = 64 << 20
SPENDING_METHODS = frozenset(
    {
        "send",
        "send_many",
        "sweep",
        "burn",
        "compound",
        "stake",
        "unstake",
        "withdraw",
        "native_sweep",
        "private_register",
        "private_enter",
        "private_transfer",
        "private_exit",
        "private_retry",
    }
)

Json = dict[str, Any]
Amount = str | Decimal


@dataclass(frozen=True, slots=True)
class Recipient:
    """A batch recipient with an optional output-specific payload."""

    address: str
    amount: Amount
    payload_text: str | None = None


@dataclass(frozen=True, slots=True)
class RequestScope:
    """Daemon archive, account and chain bound to a payment attempt."""

    request_archive_id: str
    account: int
    chain_id: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_archive_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.request_archive_id) is None
        ):
            raise ValueError("request_archive_id must be 64 lowercase hex characters")
        if type(self.account) is not int or not 0 <= self.account <= 0xFFFFFFFF:
            raise ValueError("request account must be an unsigned 32-bit integer")
        if not isinstance(self.chain_id, str) or re.fullmatch(r"[0-9a-f]{96}", self.chain_id) is None:
            raise ValueError("request chain_id must be 96 lowercase hex characters")

    @classmethod
    def from_status(cls, status: Mapping[str, Any]) -> "RequestScope":
        if not isinstance(status, Mapping) or status.get("durable_request_ids") is not True:
            raise ValueError("the daemon does not advertise durable_request_ids")
        return cls(status.get("request_archive_id"), status.get("account"), status.get("chain_id"))


def _batch_recipients(recipients: Mapping[str, Amount] | Iterable[tuple[str, Amount] | Recipient]) -> list[Json]:
    pairs = recipients.items() if isinstance(recipients, Mapping) else recipients
    encoded = []
    for recipient in pairs:
        if isinstance(recipient, Recipient):
            encoded.append(
                _given(
                    address=recipient.address, amount=amount_text(recipient.amount), payload_text=recipient.payload_text
                )
            )
        else:
            address, amount = recipient
            encoded.append({"address": address, "amount": amount_text(amount)})
    return encoded


def _private_guards(request_id: str | None, scope: RequestScope | None, recovery_only: bool | None) -> Json:
    if request_id is not None and (
        not isinstance(request_id, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", request_id) is None
    ):
        raise ValueError("request_id must be 1 to 64 canonical request characters")
    if scope is None and recovery_only is None:
        return {}
    if not isinstance(scope, RequestScope) or request_id is None:
        raise ValueError("scoped private requests require a request_id and complete RequestScope")
    if recovery_only is not None and type(recovery_only) is not bool:
        raise ValueError("recovery_only must be a boolean")
    return {
        "expected_request_archive_id": scope.request_archive_id,
        "expected_account": scope.account,
        "expected_chain_id": scope.chain_id,
        "recovery_only": False if recovery_only is None else recovery_only,
    }


def _given(**params: Any) -> Json:
    return {name: value for name, value in params.items() if value is not None}


def checked_url(url: str, allow_insecure: bool, schemes: tuple[str, ...] = ("http", "https")) -> str:
    """Normalize a daemon URL and require TLS off loopback by default."""
    parts = urlsplit(url)
    if parts.scheme not in schemes or not parts.hostname:
        raise ValueError(f"not a daemon URL: {url!r}")
    if parts.scheme in ("http", "ws") and not allow_insecure and not _loopback(parts.hostname):
        raise ValueError(
            f"{url} would send the token unencrypted to another machine; "
            "put TLS in front of the daemon or pass allow_insecure=True"
        )
    return url.rstrip("/")


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


async def _json(response: aiohttp.ClientResponse) -> Any:
    if (response.content_length or 0) > MAX_REPLY_BYTES:
        raise DaemonError("the daemon's answer is larger than 64 MiB", retryable=True)
    body = bytearray()
    async for chunk in response.content.iter_chunked(1 << 16):
        body += chunk
        if len(body) > MAX_REPLY_BYTES:
            raise DaemonError("the daemon's answer is larger than 64 MiB", retryable=True)
    return json.loads(body)


class DaemonClient:
    """Share one HTTP client across tasks and close it at shutdown."""

    def __init__(
        self,
        url: str = DEFAULT_URL,
        token: str = "",
        *,
        timeout: float = 60.0,
        session: aiohttp.ClientSession | None = None,
        allow_insecure: bool = False,
    ) -> None:
        self.url = checked_url(url, allow_insecure)
        self._token = token.strip()
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._owns_session = session is None
        self._ids = itertools.count(1)

    @classmethod
    def from_token_file(cls, url: str, token_file: str | Path, **options: Any) -> "DaemonClient":
        """Read the daemon's bearer token file."""
        return cls(url, Path(token_file).read_text(encoding="utf-8"), **options)

    async def __aenter__(self) -> "DaemonClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
        self._session = None

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    async def health(self) -> Json:
        """Report daemon health and whether the node socket is connected."""
        try:
            async with self._http().get(f"{self.url}/health", timeout=self._timeout, allow_redirects=False) as response:
                answer = await _json(response)
        except (aiohttp.ClientError, TimeoutError, ValueError) as error:
            raise DaemonError(f"cannot reach the daemon: {error}", retryable=True) from error
        if not isinstance(answer, dict):
            raise DaemonError("the daemon's health answer is not a JSON object", retryable=True)
        return answer

    async def call(self, method: str, params: Json | None = None) -> Any:
        """Call a daemon RPC by name."""
        try:
            return await self._call(method, params)
        except DaemonError as error:
            if method in SPENDING_METHODS and error.code is None and error.retryable:
                raise DaemonError(
                    "the payment outcome is unknown; recover the original request "
                    "or inspect the wallet before sending again",
                    code=ErrorCode.OUTCOME_UNKNOWN,
                ) from error
            raise

    async def _call(self, method: str, params: Json | None) -> Any:
        request = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": method,
            "params": params or {},
        }
        headers = {"Authorization": f"Bearer {self._token}"}
        try:
            async with self._http().post(
                f"{self.url}/v1/rpc",
                json=request,
                headers=headers,
                timeout=self._timeout,
                allow_redirects=False,
            ) as response:
                if response.status == 401:
                    raise DaemonError("the daemon refused the token")
                if response.status != 200:
                    raise DaemonError(
                        f"the daemon answered HTTP {response.status}",
                        retryable=response.status >= 500,
                    )
                reply = await _json(response)
        except (aiohttp.ClientError, TimeoutError) as error:
            raise DaemonError(f"cannot reach the daemon: {error}", retryable=True) from error
        except ValueError as error:
            raise DaemonError(f"the daemon did not answer with JSON: {error}", retryable=True) from error
        return rpc_result(reply, request["id"])

    async def native_lock(self, descriptor_hex: str) -> Json:
        """Validate a native lock descriptor and return its address and conditions."""
        return await self.call("native_lock", {"descriptor_hex": descriptor_hex})

    async def native_sweep(
        self,
        descriptor_hex: str,
        to: str,
        *,
        spend_branch: str = "claim",
        key_branch: str = "receive",
        index: int = 0,
        preimage_hex: str = "",
        valid_from: int | None = None,
        valid_until: int | None = None,
        max_inputs: int = 8,
        fee: str = "normal",
        dry_run: bool = True,
        outpoint: Json | None = None,
    ) -> Json:
        """Review native outputs or submit a redemption with `dry_run=False`."""
        return await self.call(
            "native_sweep",
            _given(
                descriptor_hex=descriptor_hex,
                to=to,
                spend_branch=spend_branch,
                key_branch=key_branch,
                index=index,
                preimage_hex=preimage_hex,
                valid_from=valid_from,
                valid_until=valid_until,
                max_inputs=max_inputs,
                fee=fee,
                dry_run=dry_run,
                outpoint=outpoint,
            ),
        )

    async def refund_watch_register(
        self,
        descriptor_hex: str,
        outpoint: RefundWatchOutpoint,
        *,
        max_fee_atoms: str,
        key_branch: Literal["receive", "change"] = "receive",
        index: int = 0,
    ) -> RefundWatchJob:
        """Register one funded HTLC outpoint while the daemon is unlocked.

        Automatic refunds require `--auto-refund`. The fee cap is a positive atom string.
        """
        return await self.call(
            "refund_watch_register",
            {
                "descriptor_hex": descriptor_hex,
                "outpoint": outpoint,
                "key_branch": key_branch,
                "index": index,
                "max_fee_atoms": positive_atoms(max_fee_atoms),
            },
        )

    async def refund_watch_list(self) -> RefundWatchList:
        """Read refund jobs and the automatic refund setting."""
        return await self.call("refund_watch_list")

    async def refund_watch_status(self, job_id: str) -> RefundWatchJob:
        return await self.call("refund_watch_status", {"job_id": job_id})

    async def refund_watch_cancel(self, job_id: str) -> RefundWatchJob:
        """Cancel further refund attempts while the wallet is unlocked."""
        return await self.call("refund_watch_cancel", {"job_id": job_id})

    async def refund_watch_forget(self, job_id: str) -> RefundWatchForgotten:
        """Forget a settled job after all pending attempts are resolved."""
        return await self.call("refund_watch_forget", {"job_id": job_id})

    async def wallet_lock(self) -> WalletLockStatus:
        """Stop new signing while pending transactions settle."""
        return await self.call("wallet_lock")

    async def wallet_state(self) -> WalletState:
        """Read the signing state without renewing the lease."""
        return await self.call("wallet_state")

    async def wallet_unlock(
        self,
        password: str,
        *,
        passphrase: str = "",
        unlock_for_seconds: int | None = None,
    ) -> WalletLockStatus:
        """Unlock the wallet for an optional lease of 1 to 86,400 seconds."""
        return await self.call(
            "wallet_unlock",
            _given(
                password=password,
                passphrase=passphrase,
                unlock_for_seconds=unlock_for_seconds,
            ),
        )

    # Wallet and addresses

    async def status(self) -> Json:
        return await self.call("status")

    async def balance(self, *, funded: bool = False, addresses: Iterable[str] = ()) -> Json:
        """`funded` lists the funded addresses; `addresses` reads any addresses into `queried`."""
        return await self.call("balance", {"funded": funded, "addresses": list(addresses)})

    async def addresses(self, *, branch: str | None = None, funded: bool = False) -> Json:
        return await self.call("addresses", _given(branch=branch, funded=funded))

    async def new_address(self, label: str | None = None) -> Json:
        """Issue a fresh address with an optional customer or order label."""
        return await self.call("new_address", _given(label=label))

    async def receive(self) -> Json:
        return await self.call("receive")

    async def set_label(self, address: str, label: str | None) -> Json:
        """Pass `None` to remove the label."""
        return await self.call("set_label", {"address": address, "label": label})

    async def validate_address(self, address: str) -> Json:
        """Check address validity, canonical form, verification words and ownership."""
        return await self.call("validate_address", {"address": address})

    async def outputs(self, address: str | None = None, *, largest: int | None = None) -> Json:
        """Use `largest` to limit the result to the largest unreserved outputs."""
        return await self.call("outputs", _given(address=address, largest=largest))

    # Money coming in

    async def received(self, since_height: int = 0, *, change: bool = False) -> Json:
        """Read outputs after `since_height`. Resume at `indexed_height`."""
        return await self.call("received", {"since_height": since_height, "change": change})

    async def incoming(self) -> Json:
        """Read pending incoming payments separately from the balance."""
        return await self.call("incoming")

    async def history(self, limit: int = 50) -> Json:
        return await self.call("history", {"limit": limit})

    async def transaction(self, transaction: str) -> Json:
        """Read transaction status, confirmations and payment details."""
        return await self.call("transaction", {"transaction": transaction})

    # Money going out

    async def send(
        self,
        to: str,
        amount: Amount,
        *,
        fee: str = "normal",
        payload_text: str | None = None,
        payload_hex: str | None = None,
        request_id: str | None = None,
        subtract_fee: bool = False,
        dry_run: bool = False,
    ) -> Json:
        """Send to one address. Reuse `request_id` to recover the original result."""
        return await self.call(
            "send",
            _given(
                to=to,
                amount=amount_text(amount),
                fee=fee,
                payload_text=payload_text,
                payload_hex=payload_hex,
                request_id=request_id,
                subtract_fee=subtract_fee,
                dry_run=dry_run,
            ),
        )

    async def quote_send_many(
        self,
        recipients: Mapping[str, Amount] | Iterable[tuple[str, Amount] | Recipient],
        *,
        fee: str = "normal",
        subtract_fee: bool = False,
        payload_text: str | None = None,
        payload_hex: str | None = None,
    ) -> Json:
        """Quote a batch fee without signing or reserving inputs."""
        return await self.call(
            "quote_send_many",
            _given(
                recipients=_batch_recipients(recipients),
                fee=fee,
                subtract_fee=subtract_fee,
                payload_text=payload_text,
                payload_hex=payload_hex,
            ),
        )

    async def send_many(
        self,
        recipients: Mapping[str, Amount] | Iterable[tuple[str, Amount] | Recipient],
        *,
        fee: str = "normal",
        max_fee: Amount | None = None,
        payload_text: str | None = None,
        payload_hex: str | None = None,
        request_id: str | None = None,
        subtract_fee: bool = False,
        compound_first: bool | None = None,
        dry_run: bool = False,
        scope: RequestScope | None = None,
        recovery_only: bool | None = None,
    ) -> Json:
        """Pay up to seven addresses with an optional fee cap.

        Persist the scope and attempt before sending. Use `recovery_only=True` for scoped retries.
        A `never_signed` result closes the request. A signed `dry_run` export holds its inputs.
        """
        guards: Json = {}
        if scope is not None or recovery_only is not None:
            if not isinstance(scope, RequestScope):
                raise ValueError("scoped sends require the complete RequestScope")
            if recovery_only is not None and type(recovery_only) is not bool:
                raise ValueError("recovery_only must be a boolean")
            if not isinstance(request_id, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", request_id) is None:
                raise ValueError("scoped sends require a canonical request_id")
            guards = {
                "expected_request_archive_id": scope.request_archive_id,
                "expected_account": scope.account,
                "expected_chain_id": scope.chain_id,
                "recovery_only": False if recovery_only is None else recovery_only,
            }
        encoded = _batch_recipients(recipients)
        return await self.call(
            "send_many",
            _given(
                recipients=encoded,
                max_fee=None if max_fee is None else fee_cap_text(max_fee),
                fee=fee,
                payload_text=payload_text,
                payload_hex=payload_hex,
                request_id=request_id,
                subtract_fee=subtract_fee,
                compound_first=compound_first,
                dry_run=dry_run,
                **guards,
            ),
        )

    async def sweep(
        self,
        to: str,
        *,
        fee: str = "normal",
        request_id: str | None = None,
        dry_run: bool = False,
    ) -> Json:
        """Send all spendable funds to one address."""
        return await self.call("sweep", _given(to=to, fee=fee, request_id=request_id, dry_run=dry_run))

    async def compound(self, *, fee: str = "normal", dry_run: bool = False) -> Json:
        return await self.call("compound", {"fee": fee, "dry_run": dry_run})

    async def pending(self) -> Json:
        """Read pending sends and their reserved inputs."""
        return await self.call("pending")

    async def private_balance(
        self, rescan: bool = False, *, after_note: int | None = None, limit: int | None = None
    ) -> Json:
        """Read the certified balance and page through local notes with `next_note`."""
        if after_note is not None and (type(after_note) is not int or not 0 <= after_note <= 0xFFFFFFFFFFFFFFFF):
            raise ValueError("after_note must be an unsigned 64-bit integer")
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 128):
            raise ValueError("private note page limit must be from 1 to 128")
        return await self.call(
            "private_balance", _given(rescan=True if rescan else None, after_note=after_note, limit=limit)
        )

    async def private_status(self) -> Json:
        """Read private-transfer activation, fee rules and limits."""
        return await self.call("private_status")

    async def private_authorize(self) -> Json:
        """Export owner authorization for a registration sponsor."""
        return await self.call("private_authorize")

    async def private_register(
        self,
        max_fee: Amount,
        *,
        owner_authorization: str | None = None,
        dry_run: bool = False,
        wait: bool = False,
    ) -> Json:
        """Register under a fee cap. `dry_run` exports the signed frame and reserves inputs."""
        if owner_authorization is not None and not isinstance(owner_authorization, str):
            raise TypeError("owner_authorization must be the exported authorization string")
        return await self.call(
            "private_register",
            _given(
                max_fee=amount_text(max_fee),
                owner_authorization=owner_authorization,
                dry_run=True if dry_run else None,
                wait=True if wait else None,
            ),
        )

    async def private_enter(
        self,
        amount: Amount,
        *,
        to: str | None = None,
        wait: bool = False,
        request_id: str | None = None,
        scope: RequestScope | None = None,
        recovery_only: bool | None = None,
    ) -> Json:
        """Move public funds into notes. Save the scope and request ID before submitting."""
        guards = _private_guards(request_id, scope, recovery_only)
        return await self.call(
            "private_enter",
            _given(
                amount=amount_text(amount),
                to=to,
                wait=wait or None,
                request_id=request_id,
                **guards,
            ),
        )

    async def private_transfer(
        self,
        to: str,
        amount: Amount,
        *,
        wait: bool = False,
        request_id: str | None = None,
        scope: RequestScope | None = None,
        recovery_only: bool | None = None,
    ) -> Json:
        """Send from two private notes. Recovery uses the original request only."""
        guards = _private_guards(request_id, scope, recovery_only)
        return await self.call(
            "private_transfer",
            _given(
                to=to,
                amount=amount_text(amount),
                wait=wait or None,
                request_id=request_id,
                **guards,
            ),
        )

    async def private_exit(
        self,
        to: str,
        notes: Iterable[int],
        *,
        wait: bool = False,
        request_id: str | None = None,
        scope: RequestScope | None = None,
        recovery_only: bool | None = None,
    ) -> Json:
        """Withdraw one or two whole notes after fees. Retain the same notes for retries."""
        selected = list(itertools.islice(notes, 3))
        if (
            not 1 <= len(selected) <= 2
            or any(type(n) is not int or not 0 <= n <= 0xFFFFFFFFFFFFFFFF for n in selected)
            or len(set(selected)) != len(selected)
        ):
            raise ValueError("private exit requires one or two distinct unsigned 64-bit note indices")
        guards = _private_guards(request_id, scope, recovery_only)
        return await self.call(
            "private_exit",
            _given(
                to=to,
                notes=selected,
                wait=wait or None,
                request_id=request_id,
                **guards,
            ),
        )

    async def private_pending(self) -> Json:
        """Read active private intents and interrupted pre-proof reservations."""
        return await self.call("private_pending")

    async def private_retry(self, request_id: str, *, wait: bool = False, scope: RequestScope | None = None) -> Json:
        """Retry the complete original frame."""
        guards = _private_guards(request_id, scope, True if scope is not None else None)
        return await self.call("private_retry", _given(request_id=request_id, wait=wait or None, **guards))

    async def private_abort(self, request_id: str, *, scope: RequestScope | None = None) -> Json:
        """Release an interrupted intent before it produced a complete frame."""
        guards = _private_guards(request_id, scope, True if scope is not None else None)
        return await self.call("private_abort", {"request_id": request_id, **guards})

    async def private_receipts(self, *, after: str | None = None, limit: int = 32) -> Json:
        """Page retained receipts with `next_after`, including after empty filtered pages."""
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("private receipt page limit must be from 1 to 64")
        if after is not None and (not isinstance(after, str) or re.fullmatch(r"[0-9a-f]{96}", after) is None):
            raise ValueError("private receipt cursor must be 96 lowercase hex characters")
        return await self.call("private_receipts", _given(after=after, limit=limit))

    async def rescan(self, depth: int | None = None) -> Json:
        """Scan past the last used address. The default depth is 1,000."""
        return await self.call("rescan", _given(depth=depth))

    # Address ownership

    async def proof_request(self, address: str, valid_for: int | None = None) -> Json:
        """Request an ownership proof. Defaults to a 600-second validity window."""
        return await self.call("proof_request", _given(address=address, valid_for=valid_for))

    async def prove_address(self, request: Json) -> Json:
        """Sign an ownership proof for a tracked wallet address."""
        return await self.call("prove_address", {"request": request})

    async def verify_address_proof(self, proof: Json, request: Json | None = None) -> Json:
        """Verify an ownership proof against the optional original request."""
        return await self.call("verify_address_proof", _given(proof=proof, request=request))
