"""Real HTTP wrapper/attempt tests with an explicit effects stand-in, not a chain/prover test."""

from decimal import Decimal

import pytest
from aiohttp import web
from fake_daemon import TOKEN, FakeDaemon, _Refused, asynchronous

from tenrai_daemon import DaemonClient, DaemonError, ErrorCode, RequestScope


class PrivateDaemon(FakeDaemon):
    def __init__(self):
        super().__init__()
        self.private = {}
        self.private_payments = 0
        self.lose_private_reply = False

    async def _rpc(self, request):
        result = await super()._rpc(request)
        method = (await request.json())["method"]
        if self.lose_private_reply and method in ("private_enter", "private_transfer", "private_exit"):
            self.lose_private_reply = False
            return web.Response(text="{", content_type="application/json")
        return result

    def _answer(self, method, params):
        if method == "private_authorize":
            return {"authorization": "ab" * 50}
        if method == "private_register":
            if "max_fee" not in params:
                raise _Refused(-32602, "explicit maximum fee required")
            return {"registered": not params.get("dry_run", False), "maximum_fee": params["max_fee"]}
        if method == "private_pending":
            return {"requests": list(self.private.values())}
        if method == "private_receipts":
            return {"receipts": list(self.private.values()), "next_after": None}
        if method not in ("private_enter", "private_transfer", "private_exit", "private_retry", "private_abort"):
            return super()._answer(method, params)
        rid = params.get("request_id")
        guarded = "expected_request_archive_id" in params
        if guarded:
            if (
                params["expected_request_archive_id"] != self.request_archive_id
                or params["expected_account"] != self.account
                or params["expected_chain_id"] != self.chain_id
            ):
                raise _Refused(-32602, "another authenticated archive/account/chain")
            if params.get("recovery_only") and rid not in self.private:
                raise _Refused(-32002, "missing authenticated original", {"reason": "request_not_archived"})
        original = self.private.get(rid)
        if method in ("private_retry", "private_abort"):
            if original is None:
                raise _Refused(-32002, "no original", {"reason": "request_not_archived"})
            if method == "private_abort":
                raise _Refused(-32000, "a complete frame cannot be aborted")
        else:
            semantic = {k: params[k] for k in ("to", "amount", "notes") if k in params}
            if original is not None and (original["method"] != method or original["semantic"] != semantic):
                raise _Refused(-32602, "request ID already names another payment")
            if original is None:
                self.private_payments += 1
                original = {
                    "method": method,
                    "semantic": semantic,
                    "request_id": rid,
                    "statement": f"{self.private_payments:096x}",
                    "state": "prepared",
                }
                self.private[rid] = original
        if original["state"] == "unknown":
            raise _Refused(-32002, "unknown original outcome", {"reason": "outcome_unknown"})
        if original["state"] == "expired_unused":
            raise _Refused(-32004, "expired unused", {"reason": "expired_unused"})
        return original


@asynchronous
async def test_paid_self_and_sponsored_registration_require_reviewable_cap_and_authorization():
    async with PrivateDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        authorization = (await client.private_authorize())["authorization"]
        await client.private_register(Decimal("1"), owner_authorization=authorization, dry_run=True)
        await client.private_register("1", owner_authorization=authorization, wait=True)
        assert daemon.calls == [
            ("private_authorize", {}),
            ("private_register", {"max_fee": "1.00000000", "owner_authorization": authorization, "dry_run": True}),
            ("private_register", {"max_fee": "1.00000000", "owner_authorization": authorization, "wait": True}),
        ]
        before = len(daemon.calls)
        for value in ("0", "-1", 1.25):
            with pytest.raises((ValueError, TypeError)):
                await client.private_register(value)
        assert len(daemon.calls) == before


@asynchronous
async def test_lost_response_retries_same_original_and_changed_arguments_never_replan():
    async with PrivateDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        daemon.lose_private_reply = True
        with pytest.raises(DaemonError) as failed:
            await client.private_enter("100", request_id="enter:1", scope=scope)
        assert failed.value.outcome_unknown
        assert daemon.private_payments == 1
        retried = await client.private_retry("enter:1", scope=scope, wait=True)
        assert retried["statement"] == daemon.private["enter:1"]["statement"]
        assert daemon.private_payments == 1
        with pytest.raises(DaemonError):
            await client.private_enter("101", request_id="enter:1", scope=scope, recovery_only=True)
        assert daemon.private_payments == 1
        call = daemon.calls[-2][1]
        assert call["recovery_only"] is True
        assert (call["expected_request_archive_id"], call["expected_account"], call["expected_chain_id"]) == (
            scope.request_archive_id,
            scope.account,
            scope.chain_id,
        )


@asynchronous
@pytest.mark.parametrize("changed", ["request_archive_id", "account", "chain_id"])
async def test_restore_or_scope_change_cannot_be_treated_as_first_payment(changed):
    async with PrivateDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        setattr(daemon, changed, {"request_archive_id": "bb" * 32, "account": 1, "chain_id": "cc" * 48}[changed])
        with pytest.raises(DaemonError):
            await client.private_transfer("dtrai1owner", "1", request_id="restore:1", scope=scope)
        assert daemon.private_payments == 0


@asynchronous
async def test_missing_original_and_thin_closed_outcomes_are_never_success_receipts():
    async with PrivateDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        scope = RequestScope.from_status(await client.status())
        with pytest.raises(DaemonError) as absent:
            await client.private_exit("trai1owner", [9], request_id="missing:1", scope=scope, recovery_only=True)
        assert absent.value.outcome_unknown and absent.value.reason == "request_not_archived"
        assert daemon.private_payments == 0
        await client.private_exit("trai1owner", [9], request_id="exit:1", scope=scope)
        for state, code in (("unknown", ErrorCode.OUTCOME_UNKNOWN), ("expired_unused", ErrorCode.REJECTED)):
            daemon.private["exit:1"]["state"] = state
            with pytest.raises(DaemonError) as failed:
                await client.private_retry("exit:1", scope=scope)
            assert failed.value.code == code
        assert daemon.private_payments == 1
        assert (await client.private_pending())["requests"]
        assert (await client.private_receipts(limit=1))["next_after"] is None


@asynchronous
async def test_private_pagination_and_invalid_local_arguments_are_bounded_before_post():
    async with PrivateDaemon() as daemon, DaemonClient(daemon.url, TOKEN) as client:
        await client.private_balance(after_note=0, limit=128)
        assert daemon.calls[-1] == ("private_balance", {"after_note": 0, "limit": 128})
        before = len(daemon.calls)
        scope = RequestScope.from_status(await client.status())
        before += 1
        for kwargs in (
            {"request_id": "bad space"},
            {"request_id": "x", "recovery_only": True},
            {"request_id": "x", "scope": scope, "recovery_only": 1},
            {"scope": scope},
        ):
            with pytest.raises(ValueError):
                await client.private_enter("1", **kwargs)
        for notes in ([], [1, 1], [1, 2, 3], [-1], [True], [1 << 64]):
            with pytest.raises(ValueError):
                await client.private_exit("trai1owner", notes)
        for kwargs in ({"limit": 0}, {"limit": 129}, {"after_note": -1}, {"after_note": True}):
            with pytest.raises(ValueError):
                await client.private_balance(**kwargs)
        with pytest.raises(ValueError):
            await client.private_receipts(limit=65)
        assert len(daemon.calls) == before
