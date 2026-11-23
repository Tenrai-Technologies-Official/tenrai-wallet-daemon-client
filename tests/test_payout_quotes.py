from decimal import Decimal, localcontext

import pytest
from fake_daemon import TOKEN, asynchronous

from tenrai_daemon import DaemonClient
from tenrai_daemon.budget import fee_cap_text


class Calls(DaemonClient):
    def __init__(self):
        super().__init__("http://127.0.0.1:1", TOKEN)
        self.calls = []

    async def call(self, method, params=None):
        self.calls.append((method, params))
        return {"unsigned": True, "submitted": False}


@asynchronous
async def test_quote_is_unsigned_without_export_or_request_and_send_keeps_its_numeric_cap():
    client = Calls()
    await client.quote_send_many({"test-only-address": "1"}, fee="normal")
    await client.send_many({"test-only-address": "1"}, max_fee=Decimal("0.00000020"), request_id="budget-1")
    assert client.calls[0] == (
        "quote_send_many",
        {
            "recipients": [{"address": "test-only-address", "amount": "1.00000000"}],
            "fee": "normal",
            "subtract_fee": False,
        },
    )
    assert client.calls[1][0] == "send_many"
    assert client.calls[1][1]["max_fee"] == "0.00000020"
    assert client.calls[1][1]["request_id"] == "budget-1"
    assert "dry_run" not in client.calls[0][1] and "request_id" not in client.calls[0][1]


def test_max_fee_never_rounds_under_a_low_decimal_context():
    with localcontext() as context:
        context.prec = 3
        assert fee_cap_text("3402823669209384634633746074317.68211455") == "3402823669209384634633746074317.68211455"
        assert fee_cap_text("0") == "0.00000000"
        assert fee_cap_text("2.0000000000") == "2.00000000"
    for amount in ["-1", "nan", "1.000000001", "3402823669209384634633746074317.68211456", "1e1000"]:
        with pytest.raises(ValueError):
            fee_cap_text(amount)
    with pytest.raises(TypeError):
        fee_cap_text(0.1)
