"""Reconnecting WebSocket event stream."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

from .client import DEFAULT_URL, checked_url
from .errors import DaemonError
from .rpc_reply import rpc_result

log = logging.getLogger(__name__)

CONNECTED = "connected"
BACKOFF = (1.0, 2.0, 5.0, 10.0, 30.0)


@dataclass(frozen=True)
class Event:
    """Daemon event or local `connected` notification."""

    type: str
    data: dict[str, Any] = field(default_factory=dict)


class EventStream:
    """Stream daemon events with automatic reconnect.

    Catch up with `received` after `connected`, `resync` and `resync_required`.
    """

    def __init__(
        self,
        url: str = DEFAULT_URL,
        token: str = "",
        events: Iterable[str] | None = None,
        *,
        session: aiohttp.ClientSession | None = None,
        backoff: Iterable[float] = BACKOFF,
        allow_insecure: bool = False,
    ) -> None:
        self.url = _socket_url(checked_url(url, allow_insecure, ("http", "https", "ws", "wss")))
        self._token = token.strip()
        self._events = None if events is None else list(events)
        self._session = session
        self._owns_session = session is None
        self._backoff = tuple(backoff)

    @classmethod
    def from_token_file(cls, url: str, token_file: str | Path, **options: Any) -> "EventStream":
        return cls(url, Path(token_file).read_text(encoding="utf-8"), **options)

    async def __aenter__(self) -> "EventStream":
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

    async def __aiter__(self) -> AsyncIterator[Event]:
        failures = 0
        while True:
            try:
                async with self._http().ws_connect(
                    self.url, headers={"Authorization": f"Bearer {self._token}"}, heartbeat=30.0
                ) as socket:
                    await self._subscribe(socket)
                    failures = 0
                    yield Event(CONNECTED)
                    async for message in socket:
                        if message.type is aiohttp.WSMsgType.TEXT and (event := _event(message.data)):
                            yield event
                        elif message.type is aiohttp.WSMsgType.ERROR:
                            break
                log.warning("the daemon socket closed, reconnecting")
            except aiohttp.WSServerHandshakeError as error:
                if error.status == 401:
                    raise DaemonError("the daemon refused the token") from error
                log.warning("the daemon refused the socket with HTTP %s", error.status)
            except (aiohttp.ClientError, TimeoutError, OSError) as error:
                log.warning("cannot reach the daemon socket: %s", error)
            await asyncio.sleep(self._backoff[min(failures, len(self._backoff) - 1)])
            failures += 1

    async def _subscribe(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        params = {} if self._events is None else {"events": self._events}
        await socket.send_json({"jsonrpc": "2.0", "id": "subscribe", "method": "subscribe", "params": params})
        try:
            reply = await socket.receive_json(timeout=30)
        except (TypeError, ValueError) as error:
            raise ConnectionError(f"the daemon did not confirm the subscription: {error}") from error
        try:
            result = rpc_result(reply, "subscribe")
        except DaemonError as error:
            if error.code is not None:
                raise
            raise ConnectionError("the daemon did not confirm the subscription") from error
        if not isinstance(result, dict):
            raise ConnectionError("the daemon did not confirm the subscription")


def _socket_url(url: str) -> str:
    base = url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base.removeprefix("https://")
    elif base.startswith("http://"):
        base = "ws://" + base.removeprefix("http://")
    return f"{base}/v1/ws"


def _event(text: str) -> Event | None:
    try:
        frame = json.loads(text)
    except ValueError:
        log.warning("skipped a frame that is not JSON")
        return None
    if not isinstance(frame, dict) or frame.get("method") != "event":
        return None
    params = frame.get("params")
    if not isinstance(params, dict):
        return None
    data = params.get("data")
    return Event(str(params.get("type", "")), data if isinstance(data, dict) else {})
