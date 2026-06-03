"""Thread-backed wrapper around ib_async.

ib_async is built on asyncio, while Tkinter runs its own blocking event loop.
To keep them from fighting, the IB connection and all its asyncio work live on
a dedicated thread with its own event loop. The UI thread submits coroutines via
`run_coroutine_threadsafe` and receives results through concurrent.futures.Future
objects, so the Tk loop never blocks.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Callable

from ib_async import IB, AccountValue


class IBClient:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ib = IB()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _submit(self, coro) -> Future:
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    @property
    def connected(self) -> bool:
        return self._ib.isConnected()

    def connect(self, host: str, port: int, client_id: int) -> Future:
        return self._submit(
            self._ib.connectAsync(host, port, clientId=client_id, timeout=10)
        )

    def disconnect(self) -> Future:
        async def _disc() -> None:
            self._ib.disconnect()

        return self._submit(_disc())

    def account_summary(self) -> Future:
        """Return a Future resolving to the list of AccountValue rows."""
        return self._submit(self._ib.accountSummaryAsync())

    def on_disconnect(self, callback: Callable[[], None]) -> None:
        self._ib.disconnectedEvent += lambda *_: callback()
