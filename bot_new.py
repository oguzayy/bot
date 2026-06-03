"""IB Trading Dashboard — single-file Tkinter UI that connects to IB
TWS/Gateway and shows the account balance.

Run with the project venv:
    ~/ib-trading/.venv/bin/python ~/ib-trading/app.py

Prerequisite: in TWS go to File > Global Configuration > API > Settings,
enable "Enable ActiveX and Socket Clients", and confirm the socket port
(7497 for TWS paper trading by default).

ib_async is built on asyncio, while Tkinter runs its own blocking event loop.
To keep them from fighting, the IB connection and all its asyncio work live on
a dedicated thread with its own event loop. The UI thread submits coroutines via
`run_coroutine_threadsafe` and receives results through Future objects, so the
Tk loop never blocks.
"""

from __future__ import annotations

import asyncio
import queue
import threading
import tkinter as tk
from concurrent.futures import Future
from tkinter import ttk
from typing import Callable

from ib_async import IB

# Selectable connection targets. Host and client id are handled internally;
# the user only picks which TWS/Gateway endpoint to talk to.
HOST = "127.0.0.1"
CLIENT_ID = 1
TARGETS: dict[str, int] = {
    "TWS Paper": 7497,
    "TWS Live": 7496,
    "IB Gateway Paper": 4002,
    "IB Gateway Live": 4001,
}

# Auto-refresh choices. None means manual-only (no timer).
REFRESH_OPTIONS: dict[str, int | None] = {
    "Manual": None,
    "5 sec": 5,
    "10 sec": 10,
    "15 sec": 15,
    "30 sec": 30,
    "60 sec": 60,
}

# Tags from IB's account summary we care about for a balance view.
BALANCE_TAGS = [
    "NetLiquidation",
    "TotalCashValue",
    "AvailableFunds",
    "BuyingPower",
    "GrossPositionValue",
    "UnrealizedPnL",
    "RealizedPnL",
]


class IBClient:
    """Thread-backed wrapper around ib_async."""

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


class TradingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("IB Trading Dashboard")
        self.geometry("560x460")

        self.client = IBClient()
        self.client.on_disconnect(lambda: self._events.put(("disconnected", None)))

        # Thread-safe channel for messages from the IB worker thread to the UI.
        self._events: queue.Queue = queue.Queue()
        self._refresh_after_id: str | None = None

        self._build_widgets()
        self.after(100, self._poll_events)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_widgets(self) -> None:
        conn = ttk.LabelFrame(self, text="Connection")
        conn.pack(fill="x", padx=10, pady=10)

        ttk.Label(conn, text="Target").grid(row=0, column=0, padx=4, pady=8, sticky="w")
        self.target_var = tk.StringVar(value=next(iter(TARGETS)))
        self.target_box = ttk.Combobox(
            conn,
            textvariable=self.target_var,
            values=list(TARGETS),
            state="readonly",
            width=20,
        )
        self.target_box.grid(row=0, column=1, padx=4)

        self.connect_btn = ttk.Button(conn, text="Connect", command=self._toggle_connect)
        self.connect_btn.grid(row=0, column=2, padx=8)

        # Status indicator: colored dot + text.
        status = ttk.Frame(self)
        status.pack(anchor="w", padx=12, pady=(0, 4))
        self.status_dot = tk.Canvas(status, width=14, height=14, highlightthickness=0)
        self._dot = self.status_dot.create_oval(2, 2, 12, 12, fill="red", outline="")
        self.status_dot.pack(side="left", padx=(0, 6))
        self.status_var = tk.StringVar(value="Disconnected")
        self.status_label = tk.Label(status, textvariable=self.status_var, fg="red")
        self.status_label.pack(side="left")

        bal = ttk.LabelFrame(self, text="Account Balance")
        bal.pack(fill="both", expand=True, padx=10, pady=10)

        self.tree = ttk.Treeview(bal, columns=("value", "currency"), show="tree headings", height=10)
        self.tree.heading("#0", text="Metric")
        self.tree.heading("value", text="Value")
        self.tree.heading("currency", text="Currency")
        self.tree.column("#0", width=200)
        self.tree.column("value", width=160, anchor="e")
        self.tree.column("currency", width=90, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        footer = ttk.Frame(self)
        footer.pack(pady=(0, 10))

        self.refresh_btn = ttk.Button(footer, text="Refresh Balance", command=self._refresh, state="disabled")
        self.refresh_btn.pack(side="left", padx=6)

        ttk.Label(footer, text="Auto-refresh").pack(side="left", padx=(12, 4))
        self.freq_var = tk.StringVar(value="Manual")
        self.freq_box = ttk.Combobox(
            footer,
            textvariable=self.freq_var,
            values=list(REFRESH_OPTIONS),
            state="readonly",
            width=8,
        )
        self.freq_box.pack(side="left")
        self.freq_box.bind("<<ComboboxSelected>>", lambda _e: self._reschedule_auto_refresh())

    # --- actions ---------------------------------------------------------
    def _set_status(self, text: str, color: str) -> None:
        self.status_var.set(text)
        self.status_label.config(fg=color)
        self.status_dot.itemconfig(self._dot, fill=color)

    def _toggle_connect(self) -> None:
        if self.client.connected:
            self.client.disconnect()
            return

        port = TARGETS[self.target_var.get()]
        self._set_status("Connecting...", "orange")
        self.connect_btn.config(state="disabled")
        self.target_box.config(state="disabled")
        future = self.client.connect(HOST, port, CLIENT_ID)
        future.add_done_callback(lambda f: self._events.put(("connect_done", f)))

    def _refresh(self) -> None:
        self._set_status("Fetching balance...", "orange")
        future = self.client.account_summary()
        future.add_done_callback(lambda f: self._events.put(("summary", f)))

    def _reschedule_auto_refresh(self) -> None:
        """(Re)arm the auto-refresh timer based on the combobox selection."""
        if self._refresh_after_id is not None:
            self.after_cancel(self._refresh_after_id)
            self._refresh_after_id = None
        secs = REFRESH_OPTIONS[self.freq_var.get()]
        if secs and self.client.connected:
            self._refresh_after_id = self.after(secs * 1000, self._auto_tick)

    def _auto_tick(self) -> None:
        self._refresh_after_id = None
        if self.client.connected:
            self._refresh()
        self._reschedule_auto_refresh()

    # --- event pump ------------------------------------------------------
    def _poll_events(self) -> None:
        try:
            while True:
                kind, payload = self._events.get_nowait()
                self._handle_event(kind, payload)
        except queue.Empty:
            pass
        self.after(100, self._poll_events)

    def _handle_event(self, kind: str, payload) -> None:
        if kind == "connect_done":
            exc = payload.exception()
            if exc is not None:
                self._set_status(f"Connection failed: {exc}", "red")
                self.connect_btn.config(state="normal", text="Connect")
                self.target_box.config(state="readonly")
                return
            self._set_status("Connected", "green")
            self.connect_btn.config(state="normal", text="Disconnect")
            self.refresh_btn.config(state="normal")
            self._refresh()
            self._reschedule_auto_refresh()
        elif kind == "summary":
            exc = payload.exception()
            if exc is not None:
                self._set_status(f"Failed to fetch balance: {exc}", "red")
                return
            self._populate_balance(payload.result())
            self._set_status("Connected", "green")
        elif kind == "disconnected":
            self._set_status("Disconnected", "red")
            self.connect_btn.config(state="normal", text="Connect")
            self.target_box.config(state="readonly")
            self.refresh_btn.config(state="disabled")
            self._reschedule_auto_refresh()
            for item in self.tree.get_children():
                self.tree.delete(item)

    def _populate_balance(self, rows: list) -> None:
        by_tag = {row.tag: row for row in rows}
        for item in self.tree.get_children():
            self.tree.delete(item)
        for tag in BALANCE_TAGS:
            row = by_tag.get(tag)
            if row is None:
                continue
            try:
                value = f"{float(row.value):,.2f}"
            except ValueError:
                value = row.value
            self.tree.insert("", "end", text=tag, values=(value, row.currency))

    def _on_close(self) -> None:
        if self.client.connected:
            self.client.disconnect()
        self.destroy()


if __name__ == "__main__":
    TradingApp().mainloop()
