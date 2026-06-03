"""Tkinter UI that connects to IB TWS/Gateway and shows the account balance.

Run with the project venv:
    ~/ib-trading/.venv/bin/python ~/ib-trading/app.py

Prerequisite: in TWS go to File > Global Configuration > API > Settings,
enable "Enable ActiveX and Socket Clients", and confirm the socket port
(7497 for TWS paper trading by default).
"""

from __future__ import annotations

import queue
import tkinter as tk
from tkinter import ttk

from ib_client import IBClient

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


class TradingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("IB Trading Dashboard")
        self.geometry("560x460")

        self.client = IBClient()
        self.client.on_disconnect(lambda: self._events.put(("disconnected", None)))

        # Thread-safe channel for messages from the IB worker thread to the UI.
        self._events: queue.Queue = queue.Queue()

        self._build_widgets()
        self.after(100, self._poll_events)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_widgets(self) -> None:
        conn = ttk.LabelFrame(self, text="Connection")
        conn.pack(fill="x", padx=10, pady=10)

        ttk.Label(conn, text="Host").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.host_var = tk.StringVar(value="127.0.0.1")
        ttk.Entry(conn, textvariable=self.host_var, width=12).grid(row=0, column=1, padx=4)

        ttk.Label(conn, text="Port").grid(row=0, column=2, padx=4, sticky="w")
        self.port_var = tk.StringVar(value="7497")
        ttk.Entry(conn, textvariable=self.port_var, width=7).grid(row=0, column=3, padx=4)

        ttk.Label(conn, text="Client ID").grid(row=0, column=4, padx=4, sticky="w")
        self.client_id_var = tk.StringVar(value="1")
        ttk.Entry(conn, textvariable=self.client_id_var, width=5).grid(row=0, column=5, padx=4)

        self.connect_btn = ttk.Button(conn, text="Connect", command=self._toggle_connect)
        self.connect_btn.grid(row=0, column=6, padx=8)

        self.status_var = tk.StringVar(value="Disconnected")
        ttk.Label(self, textvariable=self.status_var, foreground="#888").pack(anchor="w", padx=12)

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

        self.refresh_btn = ttk.Button(self, text="Refresh Balance", command=self._refresh, state="disabled")
        self.refresh_btn.pack(pady=(0, 10))

    # --- actions ---------------------------------------------------------
    def _toggle_connect(self) -> None:
        if self.client.connected:
            self.client.disconnect()
            return
        try:
            port = int(self.port_var.get())
            client_id = int(self.client_id_var.get())
        except ValueError:
            self.status_var.set("Port and Client ID must be numbers")
            return

        self.status_var.set("Connecting...")
        self.connect_btn.config(state="disabled")
        future = self.client.connect(self.host_var.get().strip(), port, client_id)
        future.add_done_callback(lambda f: self._events.put(("connect_done", f)))

    def _refresh(self) -> None:
        self.status_var.set("Fetching balance...")
        future = self.client.account_summary()
        future.add_done_callback(lambda f: self._events.put(("summary", f)))

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
                self.status_var.set(f"Connection failed: {exc}")
                self.connect_btn.config(state="normal", text="Connect")
                return
            self.status_var.set("Connected")
            self.connect_btn.config(state="normal", text="Disconnect")
            self.refresh_btn.config(state="normal")
            self._refresh()
        elif kind == "summary":
            exc = payload.exception()
            if exc is not None:
                self.status_var.set(f"Failed to fetch balance: {exc}")
                return
            self._populate_balance(payload.result())
            self.status_var.set("Connected")
        elif kind == "disconnected":
            self.status_var.set("Disconnected")
            self.connect_btn.config(state="normal", text="Connect")
            self.refresh_btn.config(state="disabled")
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
