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
import math
import queue
import threading
import tkinter as tk
from concurrent.futures import Future
from datetime import datetime
from tkinter import ttk
from typing import Callable

from ib_async import IB, Stock

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

# NASDAQ-100 constituents (US tech-heavy index). May drift over time as the
# index is reconstituted, but good enough for a price watchlist.
NASDAQ100 = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD", "AMGN",
    "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AZN", "BIIB", "BKNG", "BKR",
    "CCEP", "CDNS", "CDW", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO",
    "CSGP", "CSX", "CTAS", "CTSH", "DASH", "DDOG", "DXCM", "EA", "EXC", "FANG",
    "FAST", "FTNT", "GEHC", "GFS", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN",
    "INTC", "INTU", "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "LULU", "MAR",
    "MCHP", "MDLZ", "MELI", "META", "MNST", "MRVL", "MSFT", "MU", "NFLX", "NVDA",
    "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", "PLTR",
    "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX", "SNPS", "TEAM", "TMUS", "TSLA",
    "TTD", "TTWO", "TXN", "VRSK", "VRTX", "WBD", "WDAY", "XEL", "ZS",
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

    def snapshot(self) -> Future:
        """Resolve to (net_liquidation: float | None, portfolio_items)."""
        return self._submit(self._snapshot())

    async def _snapshot(self):
        summary = await self._ib.accountSummaryAsync()
        net_liq = next(
            (float(v.value) for v in summary if v.tag == "NetLiquidation"), None
        )
        return net_liq, self._ib.portfolio()

    def instrument_prices(self, symbols: list[str]) -> Future:
        """Resolve to {symbol: price}; price is NaN when no data is available."""
        return self._submit(self._instrument_prices(symbols))

    async def _instrument_prices(self, symbols: list[str]) -> dict[str, float]:
        # Delayed data (type 3) needs no live market-data subscription.
        self._ib.reqMarketDataType(3)
        prices: dict[str, float] = {}
        # primaryExchange disambiguates SMART lookups (NASDAQ-100 is NASDAQ-listed).
        contracts = [Stock(s, "SMART", "USD", primaryExchange="NASDAQ") for s in symbols]
        # reqTickers requires qualified contracts (with a conId); qualifyContracts
        # returns None for any symbol it can't resolve, so drop those.
        qualified = await self._ib.qualifyContractsAsync(*contracts)
        valid = [c for c in qualified if c is not None and c.conId]
        # Batch to stay under IB's simultaneous market-data line limit.
        for i in range(0, len(valid), 50):
            tickers = await self._ib.reqTickersAsync(*valid[i : i + 50])
            for t in tickers:
                prices[t.contract.symbol] = t.marketPrice()
        return prices

    def on_disconnect(self, callback: Callable[[], None]) -> None:
        self._ib.disconnectedEvent += lambda *_: callback()


class TradingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("IB Trading Dashboard")
        self.geometry("720x500")

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

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=(0, 6))
        self._build_balance_tab(self.notebook)
        self._build_instruments_tab(self.notebook)

        footer = ttk.Frame(self)
        footer.pack(pady=(0, 10))

        self.refresh_btn = ttk.Button(footer, text="Refresh", command=self._refresh, state="disabled")
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

    def _build_balance_tab(self, notebook: ttk.Notebook) -> None:
        tab = ttk.Frame(notebook)
        notebook.add(tab, text="Balance")

        bal = ttk.LabelFrame(tab, text="Net Liquidation")
        bal.pack(fill="x", padx=6, pady=6)
        self.netliq_var = tk.StringVar(value="—")
        tk.Label(bal, textvariable=self.netliq_var, font=("Helvetica", 22, "bold")).pack(
            anchor="w", padx=10, pady=8
        )

        pos = ttk.LabelFrame(tab, text="Positions")
        pos.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        cols = ("amount", "avgcost", "price", "value", "pnl")
        self.tree = ttk.Treeview(pos, columns=cols, show="tree headings", height=10)
        self.tree.heading("#0", text="Instrument")
        self.tree.heading("amount", text="Amount")
        self.tree.heading("avgcost", text="Avg Cost")
        self.tree.heading("price", text="Latest Price")
        self.tree.heading("value", text="Value")
        self.tree.heading("pnl", text="Unrealized P/L")
        self.tree.column("#0", width=110)
        self.tree.column("amount", width=80, anchor="e")
        self.tree.column("avgcost", width=110, anchor="e")
        self.tree.column("price", width=110, anchor="e")
        self.tree.column("value", width=120, anchor="e")
        self.tree.column("pnl", width=120, anchor="e")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        self.balance_time_var = tk.StringVar(value="Last refreshed: —")
        ttk.Label(tab, textvariable=self.balance_time_var, foreground="#888").pack(
            anchor="e", padx=8, pady=(0, 4)
        )

    def _build_instruments_tab(self, notebook: ttk.Notebook) -> None:
        tab = ttk.Frame(notebook)
        self.inst_tab = tab
        notebook.add(tab, text="Instruments")

        ttk.Label(tab, text="NASDAQ-100", font=("Helvetica", 12, "bold")).pack(
            anchor="w", padx=8, pady=(6, 2)
        )

        wrap = ttk.Frame(tab)
        wrap.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.inst_tree = ttk.Treeview(wrap, columns=("price",), show="tree headings", height=12)
        self.inst_tree.heading("#0", text="Instrument")
        self.inst_tree.heading("price", text="Latest Price")
        self.inst_tree.column("#0", width=160)
        self.inst_tree.column("price", width=160, anchor="e")
        scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.inst_tree.yview)
        self.inst_tree.configure(yscrollcommand=scroll.set)
        self.inst_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        bar = ttk.Frame(tab)
        bar.pack(fill="x", padx=8, pady=(0, 4))
        self.inst_btn = ttk.Button(
            bar, text="Refresh Prices", command=self._refresh_instruments, state="disabled"
        )
        self.inst_btn.pack(side="left")
        self.instruments_time_var = tk.StringVar(value="Last refreshed: —")
        ttk.Label(bar, textvariable=self.instruments_time_var, foreground="#888").pack(side="right")

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
        self._set_status("Fetching...", "orange")
        future = self.client.snapshot()
        future.add_done_callback(lambda f: self._events.put(("snapshot", f)))

    def _refresh_instruments(self) -> None:
        self.inst_btn.config(state="disabled")
        self.instruments_time_var.set("Fetching prices...")
        future = self.client.instrument_prices(NASDAQ100)
        future.add_done_callback(lambda f: self._events.put(("instruments", f)))

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
            # Only refresh the (heavier) instrument prices when that tab is visible.
            if self.notebook.select() == str(self.inst_tab):
                self._refresh_instruments()
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
            self.inst_btn.config(state="normal")
            self._refresh()
            self._refresh_instruments()
            self._reschedule_auto_refresh()
        elif kind == "snapshot":
            exc = payload.exception()
            if exc is not None:
                self._set_status(f"Failed to fetch: {exc}", "red")
                return
            net_liq, portfolio = payload.result()
            self._populate(net_liq, portfolio)
            self.balance_time_var.set(f"Last refreshed: {datetime.now():%H:%M:%S}")
            self._set_status("Connected", "green")
        elif kind == "instruments":
            self.inst_btn.config(state="normal" if self.client.connected else "disabled")
            exc = payload.exception()
            if exc is not None:
                self.instruments_time_var.set(f"Failed: {exc}")
                return
            self._populate_instruments(payload.result())
            self.instruments_time_var.set(f"Last refreshed: {datetime.now():%H:%M:%S}")
        elif kind == "disconnected":
            self._set_status("Disconnected", "red")
            self.connect_btn.config(state="normal", text="Connect")
            self.target_box.config(state="readonly")
            self.refresh_btn.config(state="disabled")
            self.inst_btn.config(state="disabled")
            self._reschedule_auto_refresh()
            self.netliq_var.set("—")
            self.balance_time_var.set("Last refreshed: —")
            self.instruments_time_var.set("Last refreshed: —")
            for item in self.tree.get_children():
                self.tree.delete(item)
            for item in self.inst_tree.get_children():
                self.inst_tree.delete(item)

    def _populate(self, net_liq, portfolio) -> None:
        self.netliq_var.set(f"{net_liq:,.2f}" if net_liq is not None else "—")
        for item in self.tree.get_children():
            self.tree.delete(item)
        for it in portfolio:
            symbol = it.contract.symbol
            ccy = it.contract.currency
            self.tree.insert(
                "",
                "end",
                text=symbol,
                values=(
                    f"{it.position:,g}",
                    f"{it.averageCost:,.2f} {ccy}",
                    f"{it.marketPrice:,.2f} {ccy}",
                    f"{it.marketValue:,.2f} {ccy}",
                    f"{it.unrealizedPNL:,.2f} {ccy}",
                ),
            )

    def _populate_instruments(self, prices: dict[str, float]) -> None:
        for item in self.inst_tree.get_children():
            self.inst_tree.delete(item)
        for sym in NASDAQ100:
            price = prices.get(sym)
            text = "—" if price is None or math.isnan(price) else f"{price:,.2f}"
            self.inst_tree.insert("", "end", text=sym, values=(text,))

    def _on_close(self) -> None:
        if self.client.connected:
            self.client.disconnect()
        self.destroy()


if __name__ == "__main__":
    TradingApp().mainloop()
