import asyncio

# Python 3.14 fix: create a loop before importing ib_insync/eventkit
_main_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_main_loop)

import math
import queue
import threading
import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from ib_insync import IB, Stock, Future, MarketOrder, ExecutionFilter


AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")


class IBWorker:
    """
    Runs all IB operations on one dedicated thread with one persistent event loop.
    This avoids Python 3.14 + ib_insync timeout issues caused by per-call loops.
    """
    def __init__(self):
        self.ib = None
        self.thread = None
        self.loop = None
        self.started = threading.Event()
        self.task_queue = queue.Queue()
        self.running = False

    def start(self):
        if self.thread and self.thread.is_alive():
            return

        self.running = True
        self.thread = threading.Thread(target=self._thread_main, daemon=True)
        self.thread.start()
        self.started.wait(timeout=5)

    def _thread_main(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.ib = IB()
        self.started.set()

        while self.running:
            try:
                item = self.task_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is None:
                break

            func, args, kwargs, result_q = item

            try:
                result = func(*args, **kwargs)
                result_q.put(("ok", result))
            except Exception as e:
                result_q.put(("err", e))

        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
        except Exception:
            pass

        try:
            self.loop.stop()
        except Exception:
            pass

        try:
            self.loop.close()
        except Exception:
            pass

    def call(self, func, *args, **kwargs):
        if not self.thread or not self.thread.is_alive():
            raise RuntimeError("IB worker is not started")

        result_q = queue.Queue()
        self.task_queue.put((func, args, kwargs, result_q))
        status, payload = result_q.get()

        if status == "err":
            raise payload

        return payload

    def stop(self):
        self.running = False
        try:
            self.task_queue.put(None)
        except Exception:
            pass

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)


class ConnectionApp:
    def __init__(self, root):
        self.root = root
        self.root.title("IB Connection")
        self.root.geometry("1550x1040")

        # ---------------- IB state ----------------
        self.ibw = IBWorker()
        self.ibw.start()

        self.ib_connected = False
        self.connected_account_type = None
        self.current_account_id = None

        # ---------------- UI/account data ----------------
        self.buying_power_usd = 0.00
        self.cash_amount_usd = 0.00
        self.positions = []
        self.transactions = []

        # ---------------- Quote state ----------------
        self.bid_price = None
        self.ask_price = None
        self.quote_update_interval_ms = 3000
        self.quote_after_id = None
        self.quote_loading = False

        # ---------------- Chart state ----------------
        self.chart_update_interval_ms = 15000
        self.chart_after_id = None
        self.chart_loading = False

        # ---------------- Bot state ----------------
        self.bot_running = False
        self.last_bot_signal_id = None

        # ---------------- Backtest state ----------------
        self.backtest_loading = False
        self.backtest_results = []
        self.backtest_plot_df = None
        self.backtest_plot_signals = []

        # ---------------- Instrument config ----------------
        self.instrument_market_map = {
            "NVDA": "NVDA",
            "AAPL": "AAPL",
            "MSFT": "MSFT",
            "TSLA": "TSLA",
            "AMZN": "AMZN",
            "META": "META",
            "GOOGL": "GOOGL",
            "EUR Jun26 CME": "6E=F"
        }

        self.instrument_contract_config = {
            "NVDA": {"type": "STK", "symbol": "NVDA", "exchange": "SMART", "currency": "USD"},
            "AAPL": {"type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
            "MSFT": {"type": "STK", "symbol": "MSFT", "exchange": "SMART", "currency": "USD"},
            "TSLA": {"type": "STK", "symbol": "TSLA", "exchange": "SMART", "currency": "USD"},
            "AMZN": {"type": "STK", "symbol": "AMZN", "exchange": "SMART", "currency": "USD"},
            "META": {"type": "STK", "symbol": "META", "exchange": "SMART", "currency": "USD"},
            "GOOGL": {"type": "STK", "symbol": "GOOGL", "exchange": "SMART", "currency": "USD"},
            "EUR Jun26 CME": {
                "type": "FUT",
                "symbol": "EUR",
                "exchange": "CME",
                "currency": "USD",
                "expiry": "202606",
                "multiplier": 125000
            }
        }

        # ---------------- Main container ----------------
        main_frame = ttk.Frame(root, padding=10)
        main_frame.pack(fill="both", expand=True)

        # ---------------- Row 1 (always visible) ----------------
        row1 = ttk.Frame(main_frame)
        row1.pack(fill="x", pady=(0, 10))

        ttk.Label(row1, text="Account Type:").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")

        self.account_type = tk.StringVar(value="Paper")
        self.account_combo = ttk.Combobox(
            row1,
            textvariable=self.account_type,
            values=["Paper", "Live"],
            state="readonly",
            width=12
        )
        self.account_combo.grid(row=0, column=1, padx=(0, 16), pady=5, sticky="w")

        self.connect_button = ttk.Button(
            row1,
            text="Connect",
            command=self.toggle_connection,
            width=12
        )
        self.connect_button.grid(row=0, column=2, padx=(0, 16), pady=5, sticky="w")

        ttk.Label(row1, text="Status:").grid(row=0, column=3, padx=(0, 6), pady=5, sticky="w")

        self.status_var = tk.StringVar(value="Disconnected")
        self.status_label = ttk.Label(
            row1,
            textvariable=self.status_var,
            foreground="red",
            font=("Arial", 10, "bold"),
            width=16
        )
        self.status_label.grid(row=0, column=4, padx=(0, 16), pady=5, sticky="w")

        ttk.Label(row1, text="IB Message / Error:").grid(row=0, column=5, padx=(0, 6), pady=5, sticky="w")

        self.ib_message_var = tk.StringVar(value="-")
        self.ib_message_label = ttk.Label(
            row1,
            textvariable=self.ib_message_var,
            foreground="blue",
            anchor="w"
        )
        self.ib_message_label.grid(row=0, column=6, padx=(0, 6), pady=5, sticky="w")

        row1.columnconfigure(6, weight=1)

        # ---------------- Tabs below Row 1 ----------------
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill="both", expand=True)

        self.trade_tab = ttk.Frame(self.notebook, padding=10)
        self.backtest_tab = ttk.Frame(self.notebook, padding=10)

        self.notebook.add(self.trade_tab, text="Trade")
        self.notebook.add(self.backtest_tab, text="Back Test")

        # =========================================================
        # TRADE TAB
        # =========================================================
        row2 = ttk.Frame(self.trade_tab)
        row2.pack(fill="x", pady=(0, 10))

        ttk.Label(row2, text="Buying Power (USD):").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")

        self.buying_power_var = tk.StringVar(value="0.00")
        self.buying_power_label = ttk.Label(
            row2,
            textvariable=self.buying_power_var,
            font=("Arial", 10, "bold"),
            width=18,
            anchor="w"
        )
        self.buying_power_label.grid(row=0, column=1, padx=(0, 20), pady=5, sticky="w")

        ttk.Label(row2, text="Cash Amount in Account (USD):").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")

        self.cash_amount_var = tk.StringVar(value="0.00")
        self.cash_amount_label = ttk.Label(
            row2,
            textvariable=self.cash_amount_var,
            font=("Arial", 10, "bold"),
            width=18,
            anchor="w"
        )
        self.cash_amount_label.grid(row=0, column=3, padx=(0, 6), pady=5, sticky="w")

        ttk.Label(row2, text="Bot Status:").grid(row=0, column=4, padx=(20, 6), pady=5, sticky="w")

        self.bot_status_var = tk.StringVar(value="Stopped")
        self.bot_status_label = ttk.Label(
            row2,
            textvariable=self.bot_status_var,
            foreground="red",
            font=("Arial", 10, "bold"),
            width=14,
            anchor="w"
        )
        self.bot_status_label.grid(row=0, column=5, padx=(0, 6), pady=5, sticky="w")

        row3 = ttk.Frame(self.trade_tab)
        row3.pack(fill="x", pady=(0, 12))

        ttk.Label(row3, text="Instrument:").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")

        self.instrument_var = tk.StringVar(value="EUR Jun26 CME")
        self.instrument_combo = ttk.Combobox(
            row3,
            textvariable=self.instrument_var,
            values=list(self.instrument_contract_config.keys()),
            state="readonly",
            width=20
        )
        self.instrument_combo.grid(row=0, column=1, padx=(0, 10), pady=5, sticky="w")
        self.instrument_combo.bind("<<ComboboxSelected>>", self.on_instrument_change)

        ttk.Label(row3, text="Bid:").grid(row=0, column=2, padx=(0, 4), pady=5, sticky="w")
        self.bid_var = tk.StringVar(value="-")
        self.bid_label = ttk.Label(
            row3,
            textvariable=self.bid_var,
            font=("Arial", 10, "bold"),
            width=14,
            anchor="w"
        )
        self.bid_label.grid(row=0, column=3, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(row3, text="Ask:").grid(row=0, column=4, padx=(0, 4), pady=5, sticky="w")
        self.ask_var = tk.StringVar(value="-")
        self.ask_label = ttk.Label(
            row3,
            textvariable=self.ask_var,
            font=("Arial", 10, "bold"),
            width=14,
            anchor="w"
        )
        self.ask_label.grid(row=0, column=5, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(row3, text="Amount (USD):").grid(row=0, column=6, padx=(0, 6), pady=5, sticky="w")

        self.amount_var = tk.StringVar()
        self.amount_entry = ttk.Entry(row3, textvariable=self.amount_var, width=14)
        self.amount_entry.grid(row=0, column=7, padx=(0, 16), pady=5, sticky="w")

        self.buy_button = ttk.Button(row3, text="Buy", command=self.buy_instrument, width=10)
        self.buy_button.grid(row=0, column=8, padx=(0, 8), pady=5, sticky="w")

        self.sell_button = ttk.Button(row3, text="Sell", command=self.sell_instrument, width=10)
        self.sell_button.grid(row=0, column=9, padx=(0, 8), pady=5, sticky="w")

        ttk.Label(row3, text="TP/SL:").grid(row=0, column=10, padx=(18, 6), pady=5, sticky="w")

        self.tp_sl_var = tk.StringVar(value="Yes")
        self.tp_sl_combo = ttk.Combobox(
            row3,
            textvariable=self.tp_sl_var,
            values=["Yes", "No"],
            state="readonly",
            width=8
        )
        self.tp_sl_combo.grid(row=0, column=11, padx=(0, 16), pady=5, sticky="w")

        self.bot_button = ttk.Button(
            row3,
            text="Start Bot",
            command=self.toggle_bot,
            width=12
        )
        self.bot_button.grid(row=0, column=12, padx=(0, 8), pady=5, sticky="w")

        self.refresh_ib_button = ttk.Button(
            row3,
            text="Refresh IB",
            command=self.refresh_ib_state,
            width=12
        )
        self.refresh_ib_button.grid(row=0, column=13, padx=(0, 8), pady=5, sticky="w")

        row4 = ttk.Frame(self.trade_tab)
        row4.pack(fill="x", pady=(0, 12))

        positions_group = ttk.LabelFrame(row4, text="Positions (from IB)", padding=8)
        positions_group.pack(side="left", fill="both", expand=True, padx=(0, 8))

        self.positions_tree = ttk.Treeview(
            positions_group,
            columns=("instrument", "amount", "close_position"),
            show="headings",
            height=10
        )
        self.positions_tree.heading("instrument", text="Instrument")
        self.positions_tree.heading("amount", text="Amount")
        self.positions_tree.heading("close_position", text="Close Position")

        self.positions_tree.column("instrument", width=240, anchor="w")
        self.positions_tree.column("amount", width=140, anchor="e")
        self.positions_tree.column("close_position", width=120, anchor="center")
        self.positions_tree.pack(side="left", fill="both", expand=True)

        self.positions_tree.bind("<Button-1>", self.on_positions_tree_click)
        self.positions_tree.bind("<Motion>", self.on_positions_tree_motion)

        positions_scroll = ttk.Scrollbar(positions_group, orient="vertical", command=self.positions_tree.yview)
        self.positions_tree.configure(yscrollcommand=positions_scroll.set)
        positions_scroll.pack(side="right", fill="y")

        transactions_group = ttk.LabelFrame(row4, text="Transactions (from IB)", padding=8)
        transactions_group.pack(side="left", fill="both", expand=True, padx=(8, 0))

        self.transactions_tree = ttk.Treeview(
            transactions_group,
            columns=("time", "side", "instrument", "qty", "price", "commission"),
            show="headings",
            height=10
        )
        self.transactions_tree.heading("time", text="Time")
        self.transactions_tree.heading("side", text="Side")
        self.transactions_tree.heading("instrument", text="Instrument")
        self.transactions_tree.heading("qty", text="Qty")
        self.transactions_tree.heading("price", text="Price")
        self.transactions_tree.heading("commission", text="Commission")

        self.transactions_tree.column("time", width=110, anchor="w")
        self.transactions_tree.column("side", width=80, anchor="center")
        self.transactions_tree.column("instrument", width=170, anchor="w")
        self.transactions_tree.column("qty", width=90, anchor="e")
        self.transactions_tree.column("price", width=95, anchor="e")
        self.transactions_tree.column("commission", width=110, anchor="e")
        self.transactions_tree.pack(side="left", fill="both", expand=True)

        transactions_scroll = ttk.Scrollbar(transactions_group, orient="vertical", command=self.transactions_tree.yview)
        self.transactions_tree.configure(yscrollcommand=transactions_scroll.set)
        transactions_scroll.pack(side="right", fill="y")

        chart_group = ttk.LabelFrame(self.trade_tab, text="Price Chart with Bollinger Bands", padding=10)
        chart_group.pack(fill="both", expand=True)

        self.chart_status_var = tk.StringVar(value="Connect first to load the chart")
        self.chart_status_label = ttk.Label(chart_group, textvariable=self.chart_status_var, font=("Arial", 10))
        self.chart_status_label.pack(anchor="w", pady=(0, 8))

        self.figure = Figure(figsize=(11.8, 6.2), dpi=100)
        self.ax = self.figure.add_subplot(111)

        self.canvas = FigureCanvasTkAgg(self.figure, master=chart_group)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)

        self.show_chart_placeholder("Connect first to load the chart")

        # =========================================================
        # BACK TEST TAB
        # =========================================================
        bt_controls = ttk.LabelFrame(self.backtest_tab, text="Back Test Controls", padding=10)
        bt_controls.pack(fill="x", pady=(0, 10))

        ttk.Label(bt_controls, text="Instrument:").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")

        self.backtest_instrument_var = tk.StringVar(value="EUR Jun26 CME")
        self.backtest_instrument_combo = ttk.Combobox(
            bt_controls,
            textvariable=self.backtest_instrument_var,
            values=list(self.instrument_contract_config.keys()),
            state="readonly",
            width=20
        )
        self.backtest_instrument_combo.grid(row=0, column=1, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(bt_controls, text="Strategy:").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")

        self.backtest_strategy_var = tk.StringVar(value="Bollinger Bands")
        self.backtest_strategy_combo = ttk.Combobox(
            bt_controls,
            textvariable=self.backtest_strategy_var,
            values=["Bollinger Bands"],
            state="readonly",
            width=18
        )
        self.backtest_strategy_combo.grid(row=0, column=3, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(bt_controls, text="Test Start Date:").grid(row=0, column=4, padx=(0, 6), pady=5, sticky="w")
        self.backtest_start_var = tk.StringVar(value=(datetime.now(AMSTERDAM_TZ) - timedelta(days=30)).strftime("%Y-%m-%d"))
        self.backtest_start_entry = ttk.Entry(bt_controls, textvariable=self.backtest_start_var, width=14)
        self.backtest_start_entry.grid(row=0, column=5, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(bt_controls, text="Test End Date:").grid(row=0, column=6, padx=(0, 6), pady=5, sticky="w")
        self.backtest_end_var = tk.StringVar(value=datetime.now(AMSTERDAM_TZ).strftime("%Y-%m-%d"))
        self.backtest_end_entry = ttk.Entry(bt_controls, textvariable=self.backtest_end_var, width=14)
        self.backtest_end_entry.grid(row=0, column=7, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(bt_controls, text="Amount (USD):").grid(row=1, column=0, padx=(0, 6), pady=5, sticky="w")
        self.backtest_amount_var = tk.StringVar(value="10000")
        self.backtest_amount_entry = ttk.Entry(bt_controls, textvariable=self.backtest_amount_var, width=14)
        self.backtest_amount_entry.grid(row=1, column=1, padx=(0, 12), pady=5, sticky="w")

        ttk.Label(bt_controls, text="TP/SL:").grid(row=1, column=2, padx=(0, 6), pady=5, sticky="w")
        self.backtest_tp_sl_var = tk.StringVar(value="Yes")
        self.backtest_tp_sl_combo = ttk.Combobox(
            bt_controls,
            textvariable=self.backtest_tp_sl_var,
            values=["Yes", "No"],
            state="readonly",
            width=10
        )
        self.backtest_tp_sl_combo.grid(row=1, column=3, padx=(0, 12), pady=5, sticky="w")

        self.backtest_button = ttk.Button(
            bt_controls,
            text="Test",
            command=self.start_backtest,
            width=12
        )
        self.backtest_button.grid(row=1, column=4, padx=(10, 12), pady=5, sticky="w")

        self.backtest_status_var = tk.StringVar(value="Ready")
        self.backtest_status_label = ttk.Label(
            bt_controls,
            textvariable=self.backtest_status_var,
            font=("Arial", 10, "bold"),
            foreground="blue"
        )
        self.backtest_status_label.grid(row=1, column=5, columnspan=3, padx=(10, 0), pady=5, sticky="w")

        bt_summary = ttk.LabelFrame(self.backtest_tab, text="Back Test Summary", padding=10)
        bt_summary.pack(fill="x", pady=(0, 10))

        self.backtest_long_count_var = tk.StringVar(value="0")
        self.backtest_short_count_var = tk.StringVar(value="0")
        self.backtest_total_trades_var = tk.StringVar(value="0")
        self.backtest_total_pnl_var = tk.StringVar(value="0.00")

        ttk.Label(bt_summary, text="Long Positions Opened:").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")
        ttk.Label(bt_summary, textvariable=self.backtest_long_count_var, font=("Arial", 10, "bold")).grid(
            row=0, column=1, padx=(0, 20), pady=5, sticky="w"
        )

        ttk.Label(bt_summary, text="Short Positions Opened:").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")
        ttk.Label(bt_summary, textvariable=self.backtest_short_count_var, font=("Arial", 10, "bold")).grid(
            row=0, column=3, padx=(0, 20), pady=5, sticky="w"
        )

        ttk.Label(bt_summary, text="Closed Trades:").grid(row=0, column=4, padx=(0, 6), pady=5, sticky="w")
        ttk.Label(bt_summary, textvariable=self.backtest_total_trades_var, font=("Arial", 10, "bold")).grid(
            row=0, column=5, padx=(0, 20), pady=5, sticky="w"
        )

        ttk.Label(bt_summary, text="Total Revenue / Loss (USD):").grid(row=0, column=6, padx=(0, 6), pady=5, sticky="w")
        self.backtest_total_pnl_label = ttk.Label(
            bt_summary,
            textvariable=self.backtest_total_pnl_var,
            font=("Arial", 10, "bold")
        )
        self.backtest_total_pnl_label.grid(row=0, column=7, padx=(0, 6), pady=5, sticky="w")

        bt_chart_group = ttk.LabelFrame(self.backtest_tab, text="Back Test Price Graph with Signals", padding=10)
        bt_chart_group.pack(fill="both", expand=True, pady=(0, 10))

        self.backtest_chart_status_var = tk.StringVar(value="Run a test to see graph")
        self.backtest_chart_status_label = ttk.Label(bt_chart_group, textvariable=self.backtest_chart_status_var, font=("Arial", 10))
        self.backtest_chart_status_label.pack(anchor="w", pady=(0, 8))

        self.backtest_figure = Figure(figsize=(11.8, 4.8), dpi=100)
        self.backtest_ax = self.backtest_figure.add_subplot(111)

        self.backtest_canvas = FigureCanvasTkAgg(self.backtest_figure, master=bt_chart_group)
        self.backtest_canvas_widget = self.backtest_canvas.get_tk_widget()
        self.backtest_canvas_widget.pack(fill="both", expand=True)

        self.show_backtest_chart_placeholder("Run a test to see graph")

        bt_trades_group = ttk.LabelFrame(self.backtest_tab, text="Back Test Trades", padding=10)
        bt_trades_group.pack(fill="both", expand=True)

        self.backtest_tree = ttk.Treeview(
            bt_trades_group,
            columns=("entry_time", "exit_time", "side", "qty", "entry_price", "exit_price", "pnl", "reason"),
            show="headings",
            height=14
        )

        self.backtest_tree.heading("entry_time", text="Entry Time")
        self.backtest_tree.heading("exit_time", text="Exit Time")
        self.backtest_tree.heading("side", text="Side")
        self.backtest_tree.heading("qty", text="Qty")
        self.backtest_tree.heading("entry_price", text="Entry Price")
        self.backtest_tree.heading("exit_price", text="Exit Price")
        self.backtest_tree.heading("pnl", text="P/L (USD)")
        self.backtest_tree.heading("reason", text="Close Reason")

        self.backtest_tree.column("entry_time", width=150, anchor="w")
        self.backtest_tree.column("exit_time", width=150, anchor="w")
        self.backtest_tree.column("side", width=80, anchor="center")
        self.backtest_tree.column("qty", width=90, anchor="e")
        self.backtest_tree.column("entry_price", width=100, anchor="e")
        self.backtest_tree.column("exit_price", width=100, anchor="e")
        self.backtest_tree.column("pnl", width=110, anchor="e")
        self.backtest_tree.column("reason", width=180, anchor="w")

        self.backtest_tree.pack(side="left", fill="both", expand=True)

        bt_scroll = ttk.Scrollbar(bt_trades_group, orient="vertical", command=self.backtest_tree.yview)
        self.backtest_tree.configure(yscrollcommand=bt_scroll.set)
        bt_scroll.pack(side="right", fill="y")

        self.update_ui()
        self.refresh_positions_table()
        self.refresh_transactions_table()
        self.refresh_backtest_table()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # =========================================================
    # Helpers
    # =========================================================
    def amsterdam_now(self):
        return datetime.now(AMSTERDAM_TZ)

    def format_amsterdam_time(self, dt_obj=None):
        if dt_obj is None:
            dt_obj = self.amsterdam_now()

        if isinstance(dt_obj, pd.Timestamp):
            dt_obj = dt_obj.to_pydatetime()

        if getattr(dt_obj, "tzinfo", None) is None:
            dt_obj = dt_obj.replace(tzinfo=AMSTERDAM_TZ)
        else:
            dt_obj = dt_obj.astimezone(AMSTERDAM_TZ)

        return dt_obj.strftime("%d %H:%M:%S")

    def format_backtest_time(self, dt_obj):
        if isinstance(dt_obj, pd.Timestamp):
            dt_obj = dt_obj.to_pydatetime()

        if getattr(dt_obj, "tzinfo", None) is None:
            dt_obj = dt_obj.replace(tzinfo=AMSTERDAM_TZ)
        else:
            dt_obj = dt_obj.astimezone(AMSTERDAM_TZ)

        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")

    def parse_ib_execution_time(self, exec_time_value):
        if exec_time_value is None:
            return self.amsterdam_now()

        if isinstance(exec_time_value, pd.Timestamp):
            dt_obj = exec_time_value.to_pydatetime()
        elif isinstance(exec_time_value, datetime):
            dt_obj = exec_time_value
        else:
            text = str(exec_time_value).strip()
            dt_obj = None

            for fmt in ("%Y%m%d  %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d-%H:%M:%S"):
                try:
                    dt_obj = datetime.strptime(text, fmt)
                    break
                except Exception:
                    pass

            if dt_obj is None:
                try:
                    dt_obj = pd.to_datetime(text, errors="coerce")
                    if pd.isna(dt_obj):
                        return self.amsterdam_now()
                    if isinstance(dt_obj, pd.Timestamp):
                        dt_obj = dt_obj.to_pydatetime()
                except Exception:
                    return self.amsterdam_now()

        if getattr(dt_obj, "tzinfo", None) is None:
            dt_obj = dt_obj.replace(tzinfo=AMSTERDAM_TZ)
        else:
            dt_obj = dt_obj.astimezone(AMSTERDAM_TZ)

        return dt_obj

    def normalize_execution_side(self, raw_side):
        side = str(raw_side or "").strip().upper()
        if side in ("BOT", "BUY"):
            return "Buy"
        if side in ("SLD", "SELL"):
            return "Sell"
        return side.title() if side else ""

    def ib_call(self, fn, *args, **kwargs):
        return self.ibw.call(fn, *args, **kwargs)

    def update_ib_message(self, message):
        self.ib_message_var.set(message)

    def format_price_value(self, value):
        if value is None:
            return "-"
        try:
            value = float(value)
            if math.isnan(value) or value <= 0:
                return "-"
            return f"{value:,.5f}"
        except Exception:
            return "-"

    def set_quote_values(self, bid=None, ask=None):
        self.bid_price = bid
        self.ask_price = ask
        self.bid_var.set(self.format_price_value(bid))
        self.ask_var.set(self.format_price_value(ask))

    def create_market_order(self, action, qty):
        order = MarketOrder(action, qty)
        order.tif = "DAY"
        order.outsideRth = False
        return order

    def refresh_transactions_table(self):
        for item in self.transactions_tree.get_children():
            self.transactions_tree.delete(item)

        for tx in self.transactions:
            self.transactions_tree.insert(
                "",
                "end",
                values=(
                    tx["time"],
                    tx["side"],
                    tx["instrument"],
                    tx["qty"],
                    tx["price"],
                    tx["commission"]
                )
            )

    def refresh_positions_table(self):
        for item in self.positions_tree.get_children():
            self.positions_tree.delete(item)

        for position in self.positions:
            self.positions_tree.insert(
                "",
                "end",
                values=(
                    position["instrument"],
                    f'{position["amount"]:,.4f}',
                    "Close"
                )
            )

    def refresh_backtest_table(self):
        for item in self.backtest_tree.get_children():
            self.backtest_tree.delete(item)

        for trade in self.backtest_results:
            self.backtest_tree.insert(
                "",
                "end",
                values=(
                    trade["entry_time"],
                    trade["exit_time"],
                    trade["side"],
                    trade["qty"],
                    trade["entry_price"],
                    trade["exit_price"],
                    trade["pnl"],
                    trade["reason"]
                )
            )

    def validate_amount(self):
        raw_amount = self.amount_var.get().strip()
        if raw_amount == "":
            self.update_ib_message("Please enter Amount (USD)")
            return None

        try:
            amount = float(raw_amount)
        except ValueError:
            self.update_ib_message("Invalid amount format. Use XXX.YY")
            return None

        if amount <= 0:
            self.update_ib_message("Amount must be greater than 0")
            return None

        return amount

    def validate_backtest_amount(self):
        raw_amount = self.backtest_amount_var.get().strip()
        if raw_amount == "":
            self.backtest_status_var.set("Please enter Amount (USD)")
            return None

        try:
            amount = float(raw_amount)
        except ValueError:
            self.backtest_status_var.set("Invalid Amount format")
            return None

        if amount <= 0:
            self.backtest_status_var.set("Amount must be greater than 0")
            return None

        return amount

    def parse_backtest_date(self, date_text):
        try:
            dt = datetime.strptime(date_text.strip(), "%Y-%m-%d")
            return dt
        except Exception:
            raise ValueError("Date format must be YYYY-MM-DD")

    def is_connected(self):
        try:
            return self.ib_connected and self.ibw.ib is not None and self.ibw.ib.isConnected()
        except Exception:
            return False

    def get_market_data_symbol(self, instrument_label):
        return self.instrument_market_map.get(instrument_label, instrument_label)

    def get_contract_for_instrument(self, instrument_label):
        cfg = self.instrument_contract_config[instrument_label]

        def _qualify():
            if cfg["type"] == "STK":
                contract = Stock(cfg["symbol"], "SMART", cfg["currency"])
            elif cfg["type"] == "FUT":
                contract = Future(
                    symbol=cfg["symbol"],
                    lastTradeDateOrContractMonth=cfg["expiry"],
                    exchange=cfg["exchange"],
                    currency=cfg["currency"],
                    includeExpired=True
                )
            else:
                raise ValueError(f"Unsupported instrument type: {cfg['type']}")

            qualified = self.ibw.ib.qualifyContracts(contract)
            if not qualified:
                raise RuntimeError(f"Could not qualify IB contract for {instrument_label}")
            return qualified[0]

        return self.ib_call(_qualify)

    def get_close_contract_for_position(self, position_contract):
        def _qualify():
            if position_contract.secType == "STK":
                contract = Stock(position_contract.symbol, "SMART", position_contract.currency or "USD")
            elif position_contract.secType == "FUT":
                expiry = str(position_contract.lastTradeDateOrContractMonth)[:8] or str(position_contract.lastTradeDateOrContractMonth)
                contract = Future(
                    symbol=position_contract.symbol,
                    lastTradeDateOrContractMonth=expiry,
                    exchange=position_contract.exchange,
                    currency=position_contract.currency or "USD",
                    includeExpired=True
                )
            else:
                contract = position_contract

            qualified = self.ibw.ib.qualifyContracts(contract)
            if qualified:
                return qualified[0]
            return contract

        return self.ib_call(_qualify)

    # =========================================================
    # Yahoo-only market data
    # =========================================================
    def get_yahoo_quote_snapshot(self, instrument_label):
        market_symbol = self.get_market_data_symbol(instrument_label)
        ticker = yf.Ticker(market_symbol)

        bid = None
        ask = None
        last = None

        try:
            fi = getattr(ticker, "fast_info", None)
            if fi:
                for key in ("lastPrice", "last_price", "regularMarketPrice"):
                    try:
                        val = fi.get(key, None)
                    except Exception:
                        val = None
                    if val is not None:
                        val = float(val)
                        if val > 0 and not math.isnan(val):
                            last = val
                            break
        except Exception:
            pass

        try:
            info = ticker.info or {}
            raw_bid = info.get("bid")
            raw_ask = info.get("ask")
            raw_last = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")

            if raw_bid is not None:
                raw_bid = float(raw_bid)
                if raw_bid > 0 and not math.isnan(raw_bid):
                    bid = raw_bid

            if raw_ask is not None:
                raw_ask = float(raw_ask)
                if raw_ask > 0 and not math.isnan(raw_ask):
                    ask = raw_ask

            if last is None and raw_last is not None:
                raw_last = float(raw_last)
                if raw_last > 0 and not math.isnan(raw_last):
                    last = raw_last
        except Exception:
            pass

        if last is None:
            try:
                hist = ticker.history(period="1d", interval="1m", auto_adjust=True)
                if hist is not None and not hist.empty:
                    close_series = hist["Close"].dropna()
                    if not close_series.empty:
                        last = float(close_series.iloc[-1])
            except Exception:
                pass

        if bid is None:
            bid = last
        if ask is None:
            ask = last

        if last is None and bid is not None:
            last = bid
        if last is None and ask is not None:
            last = ask

        if last is None:
            raise RuntimeError(f"Could not get Yahoo market data for {instrument_label}")

        return {"bid": bid, "ask": ask, "last": last}

    def get_last_price_for_contract(self, contract, instrument_label):
        snap = self.get_yahoo_quote_snapshot(instrument_label)
        return float(snap["last"])

    def get_bid_ask_for_contract(self, contract, instrument_label):
        snap = self.get_yahoo_quote_snapshot(instrument_label)
        return snap["bid"], snap["ask"]

    def get_order_quantity_from_usd(self, instrument_label, amount_usd):
        contract = self.get_contract_for_instrument(instrument_label)
        price = self.get_last_price_for_contract(contract, instrument_label)
        cfg = self.instrument_contract_config[instrument_label]

        if cfg["type"] == "STK":
            qty = int(amount_usd / price)
            if qty < 1:
                raise RuntimeError(f"Amount too small for 1 share of {instrument_label} at ~{price:.4f}")
            return contract, qty, price

        if cfg["type"] == "FUT":
            multiplier = float(cfg["multiplier"])
            contract_notional_usd = price * multiplier
            qty = int(amount_usd / contract_notional_usd)
            if qty < 1:
                raise RuntimeError(
                    f"Amount too small for 1 contract of {instrument_label}. "
                    f"Approx 1 contract notional is {contract_notional_usd:,.2f} USD"
                )
            return contract, qty, price

        raise RuntimeError("Unsupported instrument type")

    def get_backtest_quantity_from_usd(self, instrument_label, amount_usd, entry_price):
        cfg = self.instrument_contract_config[instrument_label]

        if cfg["type"] == "STK":
            qty = int(amount_usd / entry_price)
            if qty < 1:
                raise RuntimeError(
                    f"Backtest amount too small for 1 share of {instrument_label} at entry price {entry_price:.5f}"
                )
            return qty

        if cfg["type"] == "FUT":
            multiplier = float(cfg["multiplier"])
            contract_notional_usd = entry_price * multiplier
            qty = int(amount_usd / contract_notional_usd)
            if qty < 1:
                raise RuntimeError(
                    f"Backtest amount too small for 1 contract of {instrument_label}. "
                    f"Approx 1 contract notional is {contract_notional_usd:,.2f} USD"
                )
            return qty

        raise RuntimeError("Unsupported instrument type")

    def calculate_trade_pnl(self, instrument_label, side, entry_price, exit_price, qty):
        cfg = self.instrument_contract_config[instrument_label]
        multiplier = float(cfg.get("multiplier", 1.0))

        if side == "LONG":
            pnl = (exit_price - entry_price) * qty * multiplier
        else:
            pnl = (entry_price - exit_price) * qty * multiplier

        return pnl

    def get_ib_positions_snapshot(self):
        return self.ib_call(lambda: self.ibw.ib.positions())

    def get_ib_position_for_selected_instrument(self, instrument_label):
        selected_cfg = self.instrument_contract_config[instrument_label]
        positions = self.get_ib_positions_snapshot()

        for p in positions:
            c = p.contract

            if selected_cfg["type"] == "STK":
                if c.secType == "STK" and c.symbol == selected_cfg["symbol"]:
                    return p

            elif selected_cfg["type"] == "FUT":
                wanted_expiry = selected_cfg["expiry"]
                if (
                    c.secType == "FUT"
                    and c.symbol == selected_cfg["symbol"]
                    and str(c.lastTradeDateOrContractMonth).startswith(wanted_expiry)
                ):
                    return p

        return None

    def format_contract_for_display(self, contract):
        if contract.secType == "STK":
            return contract.symbol
        if contract.secType == "FUT":
            month = str(contract.lastTradeDateOrContractMonth)
            return f"{contract.symbol} {month} {contract.exchange}"
        return f"{contract.symbol} {contract.secType}"

    def close_position_by_display_instrument(self, display_instrument, reason_label="MANUAL CLOSE POSITION"):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return False

        try:
            positions = self.get_ib_positions_snapshot()

            target_position = None
            for p in positions:
                if float(p.position) == 0:
                    continue
                if self.format_contract_for_display(p.contract) == display_instrument:
                    target_position = p
                    break

            if target_position is None:
                self.update_ib_message(f"{reason_label}: position not found")
                return False

            signed_qty = float(target_position.position)
            if signed_qty == 0:
                self.update_ib_message(f"{reason_label}: no position to close")
                return False

            action = "SELL" if signed_qty > 0 else "BUY"
            qty = abs(int(round(signed_qty)))
            close_contract = self.get_close_contract_for_position(target_position.contract)

            def _close():
                order = self.create_market_order(action, qty)
                trade = self.ibw.ib.placeOrder(close_contract, order)
                self.ibw.ib.sleep(1.5)
                return trade

            trade = self.ib_call(_close)

            self.refresh_ib_state()
            self.refresh_ib_transactions()
            self.refresh_quote_display()

            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            self.update_ib_message(
                f"{reason_label}: {action} {qty} {display_instrument} to close | IB status: {status}"
            )
            return True

        except Exception as e:
            self.update_ib_message(f"Close position failed: {e}")
            return False

    def refresh_ib_transactions(self):
        if not self.is_connected():
            self.transactions = []
            self.refresh_transactions_table()
            return

        try:
            def _get_executions():
                filt = ExecutionFilter()
                return self.ibw.ib.reqExecutions(filt)

            fills = self.ib_call(_get_executions)

            fresh_transactions = []
            for fill in fills:
                execution = getattr(fill, "execution", None)
                contract = getattr(fill, "contract", None)
                commission_report = getattr(fill, "commissionReport", None)

                if execution is None or contract is None:
                    continue

                exec_time_dt = self.parse_ib_execution_time(getattr(execution, "time", None))
                exec_time_str = self.format_amsterdam_time(exec_time_dt)

                side = self.normalize_execution_side(getattr(execution, "side", ""))
                shares = float(getattr(execution, "shares", 0) or 0)
                price = float(getattr(execution, "price", 0) or 0)

                commission_value = getattr(commission_report, "commission", None)
                if commission_value is None:
                    commission_display = ""
                else:
                    try:
                        commission_value = float(commission_value)
                        if math.isnan(commission_value):
                            commission_display = ""
                        else:
                            commission_display = f"{commission_value:,.5f}"
                    except Exception:
                        commission_display = str(commission_value)

                if contract.secType == "STK":
                    instrument = contract.symbol
                elif contract.secType == "FUT":
                    instrument = f"{contract.symbol} {contract.lastTradeDateOrContractMonth} {contract.exchange}"
                else:
                    instrument = self.format_contract_for_display(contract)

                fresh_transactions.append({
                    "time": exec_time_str,
                    "sort_time": exec_time_dt,
                    "side": side,
                    "instrument": instrument,
                    "qty": f"{shares:,.4f}",
                    "price": f"{price:,.5f}",
                    "commission": commission_display
                })

            fresh_transactions.sort(key=lambda x: x["sort_time"], reverse=True)

            for row in fresh_transactions:
                row.pop("sort_time", None)

            self.transactions = fresh_transactions
            self.refresh_transactions_table()

        except Exception as e:
            self.update_ib_message(f"IB transaction refresh failed: {e}")

    # =========================================================
    # Quote refresh
    # =========================================================
    def schedule_next_quote_refresh(self):
        self.stop_quote_refresh()
        self.quote_after_id = self.root.after(self.quote_update_interval_ms, self.quote_refresh_cycle)

    def stop_quote_refresh(self):
        if self.quote_after_id is not None:
            self.root.after_cancel(self.quote_after_id)
            self.quote_after_id = None

    def quote_refresh_cycle(self):
        if self.is_connected():
            self.refresh_quote_display()
            self.schedule_next_quote_refresh()
        else:
            self.stop_quote_refresh()

    def refresh_quote_display(self):
        if not self.is_connected():
            self.set_quote_values(None, None)
            return

        if self.quote_loading:
            return

        instrument_label = self.instrument_var.get().strip()
        if not instrument_label:
            self.set_quote_values(None, None)
            return

        self.quote_loading = True

        thread = threading.Thread(
            target=self.fetch_quote_data_thread,
            args=(instrument_label,),
            daemon=True
        )
        thread.start()

    def fetch_quote_data_thread(self, instrument_label):
        try:
            contract = self.get_contract_for_instrument(instrument_label)
            bid, ask = self.get_bid_ask_for_contract(contract, instrument_label)
            self.root.after(0, lambda il=instrument_label, b=bid, a=ask: self.handle_quote_success(il, b, a))
        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda il=instrument_label, msg=err_msg: self.handle_quote_error(il, msg))

    def handle_quote_success(self, instrument_label, bid, ask):
        self.quote_loading = False

        if instrument_label != self.instrument_var.get().strip():
            return

        if not self.is_connected():
            self.set_quote_values(None, None)
            return

        self.set_quote_values(bid, ask)

    def handle_quote_error(self, instrument_label, message):
        self.quote_loading = False
        if instrument_label == self.instrument_var.get().strip():
            self.set_quote_values(None, None)
            self.update_ib_message(f"Quote refresh failed: {message}")

    # =========================================================
    # Connection / IB sync
    # =========================================================
    def connect_to_ib(self):
        try:
            host = "127.0.0.1"
            port = 7497 if self.account_type.get() == "Paper" else 7496
            client_id = 11 if self.account_type.get() == "Paper" else 12

            def _connect():
                if self.ibw.ib.isConnected():
                    self.ibw.ib.disconnect()
                self.ibw.ib.connect(host, port, clientId=client_id, readonly=False, timeout=8)
                return self.ibw.ib.managedAccounts()

            accounts = self.ib_call(_connect)

            self.ib_connected = True
            self.connected_account_type = self.account_type.get()
            self.current_account_id = accounts[0] if accounts else None

            self.update_ib_message(
                f"Connected to IB {self.connected_account_type}"
                + (f" | Account: {self.current_account_id}" if self.current_account_id else "")
                + " | Market data source: Yahoo"
            )
            self.refresh_ib_state()
            self.refresh_ib_transactions()
            self.refresh_quote_display()
            self.schedule_next_quote_refresh()
            self.refresh_chart()
            self.schedule_next_chart_refresh()
            self.update_ui()

        except Exception as e:
            self.ib_connected = False
            self.connected_account_type = None
            self.current_account_id = None
            self.buying_power_usd = 0.0
            self.cash_amount_usd = 0.0
            self.positions = []
            self.transactions = []
            self.stop_bot()
            self.stop_quote_refresh()
            self.set_quote_values(None, None)
            self.stop_chart_refresh()
            self.show_chart_placeholder("Connection required for chart")
            self.chart_status_var.set("Connection required for chart")
            self.update_ib_message(f"Connection failed: {e}")
            self.update_ui()
            self.refresh_positions_table()
            self.refresh_transactions_table()

    def disconnect_ib(self):
        try:
            if self.ibw.ib and self.ibw.ib.isConnected():
                self.ib_call(lambda: self.ibw.ib.disconnect())
        except Exception:
            pass

        self.ib_connected = False
        self.connected_account_type = None
        self.current_account_id = None
        self.buying_power_usd = 0.0
        self.cash_amount_usd = 0.0
        self.positions = []
        self.transactions = []
        self.stop_bot()
        self.stop_quote_refresh()
        self.set_quote_values(None, None)
        self.stop_chart_refresh()
        self.show_chart_placeholder("Connect first to load the chart")
        self.chart_status_var.set("Connect first to load the chart")
        self.update_ib_message("Disconnected from IB")
        self.update_ui()
        self.refresh_positions_table()
        self.refresh_transactions_table()

    def toggle_connection(self):
        if self.is_connected():
            self.disconnect_ib()
            return

        self.update_ib_message("Connecting to IB...")

        threading.Thread(
            target=self.connect_to_ib,
            daemon=True
        ).start()

    def refresh_ib_state(self):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return

        try:
            summary = self.ib_call(lambda: self.ibw.ib.accountSummary())

            buying_power = None
            total_cash_value = None

            for row in summary:
                if self.current_account_id and row.account != self.current_account_id:
                    continue

                if row.tag == "BuyingPower" and row.currency == "USD":
                    buying_power = float(row.value)
                elif row.tag == "TotalCashValue" and row.currency == "USD":
                    total_cash_value = float(row.value)

            self.buying_power_usd = buying_power if buying_power is not None else 0.0
            self.cash_amount_usd = total_cash_value if total_cash_value is not None else 0.0

            fresh_positions = []
            for p in self.get_ib_positions_snapshot():
                signed_position = float(p.position)
                if signed_position == 0:
                    continue

                fresh_positions.append({
                    "instrument": self.format_contract_for_display(p.contract),
                    "amount": signed_position
                })

            self.positions = fresh_positions
            self.update_ui()
            self.refresh_positions_table()
            self.refresh_ib_transactions()
            self.refresh_quote_display()

        except Exception as e:
            self.update_ib_message(f"IB refresh failed: {e}")

    # =========================================================
    # Order placement
    # =========================================================
    def place_market_order(self, instrument_label, action, amount_usd, reason_label):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return False

        try:
            contract, qty, est_price = self.get_order_quantity_from_usd(instrument_label, amount_usd)

            def _place():
                order = self.create_market_order(action, qty)
                trade = self.ibw.ib.placeOrder(contract, order)
                self.ibw.ib.sleep(1.5)
                return trade

            trade = self.ib_call(_place)

            self.refresh_ib_state()
            self.refresh_ib_transactions()
            self.refresh_quote_display()

            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            self.update_ib_message(
                f"{reason_label}: {action} {qty} {instrument_label} "
                f"(est px {est_price:.5f}) | IB status: {status}"
            )
            return True

        except Exception as e:
            self.update_ib_message(f"Order failed: {e}")
            return False

    def close_selected_instrument_position(self, instrument_label, reason_label):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return False

        try:
            p = self.get_ib_position_for_selected_instrument(instrument_label)
            if p is None or float(p.position) == 0:
                self.update_ib_message(f"{reason_label}: no existing position to close")
                return False

            signed_qty = float(p.position)
            action = "SELL" if signed_qty > 0 else "BUY"
            qty = abs(int(round(signed_qty)))
            close_contract = self.get_close_contract_for_position(p.contract)

            def _close():
                order = self.create_market_order(action, qty)
                trade = self.ibw.ib.placeOrder(close_contract, order)
                self.ibw.ib.sleep(1.5)
                return trade

            trade = self.ib_call(_close)

            self.refresh_ib_state()
            self.refresh_ib_transactions()
            self.refresh_quote_display()

            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            self.update_ib_message(f"{reason_label}: closed {instrument_label} | IB status: {status}")
            return True

        except Exception as e:
            self.update_ib_message(f"Close failed: {e}")
            return False

    def buy_instrument(self):
        amount = self.validate_amount()
        if amount is None:
            return
        self.place_market_order(self.instrument_var.get(), "BUY", amount, "MANUAL BUY")

    def sell_instrument(self):
        amount = self.validate_amount()
        if amount is None:
            return
        self.place_market_order(self.instrument_var.get(), "SELL", amount, "MANUAL SELL")

    # =========================================================
    # Positions table click action
    # =========================================================
    def on_positions_tree_motion(self, event):
        region = self.positions_tree.identify("region", event.x, event.y)
        row_id = self.positions_tree.identify_row(event.y)
        column_id = self.positions_tree.identify_column(event.x)

        if region == "cell" and row_id and column_id == "#3":
            self.positions_tree.configure(cursor="hand2")
        else:
            self.positions_tree.configure(cursor="")

    def on_positions_tree_click(self, event):
        region = self.positions_tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.positions_tree.identify_row(event.y)
        column_id = self.positions_tree.identify_column(event.x)

        if not row_id:
            return

        if column_id != "#3":
            return

        values = self.positions_tree.item(row_id, "values")
        if not values or len(values) < 1:
            return

        display_instrument = values[0]
        self.close_position_by_display_instrument(display_instrument, "MANUAL CLOSE POSITION")

    # =========================================================
    # Bot
    # =========================================================
    def update_ui(self):
        self.buying_power_var.set(f"{self.buying_power_usd:,.2f}")
        self.cash_amount_var.set(f"{self.cash_amount_usd:,.2f}")

        if self.is_connected():
            self.status_var.set("Connected")
            self.status_label.config(foreground="green")
            self.connect_button.config(text="Disconnect")
            self.account_combo.config(state="disabled")
        else:
            self.status_var.set("Disconnected")
            self.status_label.config(foreground="red")
            self.connect_button.config(text="Connect")
            self.account_combo.config(state="readonly")

        if self.bot_running:
            self.bot_status_var.set("Running")
            self.bot_status_label.config(foreground="green")
            self.bot_button.config(text="Stop Bot")
            self.tp_sl_combo.config(state="disabled")
        else:
            self.bot_status_var.set("Stopped")
            self.bot_status_label.config(foreground="red")
            self.bot_button.config(text="Start Bot")
            self.tp_sl_combo.config(state="readonly")

    def toggle_bot(self):
        if self.bot_running:
            self.stop_bot()
            return

        if not self.is_connected():
            self.update_ib_message("Connect to IB before starting the bot")
            return

        amount = self.validate_amount()
        if amount is None:
            return

        self.bot_running = True
        self.last_bot_signal_id = None
        self.update_ui()
        self.update_ib_message("Auto-trading bot started")
        self.refresh_chart()

    def stop_bot(self):
        self.bot_running = False
        self.update_ui()
        self.update_ib_message("Auto-trading bot stopped")

    def process_bot_logic(self, instrument_label, df):
        if not self.bot_running or len(df) < 3 or not self.is_connected():
            return

        amount_usd = self.validate_amount()
        if amount_usd is None:
            return

        try:
            ib_position = self.get_ib_position_for_selected_instrument(instrument_label)
            current_signed_pos = float(ib_position.position) if ib_position else 0.0
        except Exception:
            current_signed_pos = 0.0

        prev_row = df.iloc[-2]
        curr_row = df.iloc[-1]

        prev_close = float(prev_row["Close"])
        curr_close = float(curr_row["Close"])
        prev_lower = float(prev_row["Lower"])
        curr_lower = float(curr_row["Lower"])
        prev_upper = float(prev_row["Upper"])
        curr_upper = float(curr_row["Upper"])

        current_bar_time = str(df.index[-1])

        crossed_up_from_lower = (prev_close < prev_lower) and (curr_close >= curr_lower)
        crossed_down_from_upper = (prev_close > prev_upper) and (curr_close <= curr_upper)

        if self.tp_sl_var.get() == "Yes":
            last3 = df.tail(3)

            if current_signed_pos > 0:
                if (last3["Close"] < last3["Lower"]).all():
                    signal_id = f"TP_SL_LONG_EXIT_{current_bar_time}"
                    if signal_id != self.last_bot_signal_id:
                        self.last_bot_signal_id = signal_id
                        self.close_selected_instrument_position(instrument_label, "BOT TP/SL CLOSE")
                    return

            elif current_signed_pos < 0:
                if (last3["Close"] > last3["Upper"]).all():
                    signal_id = f"TP_SL_SHORT_EXIT_{current_bar_time}"
                    if signal_id != self.last_bot_signal_id:
                        self.last_bot_signal_id = signal_id
                        self.close_selected_instrument_position(instrument_label, "BOT TP/SL CLOSE")
                    return

        if crossed_up_from_lower:
            signal_id = f"LONG_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return
            self.last_bot_signal_id = signal_id

            if current_signed_pos < 0:
                self.close_selected_instrument_position(instrument_label, "BOT REVERSE CLOSE SHORT")
                self.refresh_ib_state()

            current_after_close = self.get_ib_position_for_selected_instrument(instrument_label)
            current_after_close_pos = float(current_after_close.position) if current_after_close else 0.0

            if current_after_close_pos <= 0:
                self.place_market_order(instrument_label, "BUY", amount_usd, "BOT OPEN LONG")

        elif crossed_down_from_upper:
            signal_id = f"SHORT_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return
            self.last_bot_signal_id = signal_id

            if current_signed_pos > 0:
                self.close_selected_instrument_position(instrument_label, "BOT REVERSE CLOSE LONG")
                self.refresh_ib_state()

            current_after_close = self.get_ib_position_for_selected_instrument(instrument_label)
            current_after_close_pos = float(current_after_close.position) if current_after_close else 0.0

            if current_after_close_pos >= 0:
                self.place_market_order(instrument_label, "SELL", amount_usd, "BOT OPEN SHORT")

    # =========================================================
    # Chart data
    # =========================================================
    def fetch_chart_dataframe_from_yahoo(self, instrument_label):
        market_symbol = self.get_market_data_symbol(instrument_label)
        ticker = yf.Ticker(market_symbol)
        df = ticker.history(period="1d", interval="1m", auto_adjust=True)

        if df is None or df.empty:
            raise RuntimeError("No market data returned from Yahoo")

        df = df.copy()
        df = df[["Close"]].dropna()

        if df.empty:
            raise RuntimeError("No closing price data available from Yahoo")

        idx = pd.to_datetime(df.index, errors="coerce")
        if getattr(idx, "tz", None) is None:
            idx = idx.tz_localize("UTC")
        df.index = idx.tz_convert(AMSTERDAM_TZ)

        df = df.tail(120)
        if df.empty:
            raise RuntimeError("Yahoo dataframe empty after tail(120)")

        return df

    def fetch_backtest_dataframe_from_yahoo(self, instrument_label, start_dt, end_dt):
        market_symbol = self.get_market_data_symbol(instrument_label)
        ticker = yf.Ticker(market_symbol)

        days_diff = (end_dt - start_dt).days
        if days_diff <= 60:
            interval = "30m"
        elif days_diff <= 730:
            interval = "1h"
        else:
            interval = "1d"

        end_plus_one = end_dt + timedelta(days=1)

        df = ticker.history(
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_plus_one.strftime("%Y-%m-%d"),
            interval=interval,
            auto_adjust=True
        )

        if df is None or df.empty:
            raise RuntimeError("No backtest market data returned from Yahoo")

        df = df.copy()
        df = df[["Close"]].dropna()

        if df.empty:
            raise RuntimeError("No closing price data available for backtest")

        idx = pd.to_datetime(df.index, errors="coerce")
        if getattr(idx, "tz", None) is None:
            idx = idx.tz_localize("UTC")
        df.index = idx.tz_convert(AMSTERDAM_TZ)

        start_ts = pd.Timestamp(start_dt).tz_localize(AMSTERDAM_TZ)
        end_ts = pd.Timestamp(end_plus_one).tz_localize(AMSTERDAM_TZ)

        df = df[(df.index >= start_ts) & (df.index <= end_ts)]

        if df.empty:
            raise RuntimeError("Backtest dataframe empty after date filter")

        return df

    def add_bollinger_columns(self, df):
        df = df.copy()
        df["Middle"] = df["Close"].rolling(window=20).mean()
        df["STD20"] = df["Close"].rolling(window=20).std()
        df["Upper"] = df["Middle"] + (2 * df["STD20"])
        df["Lower"] = df["Middle"] - (2 * df["STD20"])
        df = df.dropna()

        if df.empty:
            raise RuntimeError("Not enough data for Bollinger Bands")

        return df

    # =========================================================
    # Backtest
    # =========================================================
    def start_backtest(self):
        if self.backtest_loading:
            return

        amount_usd = self.validate_backtest_amount()
        if amount_usd is None:
            return

        try:
            start_dt = self.parse_backtest_date(self.backtest_start_var.get())
            end_dt = self.parse_backtest_date(self.backtest_end_var.get())
        except Exception as e:
            self.backtest_status_var.set(str(e))
            return

        if start_dt > end_dt:
            self.backtest_status_var.set("Test Start Date must be before Test End Date")
            return

        instrument_label = self.backtest_instrument_var.get().strip()
        if not instrument_label:
            self.backtest_status_var.set("Please select an instrument")
            return

        self.backtest_loading = True
        self.backtest_button.config(state="disabled")
        self.backtest_status_var.set("Running backtest...")
        self.backtest_chart_status_var.set("Running backtest and preparing graph...")

        thread = threading.Thread(
            target=self.run_backtest_thread,
            args=(instrument_label, start_dt, end_dt, amount_usd, self.backtest_tp_sl_var.get()),
            daemon=True
        )
        thread.start()

    def run_backtest_thread(self, instrument_label, start_dt, end_dt, amount_usd, tp_sl_value):
        try:
            raw_df = self.fetch_backtest_dataframe_from_yahoo(instrument_label, start_dt, end_dt)
            df = self.add_bollinger_columns(raw_df)
            results = self.simulate_bollinger_backtest(
                instrument_label=instrument_label,
                df=df,
                amount_usd=amount_usd,
                tp_sl_enabled=(tp_sl_value == "Yes")
            )
            self.root.after(0, lambda il=instrument_label, res=results, data=df: self.handle_backtest_success(il, res,
                                                                                                              data))
        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda msg=err_msg: self.handle_backtest_error(msg))

    def simulate_bollinger_backtest(self, instrument_label, df, amount_usd, tp_sl_enabled):
        current_position = None
        long_count = 0
        short_count = 0
        closed_trades = []
        signals = []

        for i in range(1, len(df)):
            prev_row = df.iloc[i - 1]
            curr_row = df.iloc[i]

            prev_close = float(prev_row["Close"])
            curr_close = float(curr_row["Close"])
            prev_lower = float(prev_row["Lower"])
            curr_lower = float(curr_row["Lower"])
            prev_upper = float(prev_row["Upper"])
            curr_upper = float(curr_row["Upper"])

            current_time = df.index[i]

            crossed_up_from_lower = (prev_close < prev_lower) and (curr_close >= curr_lower)
            crossed_down_from_upper = (prev_close > prev_upper) and (curr_close <= curr_upper)

            if tp_sl_enabled and current_position is not None and i >= 2:
                last3 = df.iloc[i - 2:i + 1]

                if current_position["side"] == "LONG":
                    if (last3["Close"] < last3["Lower"]).all():
                        exit_price = curr_close
                        pnl = self.calculate_trade_pnl(
                            instrument_label,
                            current_position["side"],
                            current_position["entry_price"],
                            exit_price,
                            current_position["qty"]
                        )

                        closed_trades.append({
                            "entry_time": self.format_backtest_time(current_position["entry_time"]),
                            "exit_time": self.format_backtest_time(current_time),
                            "side": current_position["side"],
                            "qty": f'{current_position["qty"]:,.4f}',
                            "entry_price": f'{current_position["entry_price"]:,.5f}',
                            "exit_price": f'{exit_price:,.5f}',
                            "pnl": f'{pnl:,.2f}',
                            "pnl_value": pnl,
                            "reason": "TP/SL CLOSE"
                        })

                        signals.append({
                            "time": current_time,
                            "price": exit_price,
                            "action": "SELL",   # long close = SELL
                            "kind": "CLOSE_LONG",
                            "reason": "TP/SL CLOSE"
                        })

                        current_position = None
                        continue

                elif current_position["side"] == "SHORT":
                    if (last3["Close"] > last3["Upper"]).all():
                        exit_price = curr_close
                        pnl = self.calculate_trade_pnl(
                            instrument_label,
                            current_position["side"],
                            current_position["entry_price"],
                            exit_price,
                            current_position["qty"]
                        )

                        closed_trades.append({
                            "entry_time": self.format_backtest_time(current_position["entry_time"]),
                            "exit_time": self.format_backtest_time(current_time),
                            "side": current_position["side"],
                            "qty": f'{current_position["qty"]:,.4f}',
                            "entry_price": f'{current_position["entry_price"]:,.5f}',
                            "exit_price": f'{exit_price:,.5f}',
                            "pnl": f'{pnl:,.2f}',
                            "pnl_value": pnl,
                            "reason": "TP/SL CLOSE"
                        })

                        signals.append({
                            "time": current_time,
                            "price": exit_price,
                            "action": "BUY",   # short close = BUY
                            "kind": "CLOSE_SHORT",
                            "reason": "TP/SL CLOSE"
                        })

                        current_position = None
                        continue

            if crossed_up_from_lower:
                if current_position is not None and current_position["side"] == "SHORT":
                    exit_price = curr_close
                    pnl = self.calculate_trade_pnl(
                        instrument_label,
                        current_position["side"],
                        current_position["entry_price"],
                        exit_price,
                        current_position["qty"]
                    )

                    closed_trades.append({
                        "entry_time": self.format_backtest_time(current_position["entry_time"]),
                        "exit_time": self.format_backtest_time(current_time),
                        "side": current_position["side"],
                        "qty": f'{current_position["qty"]:,.4f}',
                        "entry_price": f'{current_position["entry_price"]:,.5f}',
                        "exit_price": f'{exit_price:,.5f}',
                        "pnl": f'{pnl:,.2f}',
                        "pnl_value": pnl,
                        "reason": "REVERSE CLOSE SHORT"
                    })

                    signals.append({
                        "time": current_time,
                        "price": exit_price,
                        "action": "BUY",   # short close = BUY
                        "kind": "CLOSE_SHORT",
                        "reason": "REVERSE CLOSE SHORT"
                    })

                    current_position = None

                if current_position is None:
                    qty = self.get_backtest_quantity_from_usd(instrument_label, amount_usd, curr_close)
                    current_position = {
                        "side": "LONG",
                        "entry_time": current_time,
                        "entry_price": curr_close,
                        "qty": qty
                    }
                    long_count += 1

                    signals.append({
                        "time": current_time,
                        "price": curr_close,
                        "action": "BUY",   # long entry = BUY
                        "kind": "OPEN_LONG",
                        "reason": "LONG ENTRY"
                    })

            elif crossed_down_from_upper:
                if current_position is not None and current_position["side"] == "LONG":
                    exit_price = curr_close
                    pnl = self.calculate_trade_pnl(
                        instrument_label,
                        current_position["side"],
                        current_position["entry_price"],
                        exit_price,
                        current_position["qty"]
                    )

                    closed_trades.append({
                        "entry_time": self.format_backtest_time(current_position["entry_time"]),
                        "exit_time": self.format_backtest_time(current_time),
                        "side": current_position["side"],
                        "qty": f'{current_position["qty"]:,.4f}',
                        "entry_price": f'{current_position["entry_price"]:,.5f}',
                        "exit_price": f'{exit_price:,.5f}',
                        "pnl": f'{pnl:,.2f}',
                        "pnl_value": pnl,
                        "reason": "REVERSE CLOSE LONG"
                    })

                    signals.append({
                        "time": current_time,
                        "price": exit_price,
                        "action": "SELL",   # long close = SELL
                        "kind": "CLOSE_LONG",
                        "reason": "REVERSE CLOSE LONG"
                    })

                    current_position = None

                if current_position is None:
                    qty = self.get_backtest_quantity_from_usd(instrument_label, amount_usd, curr_close)
                    current_position = {
                        "side": "SHORT",
                        "entry_time": current_time,
                        "entry_price": curr_close,
                        "qty": qty
                    }
                    short_count += 1

                    signals.append({
                        "time": current_time,
                        "price": curr_close,
                        "action": "SELL",   # short entry = SELL
                        "kind": "OPEN_SHORT",
                        "reason": "SHORT ENTRY"
                    })

        if current_position is not None:
            final_row = df.iloc[-1]
            exit_price = float(final_row["Close"])
            exit_time = df.index[-1]
            pnl = self.calculate_trade_pnl(
                instrument_label,
                current_position["side"],
                current_position["entry_price"],
                exit_price,
                current_position["qty"]
            )

            closed_trades.append({
                "entry_time": self.format_backtest_time(current_position["entry_time"]),
                "exit_time": self.format_backtest_time(exit_time),
                "side": current_position["side"],
                "qty": f'{current_position["qty"]:,.4f}',
                "entry_price": f'{current_position["entry_price"]:,.5f}',
                "exit_price": f'{exit_price:,.5f}',
                "pnl": f'{pnl:,.2f}',
                "pnl_value": pnl,
                "reason": "END OF TEST"
            })

            signals.append({
                "time": exit_time,
                "price": exit_price,
                "action": "SELL" if current_position["side"] == "LONG" else "BUY",
                "kind": "CLOSE_LONG" if current_position["side"] == "LONG" else "CLOSE_SHORT",
                "reason": "END OF TEST"
            })

        total_pnl = sum(trade["pnl_value"] for trade in closed_trades)

        for trade in closed_trades:
            trade.pop("pnl_value", None)

        return {
            "long_count": long_count,
            "short_count": short_count,
            "closed_trades_count": len(closed_trades),
            "total_pnl": total_pnl,
            "trades": closed_trades,
            "signals": signals
        }

    def handle_backtest_success(self, instrument_label, results, df):
        self.backtest_loading = False
        self.backtest_button.config(state="normal")

        self.backtest_results = results["trades"]
        self.backtest_plot_df = df.copy()
        self.backtest_plot_signals = results["signals"]

        self.backtest_long_count_var.set(str(results["long_count"]))
        self.backtest_short_count_var.set(str(results["short_count"]))
        self.backtest_total_trades_var.set(str(results["closed_trades_count"]))
        self.backtest_total_pnl_var.set(f'{results["total_pnl"]:,.2f}')

        if results["total_pnl"] >= 0:
            self.backtest_total_pnl_label.config(foreground="green")
        else:
            self.backtest_total_pnl_label.config(foreground="red")

        self.refresh_backtest_table()
        self.draw_backtest_chart(instrument_label, self.backtest_plot_df, self.backtest_plot_signals)

        self.backtest_status_var.set(f"Backtest finished for {instrument_label}")
        self.backtest_chart_status_var.set(
            f"{instrument_label} graph ready | Long Opens: {results['long_count']} | Short Opens: {results['short_count']}"
        )

    def handle_backtest_error(self, message):
        self.backtest_loading = False
        self.backtest_button.config(state="normal")
        self.backtest_status_var.set(f"Backtest failed: {message}")
        self.backtest_chart_status_var.set("Backtest graph could not be created")
        self.show_backtest_chart_placeholder(f"Backtest error:\n{message}")

    # =========================================================
    # Backtest graph
    # =========================================================
    def show_backtest_chart_placeholder(self, message):
        self.backtest_ax.clear()
        self.backtest_ax.set_title("Back Test Price Graph")
        self.backtest_ax.text(
            0.5, 0.5, message,
            ha="center", va="center",
            transform=self.backtest_ax.transAxes,
            fontsize=12
        )
        self.backtest_ax.set_xticks([])
        self.backtest_ax.set_yticks([])
        self.backtest_figure.tight_layout()
        self.backtest_canvas.draw()

    def draw_backtest_chart(self, instrument_label, df, signals):
        if df is None or df.empty:
            self.show_backtest_chart_placeholder("No backtest chart data")
            return

        self.backtest_ax.clear()

        self.backtest_ax.plot(df.index, df["Close"], label="Close")
        self.backtest_ax.plot(df.index, df["Upper"], label="Upper Band")
        self.backtest_ax.plot(df.index, df["Lower"], label="Lower Band")
        self.backtest_ax.fill_between(df.index, df["Lower"].values, df["Upper"].values, alpha=0.15)

        buy_open_times = [s["time"] for s in signals if s["kind"] == "OPEN_LONG"]
        buy_open_prices = [s["price"] for s in signals if s["kind"] == "OPEN_LONG"]

        sell_open_times = [s["time"] for s in signals if s["kind"] == "OPEN_SHORT"]
        sell_open_prices = [s["price"] for s in signals if s["kind"] == "OPEN_SHORT"]

        buy_close_times = [s["time"] for s in signals if s["kind"] == "CLOSE_SHORT"]
        buy_close_prices = [s["price"] for s in signals if s["kind"] == "CLOSE_SHORT"]

        sell_close_times = [s["time"] for s in signals if s["kind"] == "CLOSE_LONG"]
        sell_close_prices = [s["price"] for s in signals if s["kind"] == "CLOSE_LONG"]

        if buy_open_times:
            self.backtest_ax.scatter(
                buy_open_times, buy_open_prices,
                marker="^", s=75, label="Buy Open Long"
            )

        if sell_open_times:
            self.backtest_ax.scatter(
                sell_open_times, sell_open_prices,
                marker="v", s=75, label="Sell Open Short"
            )

        if buy_close_times:
            self.backtest_ax.scatter(
                buy_close_times, buy_close_prices,
                marker="o", s=55, label="Buy Close Short"
            )

        if sell_close_times:
            self.backtest_ax.scatter(
                sell_close_times, sell_close_prices,
                marker="o", s=55, label="Sell Close Long"
            )

        self.backtest_ax.set_title(f"{instrument_label} - Back Test Signals", fontsize=13)
        self.backtest_ax.set_xlabel("Time (Amsterdam)")
        self.backtest_ax.set_ylabel("Price")
        self.backtest_ax.grid(True, alpha=0.3)
        self.backtest_ax.legend(loc="upper left")
        self.backtest_ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M", tz=AMSTERDAM_TZ))

        self.backtest_figure.autofmt_xdate()
        self.backtest_figure.tight_layout()
        self.backtest_canvas.draw()

    # =========================================================
    # Chart
    # =========================================================
    def on_instrument_change(self, event=None):
        if self.is_connected():
            self.refresh_quote_display()
            self.refresh_chart()

    def schedule_next_chart_refresh(self):
        self.stop_chart_refresh()
        self.chart_after_id = self.root.after(self.chart_update_interval_ms, self.chart_refresh_cycle)

    def stop_chart_refresh(self):
        if self.chart_after_id is not None:
            self.root.after_cancel(self.chart_after_id)
            self.chart_after_id = None

    def chart_refresh_cycle(self):
        if self.is_connected():
            self.refresh_chart()
            self.schedule_next_chart_refresh()
        else:
            self.stop_chart_refresh()

    def refresh_chart(self):
        if not self.is_connected():
            self.chart_status_var.set("Connect first to load the chart")
            self.show_chart_placeholder("Connect first to load the chart")
            return

        if self.chart_loading:
            return

        instrument_label = self.instrument_var.get().strip()
        if not instrument_label:
            return

        self.chart_loading = True
        self.chart_status_var.set(f"Loading chart for {instrument_label}...")

        thread = threading.Thread(
            target=self.fetch_chart_data_thread,
            args=(instrument_label,),
            daemon=True
        )
        thread.start()

    def fetch_chart_data_thread(self, instrument_label):
        try:
            raw_df = self.fetch_chart_dataframe_from_yahoo(instrument_label)
            df = self.add_bollinger_columns(raw_df)
            self.root.after(0, lambda il=instrument_label, data=df: self.draw_chart(il, data))
        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda il=instrument_label, msg=err_msg: self.handle_chart_error(il, msg))

    def handle_chart_error(self, instrument_label, message):
        self.chart_loading = False
        self.ax.clear()
        self.ax.set_title(f"{instrument_label} - Price with Bollinger Bands")
        self.ax.text(
            0.5, 0.5, f"Chart error:\n{message}",
            ha="center", va="center", transform=self.ax.transAxes, fontsize=11
        )
        self.ax.set_xlabel("Time")
        self.ax.set_ylabel("Price")
        self.figure.tight_layout()
        self.canvas.draw()
        self.chart_status_var.set(f"{instrument_label}: chart update failed")

    def show_chart_placeholder(self, message):
        self.ax.clear()
        self.ax.set_title("Price Chart with Bollinger Bands")
        self.ax.text(
            0.5, 0.5, message,
            ha="center", va="center", transform=self.ax.transAxes, fontsize=12
        )
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.figure.tight_layout()
        self.canvas.draw()

    def draw_chart(self, instrument_label, df):
        self.chart_loading = False

        if instrument_label != self.instrument_var.get().strip():
            return

        if not self.is_connected():
            self.show_chart_placeholder("Connect first to load the chart")
            return

        self.ax.clear()
        self.ax.plot(df.index, df["Close"], label="Close")
        self.ax.plot(df.index, df["Upper"], label="Upper Band")
        self.ax.plot(df.index, df["Lower"], label="Lower Band")
        self.ax.fill_between(df.index, df["Lower"].values, df["Upper"].values, alpha=0.15)

        last_price = float(df["Close"].iloc[-1])

        self.ax.set_title(f"{instrument_label} - Recent Price with Bollinger Bands", fontsize=14)
        self.ax.set_xlabel("Time (Amsterdam)")
        self.ax.set_ylabel("Price")
        self.ax.legend(loc="upper left")
        self.ax.grid(True, alpha=0.3)
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %H:%M:%S", tz=AMSTERDAM_TZ))

        self.figure.autofmt_xdate()
        self.figure.tight_layout()
        self.canvas.draw()

        update_time = self.format_amsterdam_time()
        bot_text = " | Bot Running" if self.bot_running else ""
        self.chart_status_var.set(
            f"{instrument_label} updated at {update_time} | Last Price: {last_price:.5f}{bot_text}"
        )

        self.process_bot_logic(instrument_label, df)

    # =========================================================
    # Close
    # =========================================================
    def on_close(self):
        self.stop_quote_refresh()
        self.stop_chart_refresh()
        self.stop_bot()
        self.disconnect_ib()
        self.ibw.stop()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = ConnectionApp(root)
    root.mainloop()
