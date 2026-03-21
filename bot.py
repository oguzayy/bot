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
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from ib_insync import IB, Stock, Future, MarketOrder, ExecutionFilter, util


AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")


class IBWorker:
    """
    Runs all IB operations on one dedicated thread with one persistent event loop.
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
                item = self.task_queue.get(timeout=0.05)
            except queue.Empty:
                try:
                    if self.ib and self.ib.isConnected():
                        self.ib.sleep(0.05)
                except Exception:
                    pass
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
        self.root.geometry("1720x1080")

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

        # ---------------- Streaming quote state ----------------
        self.bid_price = None
        self.ask_price = None
        self.last_price = None

        self.current_quote_contract = None
        self.current_quote_ticker = None
        self.current_quote_instrument_label = None

        self.current_market_data_type = 1   # 1 = Live
        self.market_data_available = False
        self.live_market_data_active = False

        # ---------------- Real-time bars state ----------------
        self.current_rt_bars = []
        self.rt_bars_lock = threading.Lock()

        # ---------------- Chart state ----------------
        self.chart_update_interval_ms = 15000
        self.chart_after_id = None
        self.chart_loading = False

        # ---------------- Bot state ----------------
        self.bot_running = False
        self.last_bot_signal_id = None
        self.bot_action_lock = threading.Lock()
        self.bot_action_in_progress = False

        # Live strategy position tracking
        self.live_strategy_positions = {}  # instrument_label -> {"side","entry_price","qty","entry_time"}

        # ---------------- Backtest state ----------------
        self.backtest_loading = False
        self.backtest_results = []
        self.backtest_plot_df = None
        self.backtest_plot_signals = []

        # ---------------- TP/SL config ----------------
        self.generic_take_profit_pct = 0.02
        self.generic_stop_loss_pct = 0.01

        # ---------------- Instrument config ----------------
        self.instrument_market_map = {
            "NVDA": "NVDA",
            "AAPL": "AAPL",
            "MSFT": "MSFT",
            "TSLA": "TSLA",
            "AMZN": "AMZN",
            "META": "META",
            "GOOGL": "GOOGL",
            "AMD": "AMD",
            "NFLX": "NFLX",
            "SPY": "SPY",
            "QQQ": "QQQ",
            "JPM": "JPM",
            "XOM": "XOM",
            "EUR Jun26 CME": "EUR"
        }

        self.instrument_contract_config = {
            "NVDA": {"type": "STK", "symbol": "NVDA", "exchange": "SMART", "currency": "USD"},
            "AAPL": {"type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
            "MSFT": {"type": "STK", "symbol": "MSFT", "exchange": "SMART", "currency": "USD"},
            "TSLA": {"type": "STK", "symbol": "TSLA", "exchange": "SMART", "currency": "USD"},
            "AMZN": {"type": "STK", "symbol": "AMZN", "exchange": "SMART", "currency": "USD"},
            "META": {"type": "STK", "symbol": "META", "exchange": "SMART", "currency": "USD"},
            "GOOGL": {"type": "STK", "symbol": "GOOGL", "exchange": "SMART", "currency": "USD"},
            "AMD": {"type": "STK", "symbol": "AMD", "exchange": "SMART", "currency": "USD"},
            "NFLX": {"type": "STK", "symbol": "NFLX", "exchange": "SMART", "currency": "USD"},
            "SPY": {"type": "STK", "symbol": "SPY", "exchange": "SMART", "currency": "USD"},
            "QQQ": {"type": "STK", "symbol": "QQQ", "exchange": "SMART", "currency": "USD"},
            "JPM": {"type": "STK", "symbol": "JPM", "exchange": "SMART", "currency": "USD"},
            "XOM": {"type": "STK", "symbol": "XOM", "exchange": "SMART", "currency": "USD"},
            "EUR Jun26 CME": {
                "type": "FUT",
                "symbol": "EUR",
                "exchange": "CME",
                "currency": "USD",
                "expiry": "202606",
                "multiplier": 125000
            }
        }

        self.strategy_options = [
            "Bollinger Bands",
            "SMA Crossover",
            "EMA Crossover",
            "RSI Reversion",
            "MACD Crossover",
            "Donchian Breakout"
        ]

        # ---------------- Main container ----------------
        main_frame = ttk.Frame(root, padding=10)
        main_frame.pack(fill="both", expand=True)

        # ---------------- Row 1 ----------------
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

        # ---------------- Tabs ----------------
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

        self.instrument_var = tk.StringVar(value="SPY")
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
        self.sell_button.grid(row=0, column=9, padx=(0, 10), pady=5, sticky="w")

        ttk.Label(row3, text="Strategy:").grid(row=0, column=10, padx=(0, 6), pady=5, sticky="w")

        self.trade_strategy_var = tk.StringVar(value="Bollinger Bands")
        self.trade_strategy_combo = ttk.Combobox(
            row3,
            textvariable=self.trade_strategy_var,
            values=self.strategy_options,
            state="readonly",
            width=20
        )
        self.trade_strategy_combo.grid(row=0, column=11, padx=(0, 16), pady=5, sticky="w")
        self.trade_strategy_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_chart())

        ttk.Label(row3, text="TP/SL:").grid(row=0, column=12, padx=(0, 6), pady=5, sticky="w")

        self.tp_sl_var = tk.StringVar(value="Yes")
        self.tp_sl_combo = ttk.Combobox(
            row3,
            textvariable=self.tp_sl_var,
            values=["Yes", "No"],
            state="readonly",
            width=8
        )
        self.tp_sl_combo.grid(row=0, column=13, padx=(0, 16), pady=5, sticky="w")

        self.bot_button = ttk.Button(
            row3,
            text="Start Bot",
            command=self.toggle_bot,
            width=12
        )
        self.bot_button.grid(row=0, column=14, padx=(0, 8), pady=5, sticky="w")

        self.refresh_ib_button = ttk.Button(
            row3,
            text="Refresh IB",
            command=self.refresh_ib_state,
            width=12
        )
        self.refresh_ib_button.grid(row=0, column=15, padx=(0, 8), pady=5, sticky="w")

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

        chart_group = ttk.LabelFrame(self.trade_tab, text="IB Strategy Chart", padding=10)
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

        self.backtest_instrument_var = tk.StringVar(value="SPY")
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
            values=self.strategy_options,
            state="readonly",
            width=20
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
        self.backtest_total_gain_var = tk.StringVar(value="0.00")
        self.backtest_total_loss_var = tk.StringVar(value="0.00")
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

        ttk.Label(bt_summary, text="Total Gain (USD):").grid(row=1, column=0, padx=(0, 6), pady=5, sticky="w")
        self.backtest_total_gain_label = ttk.Label(
            bt_summary,
            textvariable=self.backtest_total_gain_var,
            font=("Arial", 10, "bold"),
            foreground="green"
        )
        self.backtest_total_gain_label.grid(row=1, column=1, padx=(0, 20), pady=5, sticky="w")

        ttk.Label(bt_summary, text="Total Loss (USD):").grid(row=1, column=2, padx=(0, 6), pady=5, sticky="w")
        self.backtest_total_loss_label = ttk.Label(
            bt_summary,
            textvariable=self.backtest_total_loss_var,
            font=("Arial", 10, "bold"),
            foreground="red"
        )
        self.backtest_total_loss_label.grid(row=1, column=3, padx=(0, 20), pady=5, sticky="w")

        ttk.Label(bt_summary, text="Net P/L (USD):").grid(row=1, column=4, padx=(0, 6), pady=5, sticky="w")
        self.backtest_total_pnl_label = ttk.Label(
            bt_summary,
            textvariable=self.backtest_total_pnl_var,
            font=("Arial", 10, "bold")
        )
        self.backtest_total_pnl_label.grid(row=1, column=5, padx=(0, 20), pady=5, sticky="w")

        bt_chart_group = ttk.LabelFrame(self.backtest_tab, text="Back Test IB Graph", padding=10)
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

    def update_ib_message_safe(self, message):
        self.root.after(0, lambda m=message: self.update_ib_message(m))

    def safe_float(self, value):
        try:
            if value is None:
                return None
            value = float(value)
            if math.isnan(value):
                return None
            return value
        except Exception:
            return None

    def format_price_value(self, value):
        value = self.safe_float(value)
        if value is None or value <= 0:
            return "-"
        return f"{value:,.5f}"

    def set_quote_values(self, bid=None, ask=None, last=None):
        self.bid_price = bid
        self.ask_price = ask
        self.last_price = last
        self.bid_var.set(self.format_price_value(bid))
        self.ask_var.set(self.format_price_value(ask))

    def create_market_order(self, action, qty):
        order = MarketOrder(action, qty)
        order.tif = "DAY"
        order.outsideRth = False
        return order

    def set_market_data_type(self, live=True):
        if not self.is_connected():
            return

        try:
            md_type = 1 if live else 3
            self.ib_call(lambda: self.ibw.ib.reqMarketDataType(md_type))
            self.current_market_data_type = md_type
        except Exception as e:
            self.update_ib_message_safe(f"Could not set market data type: {e}")

    def set_bot_action_in_progress(self, value):
        with self.bot_action_lock:
            self.bot_action_in_progress = value

    def is_bot_action_in_progress(self):
        with self.bot_action_lock:
            return self.bot_action_in_progress

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
                    currency=cfg["currency"]
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
                    currency=position_contract.currency or "USD"
                )
            else:
                contract = position_contract

            qualified = self.ibw.ib.qualifyContracts(contract)
            if qualified:
                return qualified[0]
            return contract

        return self.ib_call(_qualify)

    def wait_until(self, condition_fn, timeout_sec=8.0, sleep_sec=0.2):
        start = datetime.now()
        while (datetime.now() - start).total_seconds() < timeout_sec:
            try:
                if condition_fn():
                    return True
            except Exception:
                pass
            threading.Event().wait(sleep_sec)
        return False

    # =========================================================
    # Strategy helpers
    # =========================================================
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

    def add_sma_columns(self, df):
        df = df.copy()
        df["SMA_FAST"] = df["Close"].rolling(window=10).mean()
        df["SMA_SLOW"] = df["Close"].rolling(window=30).mean()
        df = df.dropna()
        if df.empty:
            raise RuntimeError("Not enough data for SMA Crossover")
        return df

    def add_ema_columns(self, df):
        df = df.copy()
        df["EMA_FAST"] = df["Close"].ewm(span=12, adjust=False).mean()
        df["EMA_SLOW"] = df["Close"].ewm(span=26, adjust=False).mean()
        df = df.dropna()
        if df.empty:
            raise RuntimeError("Not enough data for EMA Crossover")
        return df

    def add_rsi_columns(self, df):
        df = df.copy()
        delta = df["Close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)

        df["RSI"] = 100 - (100 / (1 + rs))
        df["RSI"] = df["RSI"].fillna(100)
        df = df.dropna()
        if df.empty:
            raise RuntimeError("Not enough data for RSI Reversion")
        return df

    def add_macd_columns(self, df):
        df = df.copy()
        ema12 = df["Close"].ewm(span=12, adjust=False).mean()
        ema26 = df["Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
        df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]
        df = df.dropna()
        if df.empty:
            raise RuntimeError("Not enough data for MACD Crossover")
        return df

    def add_donchian_columns(self, df):
        df = df.copy()
        df["DONCHIAN_HIGH"] = df["Close"].shift(1).rolling(window=20).max()
        df["DONCHIAN_LOW"] = df["Close"].shift(1).rolling(window=20).min()
        df["DONCHIAN_MID"] = (df["DONCHIAN_HIGH"] + df["DONCHIAN_LOW"]) / 2
        df = df.dropna()
        if df.empty:
            raise RuntimeError("Not enough data for Donchian Breakout")
        return df

    def add_strategy_columns(self, df, strategy_name):
        if strategy_name == "Bollinger Bands":
            return self.add_bollinger_columns(df)
        if strategy_name == "SMA Crossover":
            return self.add_sma_columns(df)
        if strategy_name == "EMA Crossover":
            return self.add_ema_columns(df)
        if strategy_name == "RSI Reversion":
            return self.add_rsi_columns(df)
        if strategy_name == "MACD Crossover":
            return self.add_macd_columns(df)
        if strategy_name == "Donchian Breakout":
            return self.add_donchian_columns(df)

        raise RuntimeError(f"Unsupported strategy: {strategy_name}")

    def get_strategy_signal(self, strategy_name, prev_row, curr_row):
        if strategy_name == "Bollinger Bands":
            prev_close = float(prev_row["Close"])
            curr_close = float(curr_row["Close"])
            prev_lower = float(prev_row["Lower"])
            curr_lower = float(curr_row["Lower"])
            prev_upper = float(prev_row["Upper"])
            curr_upper = float(curr_row["Upper"])

            if (prev_close < prev_lower) and (curr_close >= curr_lower):
                return "LONG"
            if (prev_close > prev_upper) and (curr_close <= curr_upper):
                return "SHORT"
            return None

        if strategy_name == "SMA Crossover":
            prev_fast = float(prev_row["SMA_FAST"])
            prev_slow = float(prev_row["SMA_SLOW"])
            curr_fast = float(curr_row["SMA_FAST"])
            curr_slow = float(curr_row["SMA_SLOW"])

            if prev_fast <= prev_slow and curr_fast > curr_slow:
                return "LONG"
            if prev_fast >= prev_slow and curr_fast < curr_slow:
                return "SHORT"
            return None

        if strategy_name == "EMA Crossover":
            prev_fast = float(prev_row["EMA_FAST"])
            prev_slow = float(prev_row["EMA_SLOW"])
            curr_fast = float(curr_row["EMA_FAST"])
            curr_slow = float(curr_row["EMA_SLOW"])

            if prev_fast <= prev_slow and curr_fast > curr_slow:
                return "LONG"
            if prev_fast >= prev_slow and curr_fast < curr_slow:
                return "SHORT"
            return None

        if strategy_name == "RSI Reversion":
            prev_rsi = float(prev_row["RSI"])
            curr_rsi = float(curr_row["RSI"])

            if prev_rsi < 30 and curr_rsi >= 30:
                return "LONG"
            if prev_rsi > 70 and curr_rsi <= 70:
                return "SHORT"
            return None

        if strategy_name == "MACD Crossover":
            prev_macd = float(prev_row["MACD"])
            prev_sig = float(prev_row["MACD_SIGNAL"])
            curr_macd = float(curr_row["MACD"])
            curr_sig = float(curr_row["MACD_SIGNAL"])

            if prev_macd <= prev_sig and curr_macd > curr_sig:
                return "LONG"
            if prev_macd >= prev_sig and curr_macd < curr_sig:
                return "SHORT"
            return None

        if strategy_name == "Donchian Breakout":
            prev_close = float(prev_row["Close"])
            curr_close = float(curr_row["Close"])
            prev_high = float(prev_row["DONCHIAN_HIGH"])
            prev_low = float(prev_row["DONCHIAN_LOW"])
            curr_high = float(curr_row["DONCHIAN_HIGH"])
            curr_low = float(curr_row["DONCHIAN_LOW"])

            if prev_close <= prev_high and curr_close > curr_high:
                return "LONG"
            if prev_close >= prev_low and curr_close < curr_low:
                return "SHORT"
            return None

        return None

    def should_exit_on_tp_sl(self, strategy_name, current_position, df, idx_i):
        if current_position is None:
            return False, None

        curr_row = df.iloc[idx_i]
        curr_close = float(curr_row["Close"])
        side = current_position["side"]
        entry_price = float(current_position["entry_price"])

        if strategy_name == "Bollinger Bands":
            if idx_i >= 2:
                last3 = df.iloc[idx_i - 2:idx_i + 1]
                if side == "LONG" and (last3["Close"] < last3["Lower"]).all():
                    return True, "TP/SL CLOSE"
                if side == "SHORT" and (last3["Close"] > last3["Upper"]).all():
                    return True, "TP/SL CLOSE"
            return False, None

        if side == "LONG":
            tp_price = entry_price * (1 + self.generic_take_profit_pct)
            sl_price = entry_price * (1 - self.generic_stop_loss_pct)

            if curr_close >= tp_price:
                return True, "TAKE PROFIT"
            if curr_close <= sl_price:
                return True, "STOP LOSS"

        elif side == "SHORT":
            tp_price = entry_price * (1 - self.generic_take_profit_pct)
            sl_price = entry_price * (1 + self.generic_stop_loss_pct)

            if curr_close <= tp_price:
                return True, "TAKE PROFIT"
            if curr_close >= sl_price:
                return True, "STOP LOSS"

        return False, None

    def draw_strategy_overlay(self, ax_obj, df, strategy_name):
        if strategy_name == "Bollinger Bands":
            ax_obj.plot(df.index, df["Upper"], label="Upper Band")
            ax_obj.plot(df.index, df["Lower"], label="Lower Band")
            ax_obj.fill_between(df.index, df["Lower"].values, df["Upper"].values, alpha=0.15)

        elif strategy_name == "SMA Crossover":
            ax_obj.plot(df.index, df["SMA_FAST"], label="SMA Fast (10)")
            ax_obj.plot(df.index, df["SMA_SLOW"], label="SMA Slow (30)")

        elif strategy_name == "EMA Crossover":
            ax_obj.plot(df.index, df["EMA_FAST"], label="EMA Fast (12)")
            ax_obj.plot(df.index, df["EMA_SLOW"], label="EMA Slow (26)")

        elif strategy_name == "Donchian Breakout":
            ax_obj.plot(df.index, df["DONCHIAN_HIGH"], label="Donchian High")
            ax_obj.plot(df.index, df["DONCHIAN_LOW"], label="Donchian Low")
            ax_obj.fill_between(df.index, df["DONCHIAN_LOW"].values, df["DONCHIAN_HIGH"].values, alpha=0.15)

    def build_strategy_status_suffix(self, df, strategy_name):
        try:
            last_row = df.iloc[-1]

            if strategy_name == "RSI Reversion":
                return f" | RSI: {float(last_row['RSI']):.2f}"

            if strategy_name == "MACD Crossover":
                return f" | MACD: {float(last_row['MACD']):.4f} | Signal: {float(last_row['MACD_SIGNAL']):.4f}"

            if strategy_name == "SMA Crossover":
                return f" | SMA10: {float(last_row['SMA_FAST']):.4f} | SMA30: {float(last_row['SMA_SLOW']):.4f}"

            if strategy_name == "EMA Crossover":
                return f" | EMA12: {float(last_row['EMA_FAST']):.4f} | EMA26: {float(last_row['EMA_SLOW']):.4f}"

            if strategy_name == "Donchian Breakout":
                return f" | Donchian High: {float(last_row['DONCHIAN_HIGH']):.4f} | Donchian Low: {float(last_row['DONCHIAN_LOW']):.4f}"
        except Exception:
            pass

        return ""

    # =========================================================
    # IB streaming / subscriptions
    # =========================================================
    def cancel_market_data_subscription(self):
        try:
            if self.current_quote_ticker is not None:
                try:
                    self.current_quote_ticker.updateEvent -= self.on_ib_ticker_update
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if self.is_connected() and self.current_quote_contract is not None and self.market_data_available:
                self.ib_call(lambda: self.ibw.ib.cancelMktData(self.current_quote_contract))
        except Exception:
            pass

        self.current_quote_contract = None
        self.current_quote_ticker = None
        self.current_quote_instrument_label = None
        self.market_data_available = False
        self.live_market_data_active = False

    def reset_rt_bars(self):
        with self.rt_bars_lock:
            self.current_rt_bars = []

    def subscribe_market_data_for_selected_instrument(self):
        if not self.is_connected():
            self.set_quote_values(None, None, None)
            return

        instrument_label = self.instrument_var.get().strip()
        if not instrument_label:
            self.set_quote_values(None, None, None)
            return

        def _worker():
            self.cancel_market_data_subscription()
            self.market_data_available = False
            self.live_market_data_active = False
            self.reset_rt_bars()

            try:
                contract = self.get_contract_for_instrument(instrument_label)

                # First try LIVE
                self.set_market_data_type(live=True)

                def _subscribe_live():
                    ticker = self.ibw.ib.reqMktData(contract, "", False, False)
                    self.ibw.ib.sleep(2.0)
                    return ticker

                ticker = self.ib_call(_subscribe_live)

                self.current_quote_contract = contract
                self.current_quote_ticker = ticker
                self.current_quote_instrument_label = instrument_label

                try:
                    ticker.updateEvent += self.on_ib_ticker_update
                except Exception:
                    pass

                bid = self.safe_float(getattr(ticker, "bid", None))
                ask = self.safe_float(getattr(ticker, "ask", None))
                last = self.safe_float(getattr(ticker, "last", None))
                market_data_type = getattr(ticker, "marketDataType", None)

                live_ok = (
                    market_data_type == 1 and
                    ((last is not None and last > 0) or
                     (bid is not None and bid > 0) or
                     (ask is not None and ask > 0))
                )

                if live_ok:
                    self.live_market_data_active = True
                    self.market_data_available = True
                    self.update_ib_message_safe(f"Subscribed to IB LIVE market data for {instrument_label}")
                else:
                    # Keep subscription; maybe live entitlements limited.
                    self.live_market_data_active = False
                    self.market_data_available = True
                    self.update_ib_message_safe(
                        f"Could not confirm live tick price for {instrument_label}. "
                        f"Will use IB historical prices as fallback where needed."
                    )

            except Exception as e:
                self.market_data_available = False
                self.root.after(0, lambda: self.set_quote_values(None, None, None))
                self.update_ib_message_safe(f"IB market data subscribe failed: {e}")

        threading.Thread(target=_worker, daemon=True).start()

    def on_ib_ticker_update(self, ticker):
        try:
            bid = self.safe_float(getattr(ticker, "bid", None))
            ask = self.safe_float(getattr(ticker, "ask", None))
            last = self.safe_float(getattr(ticker, "last", None))

            if last is None or last <= 0:
                try:
                    mp = self.safe_float(ticker.marketPrice())
                    if mp is not None and mp > 0:
                        last = mp
                except Exception:
                    pass

            if last is None or last <= 0:
                close_val = self.safe_float(getattr(ticker, "close", None))
                if close_val is not None and close_val > 0:
                    last = close_val

            if bid is None or bid <= 0:
                bid = last
            if ask is None or ask <= 0:
                ask = last

            market_data_type = getattr(ticker, "marketDataType", None)
            if market_data_type == 1 and ((last and last > 0) or (bid and bid > 0) or (ask and ask > 0)):
                self.live_market_data_active = True

            self.root.after(0, lambda b=bid, a=ask, l=last: self.handle_streaming_quote_update(b, a, l))
        except Exception:
            pass

    def handle_streaming_quote_update(self, bid, ask, last):
        if not self.is_connected():
            return
        self.set_quote_values(bid, ask, last)

    # =========================================================
    # Live / historical IB pricing
    # =========================================================
    def fetch_ib_historical_dataframe(
        self,
        instrument_label,
        duration_str="2 D",
        bar_size="1 min",
        what_to_show="TRADES",
        use_rth=False,
        tail_count=None
    ):
        contract = self.get_contract_for_instrument(instrument_label)

        def _fetch():
            bars = self.ibw.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=use_rth,
                formatDate=1,
                keepUpToDate=False
            )
            return bars

        bars = self.ib_call(_fetch)

        if not bars:
            raise RuntimeError("No historical market data returned from IB")

        df = util.df(bars)
        if df is None or df.empty:
            raise RuntimeError("IB historical dataframe is empty")

        if "date" not in df.columns or "close" not in df.columns:
            raise RuntimeError("IB historical data missing required fields")

        df = df[["date", "close"]].copy()
        df.columns = ["Date", "Close"]

        idx = pd.to_datetime(df["Date"], errors="coerce")
        if idx.isna().all():
            raise RuntimeError("Could not parse IB historical dates")

        if getattr(idx.dt, "tz", None) is None:
            idx = idx.dt.tz_localize("UTC")
        else:
            idx = idx.dt.tz_convert("UTC")

        df.index = idx.dt.tz_convert(AMSTERDAM_TZ)
        df = df.drop(columns=["Date"])
        df = df[["Close"]].dropna()

        if tail_count is not None:
            df = df.tail(tail_count)

        if df.empty:
            raise RuntimeError("IB historical dataframe empty after cleanup")

        return df

    def get_best_price_for_order(self, contract, instrument_label):
        if self.current_quote_instrument_label == instrument_label:
            last = self.safe_float(self.last_price)
            bid = self.safe_float(self.bid_price)
            ask = self.safe_float(self.ask_price)

            if last is not None and last > 0 and self.live_market_data_active:
                return float(last)

            mid = None
            if bid is not None and bid > 0 and ask is not None and ask > 0:
                mid = (bid + ask) / 2.0

            if mid is not None and mid > 0 and self.live_market_data_active:
                return float(mid)

        # fallback to IB historical only
        df = self.fetch_ib_historical_dataframe(
            instrument_label=instrument_label,
            duration_str="1 D",
            bar_size="1 min",
            what_to_show="TRADES",
            use_rth=False,
            tail_count=3
        )

        price = self.safe_float(df["Close"].iloc[-1]) if not df.empty else None
        if price is not None and price > 0:
            return float(price)

        raise RuntimeError("Could not obtain live or historical IB price")

    # =========================================================
    # Account / positions
    # =========================================================
    def get_order_quantity_from_usd(self, instrument_label, amount_usd):
        contract = self.get_contract_for_instrument(instrument_label)
        price = self.get_best_price_for_order(contract, instrument_label)
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

    def refresh_live_position_tracking_from_ib(self, instrument_label):
        try:
            p = self.get_ib_position_for_selected_instrument(instrument_label)
            if p is None or float(p.position) == 0:
                self.live_strategy_positions.pop(instrument_label, None)
        except Exception:
            pass

    def close_position_by_display_instrument(self, display_instrument, reason_label="MANUAL CLOSE POSITION"):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return False

        def _worker():
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
                    self.update_ib_message_safe(f"{reason_label}: position not found")
                    return False

                signed_qty = float(target_position.position)
                if signed_qty == 0:
                    self.update_ib_message_safe(f"{reason_label}: no position to close")
                    return False

                action = "SELL" if signed_qty > 0 else "BUY"
                qty = abs(int(round(signed_qty)))
                close_contract = self.get_close_contract_for_position(target_position.contract)

                def _close():
                    order = self.create_market_order(action, qty)
                    trade = self.ibw.ib.placeOrder(close_contract, order)
                    return trade

                trade = self.ib_call(_close)

                filled = self.wait_for_order_terminal(trade, timeout_sec=12)
                self.root.after(0, self.refresh_ib_state)
                self.root.after(0, self.refresh_ib_transactions)

                status = trade.orderStatus.status if trade.orderStatus else "Submitted"
                self.update_ib_message_safe(
                    f"{reason_label}: {action} {qty} {display_instrument} to close | IB status: {status}"
                )
                return filled

            except Exception as e:
                self.update_ib_message_safe(f"Close position failed: {e}")
                return False

        threading.Thread(target=_worker, daemon=True).start()
        return True

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
                self.ibw.ib.reqMarketDataType(1)
                return self.ibw.ib.managedAccounts()

            accounts = self.ib_call(_connect)

            self.ib_connected = True
            self.connected_account_type = self.account_type.get()
            self.current_account_id = accounts[0] if accounts else None

            self.update_ib_message(
                f"Connected to IB {self.connected_account_type}"
                + (f" | Account: {self.current_account_id}" if self.current_account_id else "")
                + " | Price source: Interactive Brokers only"
            )

            self.refresh_ib_state()
            self.refresh_ib_transactions()
            self.subscribe_market_data_for_selected_instrument()
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
            self.cancel_market_data_subscription()
            self.set_quote_values(None, None, None)
            self.stop_chart_refresh()
            self.show_chart_placeholder("Connection required for chart")
            self.chart_status_var.set("Connection required for chart")
            self.update_ib_message(f"Connection failed: {e}")
            self.update_ui()
            self.refresh_positions_table()
            self.refresh_transactions_table()

    def disconnect_ib(self):
        self.cancel_market_data_subscription()

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
        self.live_strategy_positions = {}
        self.set_quote_values(None, None, None)
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
            self.refresh_live_position_tracking_from_ib(self.instrument_var.get().strip())

        except Exception as e:
            self.update_ib_message(f"IB refresh failed: {e}")

    # =========================================================
    # Order placement / fills
    # =========================================================
    def wait_for_order_terminal(self, trade, timeout_sec=12):
        def _done():
            try:
                status = trade.orderStatus.status if trade.orderStatus else ""
                return status in ("Filled", "Cancelled", "Inactive", "ApiCancelled")
            except Exception:
                return False

        return self.wait_until(_done, timeout_sec=timeout_sec, sleep_sec=0.2)

    def get_trade_avg_fill_price(self, trade):
        try:
            if trade.orderStatus and self.safe_float(trade.orderStatus.avgFillPrice):
                avg = self.safe_float(trade.orderStatus.avgFillPrice)
                if avg and avg > 0:
                    return float(avg)
        except Exception:
            pass

        try:
            fills = trade.fills or []
            prices = []
            qtys = []

            for f in fills:
                execution = getattr(f, "execution", None)
                if execution is None:
                    continue
                px = self.safe_float(getattr(execution, "price", None))
                sh = self.safe_float(getattr(execution, "shares", None))
                if px is not None and sh is not None and sh > 0:
                    prices.append(px * sh)
                    qtys.append(sh)

            if qtys and sum(qtys) > 0:
                return sum(prices) / sum(qtys)
        except Exception:
            pass

        return None

    def place_market_order(self, instrument_label, action, amount_usd, reason_label):
        if not self.is_connected():
            self.root.after(0, lambda: self.update_ib_message("Not connected to IB"))
            return False, None, None

        try:
            contract, qty, est_price = self.get_order_quantity_from_usd(instrument_label, amount_usd)

            def _place():
                order = self.create_market_order(action, qty)
                trade = self.ibw.ib.placeOrder(contract, order)
                return trade

            trade = self.ib_call(_place)
            self.wait_for_order_terminal(trade, timeout_sec=12)

            self.root.after(0, self.refresh_ib_state)
            self.root.after(0, self.refresh_ib_transactions)

            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            avg_fill = self.get_trade_avg_fill_price(trade)

            self.root.after(
                0,
                lambda: self.update_ib_message(
                    f"{reason_label}: {action} {qty} {instrument_label} "
                    f"(est px {est_price:.5f})"
                    + (f" | avg fill {avg_fill:.5f}" if avg_fill else "")
                    + f" | IB status: {status}"
                )
            )
            return True, qty, avg_fill or est_price

        except Exception as e:
            self.root.after(0, lambda: self.update_ib_message(f"Order failed: {e}"))
            return False, None, None

    def close_selected_instrument_position(self, instrument_label, reason_label):
        if not self.is_connected():
            self.update_ib_message("Not connected to IB")
            return False

        try:
            p = self.get_ib_position_for_selected_instrument(instrument_label)
            if p is None or float(p.position) == 0:
                self.update_ib_message(f"{reason_label}: no existing position to close")
                self.live_strategy_positions.pop(instrument_label, None)
                return False

            signed_qty = float(p.position)
            action = "SELL" if signed_qty > 0 else "BUY"
            qty = abs(int(round(signed_qty)))
            close_contract = self.get_close_contract_for_position(p.contract)

            def _close():
                order = self.create_market_order(action, qty)
                trade = self.ibw.ib.placeOrder(close_contract, order)
                return trade

            trade = self.ib_call(_close)
            self.wait_for_order_terminal(trade, timeout_sec=12)

            flattened = self.wait_until(
                lambda: (
                    (self.get_ib_position_for_selected_instrument(instrument_label) is None) or
                    (float(self.get_ib_position_for_selected_instrument(instrument_label).position) == 0)
                ),
                timeout_sec=10,
                sleep_sec=0.3
            )

            self.root.after(0, self.refresh_ib_state)
            self.root.after(0, self.refresh_ib_transactions)

            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            self.update_ib_message_safe(f"{reason_label}: closed {instrument_label} | IB status: {status}")

            if flattened:
                self.live_strategy_positions.pop(instrument_label, None)

            return flattened

        except Exception as e:
            self.update_ib_message_safe(f"Close failed: {e}")
            return False

    def buy_instrument(self):
        amount = self.validate_amount()
        if amount is None:
            return

        self.update_ib_message("Submitting manual buy...")
        threading.Thread(
            target=self.place_market_order,
            args=(self.instrument_var.get(), "BUY", amount, "MANUAL BUY"),
            daemon=True
        ).start()

    def sell_instrument(self):
        amount = self.validate_amount()
        if amount is None:
            return

        self.update_ib_message("Submitting manual sell...")
        threading.Thread(
            target=self.place_market_order,
            args=(self.instrument_var.get(), "SELL", amount, "MANUAL SELL"),
            daemon=True
        ).start()

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
            self.trade_strategy_combo.config(state="disabled")
        else:
            self.bot_status_var.set("Stopped")
            self.bot_status_label.config(foreground="red")
            self.bot_button.config(text="Start Bot")
            self.tp_sl_combo.config(state="readonly")
            self.trade_strategy_combo.config(state="readonly")

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

        instrument_label = self.instrument_var.get().strip()
        if not instrument_label:
            self.update_ib_message("Please select an instrument")
            return

        self.bot_running = True
        self.last_bot_signal_id = None
        self.set_bot_action_in_progress(False)
        self.refresh_live_position_tracking_from_ib(instrument_label)
        self.update_ui()
        self.update_ib_message(f"Auto-trading bot started | Strategy: {self.trade_strategy_var.get()}")
        self.refresh_chart()

    def stop_bot(self):
        self.bot_running = False
        self.set_bot_action_in_progress(False)
        self.update_ui()
        self.update_ib_message("Auto-trading bot stopped")

    def process_bot_logic(self, instrument_label, df, strategy_name):
        if not self.bot_running or len(df) < 3 or not self.is_connected():
            return

        if self.is_bot_action_in_progress():
            return

        amount_usd = self.validate_amount()
        if amount_usd is None:
            return

        prev_row = df.iloc[-2]
        curr_row = df.iloc[-1]
        current_bar_time = str(df.index[-1])

        try:
            ib_position = self.get_ib_position_for_selected_instrument(instrument_label)
            current_signed_pos = float(ib_position.position) if ib_position else 0.0
        except Exception:
            current_signed_pos = 0.0

        tracked_pos = self.live_strategy_positions.get(instrument_label)

        if current_signed_pos == 0:
            tracked_pos = None
            self.live_strategy_positions.pop(instrument_label, None)
        elif tracked_pos is None:
            # Existing position but entry price unknown. Use latest close as fallback.
            fallback_price = float(curr_row["Close"])
            self.live_strategy_positions[instrument_label] = {
                "side": "LONG" if current_signed_pos > 0 else "SHORT",
                "entry_price": fallback_price,
                "qty": abs(int(round(current_signed_pos))),
                "entry_time": df.index[-1]
            }
            tracked_pos = self.live_strategy_positions[instrument_label]

        if self.tp_sl_var.get() == "Yes" and tracked_pos is not None:
            exit_needed, exit_reason = self.should_exit_on_tp_sl(strategy_name, tracked_pos, df, len(df) - 1)
            if exit_needed:
                signal_id = f"{strategy_name}_{tracked_pos['side']}_EXIT_{current_bar_time}_{exit_reason}"
                if signal_id != self.last_bot_signal_id:
                    self.last_bot_signal_id = signal_id

                    def _tp_close():
                        self.set_bot_action_in_progress(True)
                        try:
                            self.close_selected_instrument_position(instrument_label, f"BOT {exit_reason}")
                        finally:
                            self.root.after(3000, lambda: self.set_bot_action_in_progress(False))

                    threading.Thread(target=_tp_close, daemon=True).start()
                return

        signal = self.get_strategy_signal(strategy_name, prev_row, curr_row)
        if signal is None:
            return

        if signal == "LONG":
            signal_id = f"{strategy_name}_LONG_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return
            self.last_bot_signal_id = signal_id

            def _long_worker():
                self.set_bot_action_in_progress(True)
                try:
                    if current_signed_pos < 0:
                        ok_close = self.close_selected_instrument_position(instrument_label, "BOT REVERSE CLOSE SHORT")
                        if not ok_close:
                            return

                    current_after_close = self.get_ib_position_for_selected_instrument(instrument_label)
                    current_after_close_pos = float(current_after_close.position) if current_after_close else 0.0

                    if current_after_close_pos <= 0:
                        ok, qty, fill_price = self.place_market_order(
                            instrument_label, "BUY", amount_usd, f"BOT OPEN LONG | {strategy_name}"
                        )
                        if ok and qty and fill_price:
                            self.live_strategy_positions[instrument_label] = {
                                "side": "LONG",
                                "entry_time": self.amsterdam_now(),
                                "entry_price": float(fill_price),
                                "qty": qty
                            }
                except Exception as e:
                    self.update_ib_message_safe(f"Bot long worker failed: {e}")
                finally:
                    self.root.after(3000, lambda: self.set_bot_action_in_progress(False))

            threading.Thread(target=_long_worker, daemon=True).start()

        elif signal == "SHORT":
            signal_id = f"{strategy_name}_SHORT_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return
            self.last_bot_signal_id = signal_id

            def _short_worker():
                self.set_bot_action_in_progress(True)
                try:
                    if current_signed_pos > 0:
                        ok_close = self.close_selected_instrument_position(instrument_label, "BOT REVERSE CLOSE LONG")
                        if not ok_close:
                            return

                    current_after_close = self.get_ib_position_for_selected_instrument(instrument_label)
                    current_after_close_pos = float(current_after_close.position) if current_after_close else 0.0

                    if current_after_close_pos >= 0:
                        ok, qty, fill_price = self.place_market_order(
                            instrument_label, "SELL", amount_usd, f"BOT OPEN SHORT | {strategy_name}"
                        )
                        if ok and qty and fill_price:
                            self.live_strategy_positions[instrument_label] = {
                                "side": "SHORT",
                                "entry_time": self.amsterdam_now(),
                                "entry_price": float(fill_price),
                                "qty": qty
                            }
                except Exception as e:
                    self.update_ib_message_safe(f"Bot short worker failed: {e}")
                finally:
                    self.root.after(3000, lambda: self.set_bot_action_in_progress(False))

            threading.Thread(target=_short_worker, daemon=True).start()

    # =========================================================
    # IB chart data only
    # =========================================================
    def get_chart_dataframe_from_ib(self, instrument_label, strategy_name):
        # Prefer live-tick-backed current quote, but indicators need bars.
        # So fetch historical bars from IB only.
        min_bars_needed = {
            "Bollinger Bands": 25,
            "SMA Crossover": 35,
            "EMA Crossover": 35,
            "RSI Reversion": 20,
            "MACD Crossover": 40,
            "Donchian Breakout": 25
        }.get(strategy_name, 40)

        df = self.fetch_ib_historical_dataframe(
            instrument_label=instrument_label,
            duration_str="2 D",
            bar_size="1 min",
            what_to_show="TRADES",
            use_rth=False,
            tail_count=max(120, min_bars_needed + 5)
        )

        # If live tick exists, overwrite last close with the best live tick/mid
        try:
            if self.current_quote_instrument_label == instrument_label and self.live_market_data_active:
                live_last = self.safe_float(self.last_price)
                live_bid = self.safe_float(self.bid_price)
                live_ask = self.safe_float(self.ask_price)

                best_live = None
                if live_last is not None and live_last > 0:
                    best_live = live_last
                elif live_bid is not None and live_bid > 0 and live_ask is not None and live_ask > 0:
                    best_live = (live_bid + live_ask) / 2.0

                if best_live is not None and not df.empty:
                    df.iloc[-1, df.columns.get_loc("Close")] = float(best_live)
        except Exception:
            pass

        return df

    # =========================================================
    # Backtest
    # =========================================================
    def choose_ib_backtest_bar_size(self, start_dt, end_dt):
        days_diff = (end_dt - start_dt).days

        if days_diff <= 3:
            return "5 mins", "3 D"
        if days_diff <= 10:
            return "15 mins", "10 D"
        if days_diff <= 30:
            return "30 mins", "1 M"
        if days_diff <= 180:
            return "1 hour", "6 M"
        if days_diff <= 365:
            return "1 day", "1 Y"
        return "1 day", "2 Y"

    def fetch_backtest_dataframe_from_ib(self, instrument_label, start_dt, end_dt):
        bar_size, duration_str = self.choose_ib_backtest_bar_size(start_dt, end_dt)

        df = self.fetch_ib_historical_dataframe(
            instrument_label=instrument_label,
            duration_str=duration_str,
            bar_size=bar_size,
            what_to_show="TRADES",
            use_rth=False,
            tail_count=None
        )

        start_ts = pd.Timestamp(start_dt).tz_localize(AMSTERDAM_TZ)
        end_ts = pd.Timestamp(end_dt + timedelta(days=1)).tz_localize(AMSTERDAM_TZ)

        df = df[(df.index >= start_ts) & (df.index <= end_ts)]

        if df.empty:
            raise RuntimeError("Backtest dataframe empty after IB date filter")

        return df

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

        strategy_name = self.backtest_strategy_var.get().strip()
        if not strategy_name:
            self.backtest_status_var.set("Please select a strategy")
            return

        self.backtest_loading = True
        self.backtest_button.config(state="disabled")
        self.backtest_status_var.set("Running backtest...")
        self.backtest_chart_status_var.set("Running backtest and preparing graph...")

        thread = threading.Thread(
            target=self.run_backtest_thread,
            args=(instrument_label, strategy_name, start_dt, end_dt, amount_usd, self.backtest_tp_sl_var.get()),
            daemon=True
        )
        thread.start()

    def run_backtest_thread(self, instrument_label, strategy_name, start_dt, end_dt, amount_usd, tp_sl_value):
        try:
            raw_df = self.fetch_backtest_dataframe_from_ib(instrument_label, start_dt, end_dt)
            df = self.add_strategy_columns(raw_df, strategy_name)

            results = self.simulate_strategy_backtest(
                instrument_label=instrument_label,
                strategy_name=strategy_name,
                df=df,
                amount_usd=amount_usd,
                tp_sl_enabled=(tp_sl_value == "Yes")
            )

            self.root.after(
                0,
                lambda il=instrument_label, st=strategy_name, res=results, data=df:
                self.handle_backtest_success(il, st, res, data)
            )
        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda msg=err_msg: self.handle_backtest_error(msg))

    def simulate_strategy_backtest(self, instrument_label, strategy_name, df, amount_usd, tp_sl_enabled):
        current_position = None
        long_count = 0
        short_count = 0
        closed_trades = []
        signals = []

        for i in range(1, len(df)):
            prev_row = df.iloc[i - 1]
            curr_row = df.iloc[i]
            current_time = df.index[i]
            curr_close = float(curr_row["Close"])

            if tp_sl_enabled and current_position is not None:
                exit_needed, exit_reason = self.should_exit_on_tp_sl(strategy_name, current_position, df, i)

                if exit_needed:
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
                        "reason": exit_reason
                    })

                    if current_position["side"] == "LONG":
                        signals.append({
                            "time": current_time,
                            "price": exit_price,
                            "action": "SELL",
                            "kind": "CLOSE_LONG",
                            "reason": exit_reason
                        })
                    else:
                        signals.append({
                            "time": current_time,
                            "price": exit_price,
                            "action": "BUY",
                            "kind": "CLOSE_SHORT",
                            "reason": exit_reason
                        })

                    current_position = None
                    continue

            signal = self.get_strategy_signal(strategy_name, prev_row, curr_row)

            if signal == "LONG":
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
                        "action": "BUY",
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
                        "action": "BUY",
                        "kind": "OPEN_LONG",
                        "reason": "LONG ENTRY"
                    })

            elif signal == "SHORT":
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
                        "action": "SELL",
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
                        "action": "SELL",
                        "kind": "OPEN_SHORT",
                        "reason": "SHORT ENTRY"
                    })

        total_gain = sum(trade["pnl_value"] for trade in closed_trades if trade["pnl_value"] > 0)
        total_loss = sum(abs(trade["pnl_value"]) for trade in closed_trades if trade["pnl_value"] < 0)
        total_pnl = sum(trade["pnl_value"] for trade in closed_trades)

        for trade in closed_trades:
            trade.pop("pnl_value", None)

        return {
            "long_count": long_count,
            "short_count": short_count,
            "closed_trades_count": len(closed_trades),
            "total_gain": total_gain,
            "total_loss": total_loss,
            "total_pnl": total_pnl,
            "trades": closed_trades,
            "signals": signals
        }

    def handle_backtest_success(self, instrument_label, strategy_name, results, df):
        self.backtest_loading = False
        self.backtest_button.config(state="normal")

        self.backtest_results = results["trades"]
        self.backtest_plot_df = df.copy()
        self.backtest_plot_signals = results["signals"]

        self.backtest_long_count_var.set(str(results["long_count"]))
        self.backtest_short_count_var.set(str(results["short_count"]))
        self.backtest_total_trades_var.set(str(results["closed_trades_count"]))
        self.backtest_total_gain_var.set(f'{results["total_gain"]:,.2f}')
        self.backtest_total_loss_var.set(f'{results["total_loss"]:,.2f}')
        self.backtest_total_pnl_var.set(f'{results["total_pnl"]:,.2f}')

        self.backtest_total_pnl_label.config(
            foreground="green" if results["total_pnl"] >= 0 else "red"
        )

        self.refresh_backtest_table()
        self.draw_backtest_chart(instrument_label, strategy_name, self.backtest_plot_df, self.backtest_plot_signals)

        self.backtest_status_var.set(f"Backtest finished for {instrument_label} | {strategy_name}")
        self.backtest_chart_status_var.set(
            f"{instrument_label} | {strategy_name} | Long Opens: {results['long_count']} | Short Opens: {results['short_count']}"
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

    def draw_backtest_chart(self, instrument_label, strategy_name, df, signals):
        if df is None or df.empty:
            self.show_backtest_chart_placeholder("No backtest chart data")
            return

        self.backtest_ax.clear()

        self.backtest_ax.plot(df.index, df["Close"], label="Close")
        self.draw_strategy_overlay(self.backtest_ax, df, strategy_name)

        buy_open_times = [s["time"] for s in signals if s["kind"] == "OPEN_LONG"]
        buy_open_prices = [s["price"] for s in signals if s["kind"] == "OPEN_LONG"]

        sell_open_times = [s["time"] for s in signals if s["kind"] == "OPEN_SHORT"]
        sell_open_prices = [s["price"] for s in signals if s["kind"] == "OPEN_SHORT"]

        buy_close_times = [s["time"] for s in signals if s["kind"] == "CLOSE_SHORT"]
        buy_close_prices = [s["price"] for s in signals if s["kind"] == "CLOSE_SHORT"]

        sell_close_times = [s["time"] for s in signals if s["kind"] == "CLOSE_LONG"]
        sell_close_prices = [s["price"] for s in signals if s["kind"] == "CLOSE_LONG"]

        if buy_open_times:
            self.backtest_ax.scatter(buy_open_times, buy_open_prices, marker="^", s=75, label="Buy Open Long")
        if sell_open_times:
            self.backtest_ax.scatter(sell_open_times, sell_open_prices, marker="v", s=75, label="Sell Open Short")
        if buy_close_times:
            self.backtest_ax.scatter(buy_close_times, buy_close_prices, marker="o", s=55, label="Buy Close Short")
        if sell_close_times:
            self.backtest_ax.scatter(sell_close_times, sell_close_prices, marker="o", s=55, label="Sell Close Long")

        self.backtest_ax.set_title(f"{instrument_label} - {strategy_name} - Back Test Signals", fontsize=13)
        self.backtest_ax.set_xlabel("Time (Amsterdam)")
        self.backtest_ax.set_ylabel("Price")
        self.backtest_ax.grid(True, alpha=0.3)
        self.backtest_ax.legend(loc="upper left")
        self.backtest_ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M", tz=AMSTERDAM_TZ))

        self.backtest_figure.autofmt_xdate()
        self.backtest_figure.tight_layout()
        self.backtest_canvas.draw()

    # =========================================================
    # Live chart
    # =========================================================
    def on_instrument_change(self, event=None):
        if self.is_connected():
            self.subscribe_market_data_for_selected_instrument()
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
        strategy_name = self.trade_strategy_var.get().strip()

        if not instrument_label or not strategy_name:
            return

        self.chart_loading = True
        self.chart_status_var.set(f"Loading chart for {instrument_label}...")

        thread = threading.Thread(
            target=self.fetch_chart_data_thread,
            args=(instrument_label, strategy_name),
            daemon=True
        )
        thread.start()

    def fetch_chart_data_thread(self, instrument_label, strategy_name):
        try:
            raw_df = self.get_chart_dataframe_from_ib(instrument_label, strategy_name)
            df = self.add_strategy_columns(raw_df, strategy_name)
            self.root.after(0, lambda il=instrument_label, st=strategy_name, data=df: self.draw_chart(il, st, data))
        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda il=instrument_label, msg=err_msg: self.handle_chart_error(il, msg))

    def handle_chart_error(self, instrument_label, message):
        self.chart_loading = False
        self.ax.clear()
        self.ax.set_title(f"{instrument_label} - Strategy Chart")
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
        self.ax.set_title("Strategy Chart")
        self.ax.text(
            0.5, 0.5, message,
            ha="center", va="center", transform=self.ax.transAxes, fontsize=12
        )
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.figure.tight_layout()
        self.canvas.draw()

    def draw_chart(self, instrument_label, strategy_name, df):
        self.chart_loading = False

        if instrument_label != self.instrument_var.get().strip():
            return

        if not self.is_connected():
            self.show_chart_placeholder("Connect first to load the chart")
            return

        self.ax.clear()
        self.ax.plot(df.index, df["Close"], label="Close")
        self.draw_strategy_overlay(self.ax, df, strategy_name)

        last_price = float(df["Close"].iloc[-1])

        self.ax.set_title(f"{instrument_label} - {strategy_name} - IB Price", fontsize=14)
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
        strategy_text = f" | Strategy: {strategy_name}"
        extra_text = self.build_strategy_status_suffix(df, strategy_name)

        if self.live_market_data_active:
            source_text = " | Live tick active"
        else:
            source_text = " | Historical IB fallback"

        self.chart_status_var.set(
            f"{instrument_label} updated at {update_time} | Last Price: {last_price:.5f}"
            f"{strategy_text}{extra_text}{source_text}{bot_text}"
        )

        self.process_bot_logic(instrument_label, df, strategy_name)

    # =========================================================
    # Close
    # =========================================================
    def on_close(self):
        self.stop_chart_refresh()
        self.stop_bot()
        self.cancel_market_data_subscription()
        self.disconnect_ib()
        self.ibw.stop()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = ConnectionApp(root)
    root.mainloop()
