import asyncio

# Python 3.14 fix: create a loop before importing ib_insync/eventkit
_main_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_main_loop)

import math
import queue
import threading
import tkinter as tk
from tkinter import ttk
from datetime import datetime

import yfinance as yf
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from ib_insync import IB, Stock, Future, MarketOrder


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
        self.root.geometry("1320x960")

        # ---------------- IB state ----------------
        self.ibw = IBWorker()
        self.ibw.start()

        self.ib_connected = False
        self.connected_account_type = None
        self.current_account_id = None

        # ---------------- UI/account data ----------------
        self.buying_power_usd = 0.00
        self.withdrawable_cash_usd = 0.00
        self.positions = []
        self.transactions = []

        # ---------------- Chart state ----------------
        self.chart_update_interval_ms = 15000
        self.chart_after_id = None
        self.chart_loading = False

        # ---------------- Bot state ----------------
        self.bot_running = False
        self.last_bot_signal_id = None

        # ---------------- Instrument config ----------------
        self.instrument_market_map = {
            "NVDA": "NVDA",
            "AAPL": "AAPL",
            "MSFT": "MSFT",
            "TSLA": "TSLA",
            "AMZN": "AMZN",
            "META": "META",
            "GOOGL": "GOOGL",
            "EUR Mar26 CME": "6E=F"
        }

        self.instrument_contract_config = {
            "NVDA": {"type": "STK", "symbol": "NVDA", "exchange": "SMART", "currency": "USD"},
            "AAPL": {"type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
            "MSFT": {"type": "STK", "symbol": "MSFT", "exchange": "SMART", "currency": "USD"},
            "TSLA": {"type": "STK", "symbol": "TSLA", "exchange": "SMART", "currency": "USD"},
            "AMZN": {"type": "STK", "symbol": "AMZN", "exchange": "SMART", "currency": "USD"},
            "META": {"type": "STK", "symbol": "META", "exchange": "SMART", "currency": "USD"},
            "GOOGL": {"type": "STK", "symbol": "GOOGL", "exchange": "SMART", "currency": "USD"},
            "EUR Mar26 CME": {
                "type": "FUT",
                "symbol": "EUR",
                "exchange": "CME",
                "currency": "USD",
                "expiry": "202603",
                "multiplier": 125000
            }
        }

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

        # ---------------- Row 2 ----------------
        row2 = ttk.Frame(main_frame)
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

        ttk.Label(row2, text="Cash Available to Withdraw (USD):").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")

        self.withdrawable_cash_var = tk.StringVar(value="0.00")
        self.withdrawable_cash_label = ttk.Label(
            row2,
            textvariable=self.withdrawable_cash_var,
            font=("Arial", 10, "bold"),
            width=18,
            anchor="w"
        )
        self.withdrawable_cash_label.grid(row=0, column=3, padx=(0, 6), pady=5, sticky="w")

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

        # ---------------- Row 3 ----------------
        row3 = ttk.Frame(main_frame)
        row3.pack(fill="x", pady=(0, 12))

        ttk.Label(row3, text="Instrument:").grid(row=0, column=0, padx=(0, 6), pady=5, sticky="w")

        self.instrument_var = tk.StringVar(value="EUR Mar26 CME")
        self.instrument_combo = ttk.Combobox(
            row3,
            textvariable=self.instrument_var,
            values=list(self.instrument_contract_config.keys()),
            state="readonly",
            width=20
        )
        self.instrument_combo.grid(row=0, column=1, padx=(0, 16), pady=5, sticky="w")
        self.instrument_combo.bind("<<ComboboxSelected>>", self.on_instrument_change)

        ttk.Label(row3, text="Amount (USD):").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")

        self.amount_var = tk.StringVar()
        self.amount_entry = ttk.Entry(row3, textvariable=self.amount_var, width=14)
        self.amount_entry.grid(row=0, column=3, padx=(0, 16), pady=5, sticky="w")

        self.buy_button = ttk.Button(row3, text="Buy", command=self.buy_instrument, width=10)
        self.buy_button.grid(row=0, column=4, padx=(0, 8), pady=5, sticky="w")

        self.sell_button = ttk.Button(row3, text="Sell", command=self.sell_instrument, width=10)
        self.sell_button.grid(row=0, column=5, padx=(0, 8), pady=5, sticky="w")

        ttk.Label(row3, text="TP/SL:").grid(row=0, column=6, padx=(18, 6), pady=5, sticky="w")

        self.tp_sl_var = tk.StringVar(value="Yes")
        self.tp_sl_combo = ttk.Combobox(
            row3,
            textvariable=self.tp_sl_var,
            values=["Yes", "No"],
            state="readonly",
            width=8
        )
        self.tp_sl_combo.grid(row=0, column=7, padx=(0, 16), pady=5, sticky="w")

        self.bot_button = ttk.Button(
            row3,
            text="Start Bot",
            command=self.toggle_bot,
            width=12
        )
        self.bot_button.grid(row=0, column=8, padx=(0, 8), pady=5, sticky="w")

        self.refresh_ib_button = ttk.Button(
            row3,
            text="Refresh IB",
            command=self.refresh_ib_state,
            width=12
        )
        self.refresh_ib_button.grid(row=0, column=9, padx=(0, 8), pady=5, sticky="w")

        # ---------------- Row 4 ----------------
        row4 = ttk.Frame(main_frame)
        row4.pack(fill="x", pady=(0, 12))

        positions_group = ttk.LabelFrame(row4, text="Positions (from IB)", padding=8)
        positions_group.pack(side="left", fill="both", expand=True, padx=(0, 8))

        self.positions_tree = ttk.Treeview(
            positions_group,
            columns=("instrument", "amount"),
            show="headings",
            height=8
        )
        self.positions_tree.heading("instrument", text="Instrument")
        self.positions_tree.heading("amount", text="Amount")
        self.positions_tree.column("instrument", width=220, anchor="w")
        self.positions_tree.column("amount", width=140, anchor="e")
        self.positions_tree.pack(side="left", fill="both", expand=True)

        positions_scroll = ttk.Scrollbar(
            positions_group,
            orient="vertical",
            command=self.positions_tree.yview
        )
        self.positions_tree.configure(yscrollcommand=positions_scroll.set)
        positions_scroll.pack(side="right", fill="y")

        transactions_group = ttk.LabelFrame(row4, text="Transactions (app log)", padding=8)
        transactions_group.pack(side="left", fill="both", expand=True, padx=(8, 0))

        self.transactions_tree = ttk.Treeview(
            transactions_group,
            columns=("time", "side", "instrument", "amount_usd"),
            show="headings",
            height=8
        )
        self.transactions_tree.heading("time", text="Time")
        self.transactions_tree.heading("side", text="Side")
        self.transactions_tree.heading("instrument", text="Instrument")
        self.transactions_tree.heading("amount_usd", text="Amount (USD)")
        self.transactions_tree.column("time", width=170, anchor="w")
        self.transactions_tree.column("side", width=170, anchor="center")
        self.transactions_tree.column("instrument", width=170, anchor="w")
        self.transactions_tree.column("amount_usd", width=130, anchor="e")
        self.transactions_tree.pack(side="left", fill="both", expand=True)

        transactions_scroll = ttk.Scrollbar(
            transactions_group,
            orient="vertical",
            command=self.transactions_tree.yview
        )
        self.transactions_tree.configure(yscrollcommand=transactions_scroll.set)
        transactions_scroll.pack(side="right", fill="y")

        # ---------------- Row 5 ----------------
        chart_group = ttk.LabelFrame(main_frame, text="Price Chart with Bollinger Bands", padding=10)
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

        self.update_ui()
        self.refresh_positions_table()
        self.refresh_transactions_table()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # =========================================================
    # Helpers
    # =========================================================
    def ib_call(self, fn, *args, **kwargs):
        return self.ibw.call(fn, *args, **kwargs)

    def update_ib_message(self, message):
        self.ib_message_var.set(message)

    def add_transaction_record(self, side, instrument, amount, time_str=None):
        if time_str is None:
            time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            amount = float(amount)
        except Exception:
            amount = 0.0

        self.transactions.insert(0, {
            "time": time_str,
            "side": side,
            "instrument": instrument,
            "amount_usd": amount
        })
        self.refresh_transactions_table()

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
                    f'{tx["amount_usd"]:,.2f}'
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
                    f'{position["amount"]:,.4f}'
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
                contract = Stock(cfg["symbol"], cfg["exchange"], cfg["currency"])
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

    def get_last_price_for_contract(self, contract, instrument_label):
        def _get_price():
            tickers = self.ibw.ib.reqTickers(contract)
            if tickers and tickers[0]:
                t = tickers[0]
                for px in [t.marketPrice(), t.last, t.close]:
                    if px is not None:
                        try:
                            px = float(px)
                            if px > 0 and not math.isnan(px):
                                return px
                        except Exception:
                            pass
            return None

        ib_price = self.ib_call(_get_price)
        if ib_price is not None:
            return ib_price

        yahoo_symbol = self.get_market_data_symbol(instrument_label)
        df = yf.Ticker(yahoo_symbol).history(period="1d", interval="1m", auto_adjust=True)
        if df is not None and not df.empty:
            return float(df["Close"].dropna().iloc[-1])

        raise RuntimeError(f"Could not get a valid price for {instrument_label}")

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
            )
            self.refresh_ib_state()
            self.refresh_chart()
            self.schedule_next_chart_refresh()
            self.update_ui()

        except Exception as e:
            self.ib_connected = False
            self.connected_account_type = None
            self.current_account_id = None
            self.buying_power_usd = 0.0
            self.withdrawable_cash_usd = 0.0
            self.positions = []
            self.stop_bot()
            self.stop_chart_refresh()
            self.show_chart_placeholder("Connection required for chart")
            self.chart_status_var.set("Connection required for chart")
            self.update_ib_message(f"Connection failed: {e}")
            self.update_ui()
            self.refresh_positions_table()

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
        self.withdrawable_cash_usd = 0.0
        self.positions = []
        self.stop_bot()
        self.stop_chart_refresh()
        self.show_chart_placeholder("Connect first to load the chart")
        self.chart_status_var.set("Connect first to load the chart")
        self.update_ib_message("Disconnected from IB")
        self.update_ui()
        self.refresh_positions_table()

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
            available_funds = None

            for row in summary:
                if self.current_account_id and row.account != self.current_account_id:
                    continue

                if row.tag == "BuyingPower" and row.currency == "USD":
                    buying_power = float(row.value)
                elif row.tag == "AvailableFunds" and row.currency == "USD":
                    available_funds = float(row.value)

            self.buying_power_usd = buying_power if buying_power is not None else 0.0
            self.withdrawable_cash_usd = available_funds if available_funds is not None else 0.0

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
                order = MarketOrder(action, qty)
                trade = self.ibw.ib.placeOrder(contract, order)
                self.ibw.ib.sleep(1.0)
                return trade

            trade = self.ib_call(_place)

            self.add_transaction_record(reason_label, instrument_label, amount_usd)
            self.refresh_ib_state()

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

            def _close():
                order = MarketOrder(action, qty)
                trade = self.ibw.ib.placeOrder(p.contract, order)
                self.ibw.ib.sleep(1.0)
                return trade

            trade = self.ib_call(_close)

            self.refresh_ib_state()
            status = trade.orderStatus.status if trade.orderStatus else "Submitted"
            self.add_transaction_record(reason_label, instrument_label, 0.0)
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
    # Bot
    # =========================================================
    def update_ui(self):
        self.buying_power_var.set(f"{self.buying_power_usd:,.2f}")
        self.withdrawable_cash_var.set(f"{self.withdrawable_cash_usd:,.2f}")

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
    # Chart
    # =========================================================
    def on_instrument_change(self, event=None):
        if self.is_connected():
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

        market_symbol = self.get_market_data_symbol(instrument_label)

        self.chart_loading = True
        self.chart_status_var.set(f"Loading chart for {instrument_label}...")

        thread = threading.Thread(
            target=self.fetch_chart_data_thread,
            args=(instrument_label, market_symbol),
            daemon=True
        )
        thread.start()

    def fetch_chart_data_thread(self, instrument_label, market_symbol):
        try:
            ticker = yf.Ticker(market_symbol)
            df = ticker.history(period="1d", interval="1m", auto_adjust=True)

            if df is None or df.empty:
                self.root.after(0, lambda: self.handle_chart_error(instrument_label, "No market data returned"))
                return

            df = df.copy()
            df = df[["Close"]].dropna()

            if df.empty:
                self.root.after(0, lambda: self.handle_chart_error(instrument_label, "No closing price data available"))
                return

            df = df.tail(120)

            df["Middle"] = df["Close"].rolling(window=20).mean()
            df["STD20"] = df["Close"].rolling(window=20).std()
            df["Upper"] = df["Middle"] + (2 * df["STD20"])
            df["Lower"] = df["Middle"] - (2 * df["STD20"])
            df = df.dropna()

            if df.empty:
                self.root.after(0, lambda: self.handle_chart_error(instrument_label, "Not enough data for Bollinger Bands"))
                return

            self.root.after(0, lambda: self.draw_chart(instrument_label, df))

        except Exception as e:
            self.root.after(0, lambda: self.handle_chart_error(instrument_label, str(e)))

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
        self.ax.set_xlabel("Time")
        self.ax.set_ylabel("Price")
        self.ax.legend(loc="upper left")
        self.ax.grid(True, alpha=0.3)

        self.figure.autofmt_xdate()
        self.figure.tight_layout()
        self.canvas.draw()

        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bot_text = " | Bot Running" if self.bot_running else ""
        self.chart_status_var.set(
            f"{instrument_label} updated at {update_time} | Last Price: {last_price:.5f}{bot_text}"
        )

        self.process_bot_logic(instrument_label, df)

    # =========================================================
    # Close
    # =========================================================
    def on_close(self):
        self.stop_chart_refresh()
        self.stop_bot()
        self.disconnect_ib()
        self.ibw.stop()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = ConnectionApp(root)
    root.mainloop()
