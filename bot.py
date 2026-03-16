import tkinter as tk
from tkinter import ttk
from datetime import datetime
import threading

import yfinance as yf
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


class ConnectionApp:
    def __init__(self, root):
        self.root = root
        self.root.title("IB Connection")
        self.root.geometry("1280x930")

        # Connection state
        self.ws_connected = False
        self.connected_account_type = None

        # Account data
        self.buying_power_usd = 0.00
        self.withdrawable_cash_usd = 0.00
        self.positions = []
        self.transactions = []

        # Chart state
        self.chart_update_interval_ms = 15000
        self.chart_after_id = None
        self.chart_loading = False
        self.current_chart_symbol = None

        # Bot state
        self.bot_running = False
        self.bot_position_side = None          # "LONG", "SHORT", or None
        self.bot_position_amount_usd = 0.0
        self.bot_position_instrument = None
        self.last_bot_signal_id = None

        # UI instrument -> market data symbol mapping
        self.instrument_market_map = {
            "NVDA": "NVDA",
            "AAPL": "AAPL",
            "MSFT": "MSFT",
            "TSLA": "TSLA",
            "AMZN": "AMZN",
            "META": "META",
            "GOOGL": "GOOGL",
            "EUR Mar16'26 CME": "6E=F"
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
            width=14
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

        self.instrument_var = tk.StringVar(value="NVDA")
        self.instrument_combo = ttk.Combobox(
            row3,
            textvariable=self.instrument_var,
            values=list(self.instrument_market_map.keys()),
            state="readonly",
            width=20
        )
        self.instrument_combo.grid(row=0, column=1, padx=(0, 16), pady=5, sticky="w")
        self.instrument_combo.bind("<<ComboboxSelected>>", self.on_instrument_change)

        ttk.Label(row3, text="Amount (USD):").grid(row=0, column=2, padx=(0, 6), pady=5, sticky="w")

        self.amount_var = tk.StringVar()
        self.amount_entry = ttk.Entry(
            row3,
            textvariable=self.amount_var,
            width=14
        )
        self.amount_entry.grid(row=0, column=3, padx=(0, 16), pady=5, sticky="w")

        self.buy_button = ttk.Button(
            row3,
            text="Buy",
            command=self.buy_instrument,
            width=10
        )
        self.buy_button.grid(row=0, column=4, padx=(0, 8), pady=5, sticky="w")

        self.sell_button = ttk.Button(
            row3,
            text="Sell",
            command=self.sell_instrument,
            width=10
        )
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

        # ---------------- Row 4: Smaller tables ----------------
        row4 = ttk.Frame(main_frame)
        row4.pack(fill="x", pady=(0, 12))

        positions_group = ttk.LabelFrame(row4, text="Positions", padding=8)
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

        transactions_group = ttk.LabelFrame(row4, text="Transactions", padding=8)
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
        self.transactions_tree.column("side", width=140, anchor="center")
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

        # ---------------- Row 5: Bigger chart ----------------
        chart_group = ttk.LabelFrame(main_frame, text="Price Chart with Bollinger Bands", padding=10)
        chart_group.pack(fill="both", expand=True)

        self.chart_status_var = tk.StringVar(value="Connect first to load the chart")
        self.chart_status_label = ttk.Label(
            chart_group,
            textvariable=self.chart_status_var,
            font=("Arial", 10)
        )
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

    # =========================
    # Basic Helpers
    # =========================
    def get_market_data_symbol(self, instrument_label):
        return self.instrument_market_map.get(instrument_label, instrument_label)

    def update_ib_message(self, message):
        self.ib_message_var.set(message)

    def is_really_connected(self, selected_account_type):
        return (
            self.ws_connected
            and self.connected_account_type == selected_account_type
        )

    def is_any_account_connected(self):
        return self.ws_connected and self.connected_account_type is not None

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

    # =========================
    # Connection
    # =========================
    def connect_to_ws(self, selected_account_type):
        try:
            print(f"Connecting to {selected_account_type}...")

            if selected_account_type == "Paper":
                self.ws_connected = True
                self.connected_account_type = "Paper"

                self.buying_power_usd = 6_667_242.33
                self.withdrawable_cash_usd = 1_000_000.00
                self.positions = []
                self.transactions = []

                self.bot_position_side = None
                self.bot_position_amount_usd = 0.0
                self.bot_position_instrument = None
                self.last_bot_signal_id = None

                self.update_ib_message("Connected to Paper account")

            elif selected_account_type == "Live":
                self.ws_connected = False
                self.connected_account_type = None
                self.buying_power_usd = 0.00
                self.withdrawable_cash_usd = 0.00
                self.positions = []
                self.transactions = []
                self.update_ib_message("Live connection failed")

        except Exception as e:
            self.ws_connected = False
            self.connected_account_type = None
            self.buying_power_usd = 0.00
            self.withdrawable_cash_usd = 0.00
            self.positions = []
            self.transactions = []
            self.update_ib_message(str(e))

    def disconnect_ws(self):
        print("Disconnecting...")
        self.ws_connected = False
        self.connected_account_type = None
        self.buying_power_usd = 0.00
        self.withdrawable_cash_usd = 0.00
        self.positions = []
        self.transactions = []

        self.stop_bot()
        self.stop_chart_refresh()

        self.bot_position_side = None
        self.bot_position_amount_usd = 0.0
        self.bot_position_instrument = None
        self.last_bot_signal_id = None

        self.update_ib_message("Disconnected from IB")
        self.chart_status_var.set("Connect first to load the chart")
        self.show_chart_placeholder("Connect first to load the chart")

    def toggle_connection(self):
        selected_account = self.account_type.get()

        if self.is_really_connected(selected_account):
            self.disconnect_ws()
            self.update_ui()
            self.refresh_positions_table()
            self.refresh_transactions_table()
            return

        if self.ws_connected and self.connected_account_type != selected_account:
            self.disconnect_ws()

        self.connect_to_ws(selected_account)

        if not self.is_really_connected(selected_account):
            self.update_ib_message(f"Could not connect to {selected_account}")
            self.stop_chart_refresh()
            self.chart_status_var.set("Connection required for chart")
            self.show_chart_placeholder("Connection required for chart")
        else:
            self.refresh_chart()
            self.schedule_next_chart_refresh()

        self.update_ui()
        self.refresh_positions_table()
        self.refresh_transactions_table()

    # =========================
    # Manual Trading
    # =========================
    def buy_instrument(self):
        selected_account = self.account_type.get()

        if not self.is_really_connected(selected_account):
            self.update_ib_message("Cannot buy: not connected")
            return

        instrument = self.instrument_var.get()
        amount = self.validate_amount()

        if amount is None:
            return

        if amount > self.buying_power_usd:
            self.update_ib_message("Cannot buy: insufficient buying power")
            return

        self.update_ib_message(f"Buy request sent: {instrument}, USD {amount:.2f}")
        print(f"BUY -> Instrument: {instrument}, Amount: {amount:.2f}, Account: {selected_account}")

    def sell_instrument(self):
        selected_account = self.account_type.get()

        if not self.is_really_connected(selected_account):
            self.update_ib_message("Cannot sell: not connected")
            return

        instrument = self.instrument_var.get()
        amount = self.validate_amount()

        if amount is None:
            return

        self.update_ib_message(f"Sell request sent: {instrument}, USD {amount:.2f}")
        print(f"SELL -> Instrument: {instrument}, Amount: {amount:.2f}, Account: {selected_account}")

    # =========================
    # Positions / Transactions UI
    # =========================
    def set_buying_power(self, amount):
        try:
            self.buying_power_usd = float(amount)
        except (ValueError, TypeError):
            self.buying_power_usd = 0.00
        self.update_ui()

    def set_withdrawable_cash(self, amount):
        try:
            self.withdrawable_cash_usd = float(amount)
        except (ValueError, TypeError):
            self.withdrawable_cash_usd = 0.00
        self.update_ui()

    def set_positions(self, positions_list):
        clean_positions = []

        for position in positions_list:
            instrument = position.get("instrument")
            amount = position.get("amount", 0)

            try:
                amount = float(amount)
            except (ValueError, TypeError):
                continue

            if instrument and amount != 0:
                clean_positions.append({
                    "instrument": instrument,
                    "amount": amount
                })

        self.positions = clean_positions
        self.refresh_positions_table()

    def clear_positions(self):
        self.positions = []
        self.refresh_positions_table()

    def add_transaction_record(self, side, instrument, amount, time_str=None):
        if time_str is None:
            time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.transactions.insert(0, {
            "time": time_str,
            "side": side,
            "instrument": instrument,
            "amount_usd": float(amount)
        })
        self.refresh_transactions_table()

    def clear_transactions(self):
        self.transactions = []
        self.refresh_transactions_table()

    def get_position(self, instrument):
        for position in self.positions:
            if position["instrument"] == instrument:
                return position
        return None

    def set_or_remove_position(self, instrument, signed_amount):
        updated = False

        for position in self.positions:
            if position["instrument"] == instrument:
                if signed_amount == 0:
                    self.positions.remove(position)
                else:
                    position["amount"] = signed_amount
                updated = True
                break

        if not updated and signed_amount != 0:
            self.positions.append({
                "instrument": instrument,
                "amount": signed_amount
            })

        self.refresh_positions_table()

    def refresh_positions_table(self):
        for item in self.positions_tree.get_children():
            self.positions_tree.delete(item)

        for position in self.positions:
            self.positions_tree.insert(
                "",
                "end",
                values=(
                    position["instrument"],
                    f'{position["amount"]:,.2f}'
                )
            )

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

    # =========================
    # UI State
    # =========================
    def update_ui(self):
        selected_account = self.account_type.get()
        really_connected = self.is_really_connected(selected_account)

        self.buying_power_var.set(f"{self.buying_power_usd:,.2f}")
        self.withdrawable_cash_var.set(f"{self.withdrawable_cash_usd:,.2f}")

        if really_connected:
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

    # =========================
    # Bot
    # =========================
    def toggle_bot(self):
        if self.bot_running:
            self.stop_bot()
            return

        if not self.is_any_account_connected():
            self.update_ib_message("Connect first before starting the bot")
            return

        amount = self.validate_amount()
        if amount is None:
            return

        self.bot_running = True
        self.last_bot_signal_id = None
        self.update_ui()
        self.update_ib_message("Auto-trading bot started")

        # Refresh immediately so bot can evaluate latest data
        self.refresh_chart()

    def stop_bot(self):
        self.bot_running = False
        self.update_ui()
        self.update_ib_message("Auto-trading bot stopped")

    def bot_open_long(self, instrument, amount_usd):
        if amount_usd <= 0:
            return

        print(f"BOT OPEN LONG -> {instrument}, USD {amount_usd:.2f}")

        self.bot_position_side = "LONG"
        self.bot_position_amount_usd = amount_usd
        self.bot_position_instrument = instrument

        self.set_or_remove_position(instrument, amount_usd)
        self.add_transaction_record("BOT LONG", instrument, amount_usd)

        self.update_ib_message(f"Bot opened LONG: {instrument}, USD {amount_usd:.2f}")

    def bot_open_short(self, instrument, amount_usd):
        if amount_usd <= 0:
            return

        print(f"BOT OPEN SHORT -> {instrument}, USD {amount_usd:.2f}")

        self.bot_position_side = "SHORT"
        self.bot_position_amount_usd = amount_usd
        self.bot_position_instrument = instrument

        self.set_or_remove_position(instrument, -amount_usd)
        self.add_transaction_record("BOT SHORT", instrument, amount_usd)

        self.update_ib_message(f"Bot opened SHORT: {instrument}, USD {amount_usd:.2f}")

    def bot_close_position(self, reason="BOT CLOSE"):
        if self.bot_position_side is None or self.bot_position_instrument is None:
            return

        instrument = self.bot_position_instrument
        amount_usd = self.bot_position_amount_usd
        side = self.bot_position_side

        print(f"{reason} -> Close {side} {instrument}, USD {amount_usd:.2f}")

        self.set_or_remove_position(instrument, 0)

        if side == "LONG":
            self.add_transaction_record(f"{reason} LONG", instrument, amount_usd)
        elif side == "SHORT":
            self.add_transaction_record(f"{reason} SHORT", instrument, amount_usd)

        self.bot_position_side = None
        self.bot_position_amount_usd = 0.0
        self.bot_position_instrument = None

        self.update_ib_message(f"{reason}: position closed")

    def process_bot_logic(self, instrument_label, df):
        if not self.bot_running:
            return

        if len(df) < 3:
            return

        amount_usd = self.validate_amount()
        if amount_usd is None:
            return

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

        # ----- TP/SL exit logic -----
        if self.tp_sl_var.get() == "Yes" and len(df) >= 3:
            last3 = df.tail(3)

            if self.bot_position_side == "LONG":
                all_3_below_lower = (last3["Close"] < last3["Lower"]).all()
                if all_3_below_lower:
                    signal_id = f"TP_SL_LONG_EXIT_{current_bar_time}"
                    if signal_id != self.last_bot_signal_id:
                        self.last_bot_signal_id = signal_id
                        self.bot_close_position(reason="TP/SL")
                    return

            elif self.bot_position_side == "SHORT":
                all_3_above_upper = (last3["Close"] > last3["Upper"]).all()
                if all_3_above_upper:
                    signal_id = f"TP_SL_SHORT_EXIT_{current_bar_time}"
                    if signal_id != self.last_bot_signal_id:
                        self.last_bot_signal_id = signal_id
                        self.bot_close_position(reason="TP/SL")
                    return

        # ----- Entry / reversal logic -----
        if crossed_up_from_lower:
            signal_id = f"LONG_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return

            self.last_bot_signal_id = signal_id

            if self.bot_position_side == "SHORT":
                self.bot_close_position(reason="BOT REVERSE")

            if self.bot_position_side != "LONG":
                self.bot_open_long(instrument_label, amount_usd)

        elif crossed_down_from_upper:
            signal_id = f"SHORT_ENTRY_{current_bar_time}"
            if signal_id == self.last_bot_signal_id:
                return

            self.last_bot_signal_id = signal_id

            if self.bot_position_side == "LONG":
                self.bot_close_position(reason="BOT REVERSE")

            if self.bot_position_side != "SHORT":
                self.bot_open_short(instrument_label, amount_usd)

    # =========================
    # Chart
    # =========================
    def on_instrument_change(self, event=None):
        if self.is_any_account_connected():
            self.refresh_chart()

    def schedule_next_chart_refresh(self):
        self.stop_chart_refresh()
        self.chart_after_id = self.root.after(
            self.chart_update_interval_ms,
            self.chart_refresh_cycle
        )

    def stop_chart_refresh(self):
        if self.chart_after_id is not None:
            self.root.after_cancel(self.chart_after_id)
            self.chart_after_id = None

    def chart_refresh_cycle(self):
        if self.is_any_account_connected():
            self.refresh_chart()
            self.schedule_next_chart_refresh()
        else:
            self.stop_chart_refresh()

    def refresh_chart(self):
        if not self.is_any_account_connected():
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
        self.current_chart_symbol = market_symbol
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

            # Recent data
            df = ticker.history(period="1d", interval="1m", auto_adjust=True)

            if df is None or df.empty:
                self.root.after(
                    0,
                    lambda: self.handle_chart_error(instrument_label, "No market data returned")
                )
                return

            df = df.copy()
            df = df[["Close"]].dropna()

            if df.empty:
                self.root.after(
                    0,
                    lambda: self.handle_chart_error(instrument_label, "No closing price data available")
                )
                return

            # Most recent prices only
            df = df.tail(120)

            # Bollinger Bands
            df["Middle"] = df["Close"].rolling(window=20).mean()
            df["STD20"] = df["Close"].rolling(window=20).std()
            df["Upper"] = df["Middle"] + (2 * df["STD20"])
            df["Lower"] = df["Middle"] - (2 * df["STD20"])
            df = df.dropna()

            if df.empty:
                self.root.after(
                    0,
                    lambda: self.handle_chart_error(instrument_label, "Not enough data for Bollinger Bands")
                )
                return

            self.root.after(0, lambda: self.draw_chart(instrument_label, df))

        except Exception as e:
            self.root.after(0, lambda: self.handle_chart_error(instrument_label, str(e)))

    def handle_chart_error(self, instrument_label, message):
        self.chart_loading = False
        self.ax.clear()
        self.ax.set_title(f"{instrument_label} - Price with Bollinger Bands")
        self.ax.text(
            0.5,
            0.5,
            f"Chart error:\n{message}",
            ha="center",
            va="center",
            transform=self.ax.transAxes,
            fontsize=11
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
            0.5,
            0.5,
            message,
            ha="center",
            va="center",
            transform=self.ax.transAxes,
            fontsize=12
        )
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.figure.tight_layout()
        self.canvas.draw()

    def draw_chart(self, instrument_label, df):
        self.chart_loading = False

        if instrument_label != self.instrument_var.get().strip():
            return

        if not self.is_any_account_connected():
            self.show_chart_placeholder("Connect first to load the chart")
            return

        self.ax.clear()

        self.ax.plot(df.index, df["Close"], label="Close")
        self.ax.plot(df.index, df["Upper"], label="Upper Band")
        self.ax.plot(df.index, df["Lower"], label="Lower Band")

        self.ax.fill_between(
            df.index,
            df["Lower"].values,
            df["Upper"].values,
            alpha=0.15
        )

        last_price = df["Close"].iloc[-1]

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

        # Run bot logic after chart is updated
        self.process_bot_logic(instrument_label, df)

    # =========================
    # Close
    # =========================
    def on_close(self):
        self.stop_chart_refresh()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = ConnectionApp(root)
    root.mainloop()
