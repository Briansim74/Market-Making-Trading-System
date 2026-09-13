import time
import json
import math
import heapq
import random
import requests
import threading
import websocket
import itertools
import numpy as np
import pandas as pd
from collections import deque
from sortedcontainers import SortedDict

from rich import box
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel

# # =========================
# # MARKET MICROSTRUCTURE UTILS
# # =========================

class MarketConfig:
    def __init__(self, tick_size):
        self.tick_size = tick_size

    def to_tick(self, price):
        return int(round(price / self.tick_size))

    def from_tick(self, tick):
        return tick * self.tick_size

    def round_price(self, price):
        return round(price / self.tick_size) * self.tick_size
    
    def now_ms(self):
        return int(time.time() * 1000)

    def to_ms(self, ts):
        return int(ts * 1000)
    
    def format_ms(self, ts_ms):

        return time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(ts_ms / 1000)
        )
    
    def format_ms_precise(self, ts_ms):
        ts_ms = int(ts_ms)

        base = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))
        ms = ts_ms % 1000
        return f"{base}.{ms:03d}"

# Market Data Layer
class OrderBook:
    def __init__(self, config):
        self.config = config
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.last_update_id = None
        self.lock = threading.Lock()

    def best_bid(self):
        with self.lock:
            if not self.bids:
                return (0.0, 0.0)
            return self.bids.peekitem(-1)

    def best_ask(self):
        with self.lock:
            if not self.asks:
                return (0.0, 0.0)
            return self.asks.peekitem(0)
    
    def initialize_from_binance(self, symbol, limit):

        url = (
            f"https://api.binance.com/api/v3/depth"
            f"?symbol={symbol}&limit={limit}"
        )

        snapshot = requests.get(url).json()

        self.last_update_id = snapshot["lastUpdateId"]

        bids = {
            self.config.to_tick(float(p)): float(q)
            for p, q in snapshot["bids"]
        }

        asks = {
            self.config.to_tick(float(p)): float(q)
            for p, q in snapshot["asks"]
        }

        self.bids = SortedDict(bids)
        self.asks = SortedDict(asks)

        print("ORDER BOOK INITIALIZED")

        return self.last_update_id

    def apply_delta(self, bids, asks, state=None): # Apply deltas to the order book, removing levels with zero quantity and adding/updating levels with non-zero quantity
        with self.lock:
            for p, q in bids:
                p = self.config.to_tick(float(p))
                q = float(q)

                if q == 0:
                    self.bids.pop(p, None)
                else:
                    self.bids[p] = q
    

            for p, q in asks:
                p = self.config.to_tick(float(p))
                q = float(q)

                if q == 0:
                    self.asks.pop(p, None)
                else:
                    self.asks[p] = q

    def mid(self):
        with self.lock:
            if not self.bids or not self.asks:
                return 0

            bid_tick, _ = self.bids.peekitem(-1)
            ask_tick, _ = self.asks.peekitem(0)

            return self.config.from_tick((bid_tick + ask_tick) / 2)
        
# Feed Layer
class BinanceFeed:
    def __init__(self, state, on_market_data, on_trade_event):
        self.state = state

        self.on_market_data = on_market_data
        self.on_trade_event = on_trade_event

        self.depth_socket = None
        self.trade_socket = None

        self.depth_thread = None
        self.trade_thread = None

        self.running = False
        self.depth_buffer = deque()
        self.buffering = True
        self.sync_done = False

    # def start(self):
    #     self.running = True
    #     self.buffering = True
    #     self.depth_buffer = deque()
    #     self.sync_done = False

    #     depth_socket = "wss://stream.binance.com:9443/ws/btcusdt@depth"
    #     trade_socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    #     self.depth_socket = websocket.WebSocketApp(
    #         depth_socket,
    #         on_message=self._on_depth_message
    #     )

    #     self.trade_socket = websocket.WebSocketApp(
    #         trade_socket,
    #         on_message=self._on_trade_message
    #     )

    #     # -------------------------
    #     # START STREAM FIRST
    #     # -------------------------
    #     self.depth_thread = threading.Thread(
    #         target=self.depth_socket.run_forever,
    #         daemon=True
    #     )

    #     self.trade_thread = threading.Thread(
    #         target=self.trade_socket.run_forever,
    #         daemon=True
    #     )

    #     self.depth_thread.start()
    #     self.trade_thread.start()

    #     print("SOCKETS STARTED")

    #     # -------------------------
    #     # SNAPSHOT
    #     # -------------------------
    #     last_update_id = self.state.market_book.initialize_from_binance(
    #         "BTCUSDT",
    #         1000
    #     )

    #     print("SNAPSHOT FETCHED:", last_update_id)

    #     # -------------------------
    #     # DRAIN BUFFER UNTIL SYNC FOUND
    #     # -------------------------
    #     start_time = time.time()

    #     while not self.sync_done:

    #         if time.time() - start_time > 5:
    #             print("SYNC TIMEOUT → RESTART SOCKET ONLY")
    #             self.depth_socket.close()
    #             return self.start()

    #         if not self.depth_buffer:
    #             time.sleep(0.001)
    #             continue

    #         msg = self.depth_buffer.popleft()

    #         U = msg["U"]
    #         u = msg["u"]

    #         # ignore old data
    #         if u <= last_update_id:
    #             continue

    #         # strict bridge condition
    #         if U <= last_update_id + 1 <= u:

    #             bids, asks = self._parse_book(msg)

    #             self.state.market_book.apply_delta(
    #                 bids,
    #                 asks,
    #                 self.state
    #             )

    #             self.state.market_book.last_update_id = u

    #             self.state.update_vol()

    #             self.state.compute_order_imbalance()

    #             self.sync_done = True
    #             print("BOOK SYNCHRONIZED")
    #             break

    #     # -------------------------
    #     # LIVE MODE
    #     # -------------------------
    #     self.buffering = False
    #     self.state.initialized = True

    def start(self): #100ms

        self.running = True
        self.buffering = True
        self.sync_done = False

        # IMPORTANT:
        # use LIST during buffering
        self.depth_buffer = []

        depth_socket = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
        trade_socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"

        self.depth_socket = websocket.WebSocketApp(
            depth_socket,
            on_message=self._on_depth_message
        )

        self.trade_socket = websocket.WebSocketApp(
            trade_socket,
            on_message=self._on_trade_message
        )

        # -------------------------
        # START SOCKETS
        # -------------------------
        self.depth_thread = threading.Thread(
            target=self.depth_socket.run_forever,
            daemon=True
        )

        self.trade_thread = threading.Thread(
            target=self.trade_socket.run_forever,
            daemon=True
        )

        self.depth_thread.start()
        self.trade_thread.start()

        print("SOCKETS STARTED")

        # give websocket time to buffer events
        time.sleep(1.0)

        # -------------------------
        # FETCH SNAPSHOT
        # -------------------------
        last_update_id = self.state.market_book.initialize_from_binance(
            "BTCUSDT",
            1000
        )

        print("SNAPSHOT FETCHED:", last_update_id)

        # -------------------------
        # SORT BUFFER
        # -------------------------
        self.depth_buffer.sort(key=lambda x: x["U"])

        synced = False

        # -------------------------
        # PROCESS BUFFER
        # -------------------------
        for msg in self.depth_buffer:

            U = msg["U"]
            u = msg["u"]

            print("BUFFER", U, u, last_update_id)

            # discard old events
            if u < last_update_id:
                continue

            # -------------------------
            # FIRST BRIDGE EVENT
            # -------------------------
            if not synced:

                if U <= last_update_id <= u:

                    bids, asks = self._parse_book(msg)

                    self.state.market_book.apply_delta(
                        bids,
                        asks,
                        self.state
                    )

                    self.state.market_book.last_update_id = u

                    self.state.update_vol()

                    synced = True

                    print("BOOK SYNCHRONIZED")

                continue

            # -------------------------
            # CONTINUITY CHECK
            # -------------------------
            expected = self.state.market_book.last_update_id + 1

            if U != expected:

                raise RuntimeError(
                    f"ORDER BOOK GAP DETECTED "
                    f"expected={expected} got={U}"
                )

            bids, asks = self._parse_book(msg)

            self.state.market_book.apply_delta(
                bids,
                asks,
                self.state
            )

            self.state.market_book.last_update_id = u

            self.state.update_vol()

        if not synced:
            raise RuntimeError("FAILED TO SYNCHRONIZE BOOK")

        # -------------------------
        # LIVE MODE
        # -------------------------
        self.buffering = False
        self.state.initialized = True

        print("LIVE BOOK RUNNING")

    def stop(self):

        print("STOPPING BINANCE FEED")

        self.running = False

        try:
            if self.depth_socket:
                self.depth_socket.close()

            if self.trade_socket:
                self.trade_socket.close()

        except Exception as e:
            print("Socket close error:", e)

        if self.depth_thread:
            self.depth_thread.join(timeout=2)

        if self.trade_thread:
            self.trade_thread.join(timeout=2)

        print("BINANCE FEED STOPPED")

    def _parse_book(self, message):
        bids = [(float(p), float(q)) for p, q in message.get("b", [])]
        asks = [(float(p), float(q)) for p, q in message.get("a", [])]

        return bids, asks
    
    def _parse_trade(self, message):

        return {
            "side": "SELL" if message["m"] else "BUY", # message["m"] is a boolean flag called “isBuyerMaker”, if true, SELLER is aggressive taker, if false, BUYER is aggressive taker
            "price": float(message["p"]),
            "qty": float(message["q"]),
            "timestamp": message["T"]
        }

    def _on_depth(self, message):

        state = self.state
        book = state.market_book

        U = message["U"]
        u = message["u"]
        last_id = book.last_update_id

        # -----------------------------
        # 1. FIRST VALIDATION (CRITICAL)
        # -----------------------------
        if last_id is None:
            return  # must wait for snapshot initialization

        # drop outdated updates
        if u <= last_id:
            return

        # gap detected → must resync
        if U > last_id + 1:
            print("GAP DETECTED — RESYNC REQUIRED")
            state.initialized = False
            return

        # -----------------------------
        # 2. APPLY DELTA
        # -----------------------------

        bids, asks = self._parse_book(message)

        self.state.update_queue_from_depth(
            bids=bids,
            asks=asks
        )

        book.apply_delta(bids, asks, self.state)

        book.last_update_id = u

        # -----------------------------
        # 3. VOL UPDATE
        # -----------------------------
        state.update_vol()

        state.compute_order_imbalance()

        # -----------------------------
        # 4. INITIALIZATION FIX (SEE BELOW)
        # -----------------------------
        if not state.initialized:
            return

        # -----------------------------
        # 5. STRATEGY CALL
        # -----------------------------
        if state.initialized:
            self.on_market_data()

    def _on_depth_message(self, ws, message):
        msg = json.loads(message)

        # -------------------------
        # RAW LOGGING (BEST PLACE)
        # -------------------------
        self.recorder.log_raw_event({
            "type": "depth",
            "ts": msg["E"],
            "message": message
        })

        # -------------------------
        # BUFFER DURING INIT
        # -------------------------

        if self.buffering:
            self.depth_buffer.append(msg)
            return

        # -------------------------
        # LIVE PROCESSING
        # -------------------------

        self.state.last_depth_ts = msg["E"]

        self._on_depth(msg)

    def _on_trade_message(self, ws, message):
        if not self.running:
            return
        
        msg = json.loads(message)

        # -------------------------
        # RAW LOGGING (BEST PLACE)
        # -------------------------
        self.recorder.log_raw_event({
            "type": "trade",
            "ts": msg["T"],
            "message": message
        })

        trade = self._parse_trade(msg)
        
        self.state.last_trade = trade

        # ✅ store latest exchange timestamp globally
        self.state.last_trade_ts = trade["timestamp"]

        # send to engine execution layer
        self.on_trade_event(trade)


# Strategy Layer
class MarketMakingStrategy:
    def __init__(self, config, gamma=0.1):
        self.config = config
        self.gamma = gamma
    
    """
    Strategy-level last_bid/last_ask

    Used for:

    quote stability logic (avoid churn)
    signal smoothing decisions
    “did my model change meaningfully?”

    This is decision memory
    """

    # -------------------------
    # CORE ALPHA SIGNALS
    # -------------------------
    
    def compute_fair(self, state):

        book = state.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        bid_price = self.config.from_tick(bid_tick)
        ask_price = self.config.from_tick(ask_tick)

        micro = (ask_price * bid_size + bid_price * ask_size) / (bid_size + ask_size + 1e-9)

        imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1e-9)

        flow = state.trade_imbalance

        alpha_imb = 0.2
        alpha_flow = 0.05

        return micro + alpha_imb * imbalance + alpha_flow * flow
    
    def compute_spread(self, state):

        sigma = state.get_vol()

        base = 0.03
        vol_component = 3 * sigma

        return max(base, vol_component)
    
    def compute_skew(self, state):

        sigma = state.get_vol()

        mid = state.market_book.mid()

        return -state.inventory * self.gamma * sigma * mid
    
    # -------------------------
    # FINAL QUOTE GENERATION
    # -------------------------
    
    def generate_quotes(self, state): ############## Fair Value Anchoring (not market anchoring)

        book = state.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)

        mid = (best_bid + best_ask) / 2

        # 1. Model components
        fair = self.compute_fair(state)
        skew = self.compute_skew(state)
        reservation = fair + skew

        # expected_return = model.predict(features)

        # vol = state.get_vol()

        # k = k0 / (vol + 1e-6)

        # reservation = mid + k * expected_return

        # center = reservation

        # 2. Blend market + model
        alpha = 0.3
        center = mid + alpha * (reservation - mid)

        """
        If alpha = 0
        center = mid

        → pure market making (no alpha)

        If alpha = 1
        center = reservation

        → fully model-driven pricing (aggressive alpha trading)

        If alpha = 0.2-0.4 (typical)
        80% market
        20% model

        → this is what most real MM systems effectively do
        """

        # 3. Spread (your model or fallback to market spread)
        spread = self.compute_spread(state)

        half = spread / 2

        bid = center - half
        ask = center + half

        # 4. CRITICAL: enforce market constraints (THIS is what you were missing)
        tick = self.config.tick_size

        # do not cross book
        bid = min(bid, best_bid)
        ask = max(ask, best_ask)

        # ensure minimum spread validity
        if bid >= ask:
            bid = best_bid - tick
            ask = best_ask + tick

        # 5. round to tick
        bid = self.config.round_price(bid)
        ask = self.config.round_price(ask)

        signal = {
            # market
            "mid": mid,
            "spread": best_ask - best_bid,
            "best_bid": best_bid,
            "best_ask": best_ask,

            # microstructure
            "order_imbalance": state.order_imbalance,
            "trade_imbalance": state.trade_imbalance,
            "volatility": state.get_vol(),
            "queue_ahead_bid": state.compute_queue_ahead("bids", best_bid),
            "queue_ahead_ask": state.compute_queue_ahead("asks", best_ask),

            # risk
            "inventory": state.inventory,
            "realized_pnl": state.realized_pnl,
            "unrealized_pnl": state.get_unrealized_pnl(mid),
            "total_pnl": state.get_pnl(),
            "equity": state.cash + state.inventory * mid,

            # model internals
            "fair": fair,
            "skew": skew,
            "reservation": reservation,
            "alpha": alpha,

            # action
            "my_bid": bid,
            "my_ask": ask
        }

        return signal

    # def generate_quotes(self, state): ############## Market Anchoring (not fair value anchoring)

    #     book = state.market_book

    #     bid_tick, bid_size = book.best_bid()
    #     ask_tick, ask_size = book.best_ask()

    #     bid = self.config.from_tick(bid_tick)
    #     ask = self.config.from_tick(ask_tick)

    #     spread = ask - bid

    #     # inventory skew (keep your logic but smaller weight)
    #     #skew = -state.inventory * self.gamma * 0.001
    #     skew = self.compute_skew(state)

    #     # anchor to market, not fair
    #     bid_quote = bid + skew
    #     ask_quote = ask + skew

    #     return {
    #         "my_bid": bid_quote,
    #         "my_ask": ask_quote,
    #         "spread": spread
    #     }

    #     return signal
    
    # -------------------------
    # MAIN ENTRY POINT
    # -------------------------
    
    def on_market_update(self, state):

        """
        Pure function:
        - no memory
        - no throttling
        - no execution awareness
        """

        return self.generate_quotes(state)
    

# Dataset Recording Layer
# For feature engineering (for later ML model training)
class DatasetRecorder:
    def __init__(self, config):
        self.config = config
        
        self.rows = []
        self.trades = []
        self.quotes = []
        self.fills = []

        # for replayengine
        self.raw_events = [] # or buffer for parquet

    def log_raw_event(self, event):
        """
        Stores raw exchange event BEFORE any processing.
        This is your replay ground truth.
        """

        self.raw_events.append({
            "type": event["type"],
            "ts": event["ts"],
            "message": event["message"]
        })

    """
    log_snapshot = “state + decision dataset” (MOST IMPORTANT for ML)

    You already understood this one correctly.

    What it represents:

    What the market looked like + what you decided to quote

    Contains:
    best bid/ask
    mid
    volatility
    inventory
    my_bid / my_ask
    interaction features (distance_to_touch, spread distance)

    Used for:
    fill probability models
    quoting policy learning
    RL / imitation learning
    adverse selection prediction

    This is your training dataset backbone
    
    “I wanted to quote X at time t”
    """

    def log_snapshot(self, ts, features, execution, symbol="BTCUSDT"):

        bid_delta = 0.0 if execution.last_bid is None else features["my_bid"] - execution.last_bid
        ask_delta = 0.0 if execution.last_ask is None else features["my_ask"] - execution.last_ask

        self.rows.append({
            "ts": ts,
            "symbol": symbol,

            # market
            "best_bid": features["best_bid"],
            "best_ask": features["best_ask"],
            "mid": features["mid"],
            
            "best_bid_tick": self.config.to_tick(features["best_bid"]),
            "best_ask_tick": self.config.to_tick(features["best_ask"]),
            "mid_tick": self.config.to_tick(features["mid"]),

            "spread": features["best_ask"] - features["best_bid"],

            # microstructure
            "order_imbalance": features["order_imbalance"],
            "trade_imbalance": features["trade_imbalance"],
            "volatility": features["volatility"],
            "queue_ahead_bid": features["queue_ahead_bid"],
            "queue_ahead_ask": features["queue_ahead_ask"],

            # risk
            "inventory": features["inventory"],
            "realized_pnl": features["realized_pnl"],
            "unrealized_pnl": features["unrealized_pnl"],
            "total_pnl": features["total_pnl"],
            "equity": features["equity"],

            # model internals
            "fair": features["fair"],
            "skew": features["skew"],
            "reservation": features["reservation"],
            "alpha": features["alpha"],

            # action
            "my_bid": features["my_bid"],
            "my_ask": features["my_ask"],

            "my_bid_tick": self.config.to_tick(features["my_bid"]),
            "my_ask_tick": self.config.to_tick(features["my_ask"]),

            # execution geometry
            # aggressiveness vs touch
            "bid_distance_touch": features["my_bid"] - features["best_bid"],
            "ask_distance_touch": features["my_ask"] - features["best_ask"],

            # position inside spread
            "bid_distance_spread": features["my_bid"] - features["best_ask"],
            "ask_distance_spread": features["my_ask"] - features["best_bid"],

            # quote churn
            "bid_delta": bid_delta,
            "ask_delta": ask_delta,
            "quote_churn": abs(bid_delta) + abs(ask_delta),
        })

    """
    log_trade = “external market ground truth”
    What it represents:

    “What the market did, independent of me”

    Contains:
    aggressor side (BUY/SELL)
    price
    size
    timestamp
    Used for:
    A) Flow imbalance signals

    You already use this in:
    trade_imbalance

    B) Toxicity labeling
    did price move against me after aggressive flow?

    C) Market impact models
    how trades move mid price

    D) Alpha signals
    order flow prediction

    Key point:

    Trades are exogenous — they are NOT your actions.

    """

    def log_trade(self, trade, book, symbol="BTCUSDT"):

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)
        mid = (best_bid + best_ask) / 2

        microprice = (
            best_ask * bid_size + best_bid * ask_size
        ) / (bid_size + ask_size + 1e-9)

        price = trade["price"]
        qty = trade["qty"]
        notional = price * qty

        self.trades.append({
            "ts": trade["timestamp"],
            "symbol": symbol,

            # raw trade
            "price": price,
            "price_tick": self.config.to_tick(trade["price"]),
            "qty": qty,
            "side": trade["side"],
            "is_buyer_maker": True if trade["side"] == "SELL" else False,

            # market context
            "mid": mid,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": best_ask - best_bid,
            "microprice": microprice,

            # trade position in market
            "trade_to_mid": price - mid,
            "trade_to_microprice": price - microprice,
            "price_to_best_bid": price - best_bid,
            "price_to_best_ask": price - best_ask,

            # liquidity context
            "bid_size": bid_size,
            "ask_size": ask_size,

            # regime features
            "trade_side": "SELL_AGGRESSOR" if trade["side"] == "SELL" else "BUY_AGGRESSOR",
            "trade_sign": -1 if trade["side"] == "SELL" else 1, # -1 for aggressive sell order else 1

            # NEW FEATURES
            "notional": notional,
            "log_notional": np.log1p(notional),
            "intensity": qty / (bid_size + ask_size + 1e-9),
        })

    """
    log_quote = “order lifecycle + microstructure behavior”

    This is often misunderstood.

    What it represents:

    “I attempted to place/modify/cancel liquidity at time t”

    NOT whether it filled.

    Contains:
    price
    side
    size
    timestamp
    maybe cancel/replace info
    maybe queue position estimate

    What it is used for:

    A) Microstructure behavior analysis
    quote churn rate
    how often you reprice
    latency sensitivity

    B) Queue position modeling
    how often you improve queue
    how often you get stale

    C) Strategy diagnostics
    “am I over-trading quotes?”
    Important insight:

    D) Fill probability modeling
    P(fill | quote geometry, volatility, queue state)

    log_quote is about intent and action frequency, NOT outcome.

    “I actually placed/cancelled/replaced X in the book at time t”
    """

    def log_quote(self, ts, side, event_type, price, size, trade_imbalance, queue_ahead_at_join, inventory, volatility, features):
        self.quotes.append({
            # Event
            "ts": ts,
            "side": side,
            "event_type": event_type,

            # Quote
            "price": price,
            "price_tick": self.config.to_tick(price),
            "size": size,

            # Market State
            "mid": features["mid"],
            "spread": features["spread"],
            "best_bid": features["best_bid"],
            "best_ask": features["best_ask"],
            "order_imbalance": features["order_imbalance"],
            "trade_imbalance": trade_imbalance,


            # Quote Geometry
            "distance_to_mid": price - features["mid"],
            "distance_to_touch": (
                price - features["best_bid"]
                if side == "BUY"
                else price - features["best_ask"]
            ),

            # Execution / Microstructure
            "queue_ahead_at_join": queue_ahead_at_join,

            # Risk
            "inventory": inventory,
            "volatility": volatility,

            # Model Internals
            "fair": features["fair"],
            "skew": features["skew"],
            "reservation": features["reservation"],
            "alpha": features["alpha"]
        })

    """
    log_fill = “your execution outcome”

    What it represents:

    “Did I get filled, at what price, and what happened to me?”

    Contains:
    fill price
    size
    side
    inventory after fill
    realized pnl impact
    Used for:
    A) PnL attribution
    spread capture
    adverse selection loss
    B) Fill probability training labels
    did quote lead to execution or not
    C) Execution quality metrics

    """

    def log_fill(self, ts, qty, order, inventory, volatility, best_bid, best_ask, features, is_maker):

        my_bid = order.metadata["my_bid"]
        my_ask = order.metadata["my_ask"]

        self.fills.append({
            # time
            "ts": ts,

            # execution
            "side": order.side,
            "price": self.config.from_tick(order.price),
            "price_tick": order.price,
            "qty": qty,

            # execution classification
            "is_maker": is_maker,
            "fill_type": "BID_HIT" if order.side == "BUY" else "ASK_LIFT",
            "fill_status": "PARTIAL" if order.remaining > 0 else "FULL",

            # portfolio state
            "inventory": inventory,

            # market state at fill
            "mid": (best_bid + best_ask) / 2,
            "spread": best_ask - best_bid,
            "volatility": volatility,
            "volatility_bps": volatility * 10000,

            # quote geometry
            "my_bid": my_bid,
            "my_ask": my_ask,
            "bid_distance_touch": my_bid - best_bid,
            "ask_distance_touch": my_ask - best_ask,
            "bid_distance_spread": my_bid - best_ask,
            "ask_distance_spread": my_ask - best_bid,

            # microstructure
            "queue_ahead_at_join": order.metadata["queue_ahead_at_join"],

            # model state
            "fair": features["fair"],
            "skew": features["skew"],
            "reservation": features["fair"] + features["skew"],
            "alpha": features["alpha"]
    })

    def finalize_returns(self, horizon=10):
        """
        This is a classic supervised learning target in trading datasets.

        It's used to train models to predict:

        short-term price direction
        expected move size
        volatility-adjusted returns
        market making signals (spread widening/narrowing)
        Important interpretation

        This is NOT:

        trade-level outcome
        execution PnL
        fill quality

        It is:

        “pure market movement from this snapshot forward”

        So it's a market-only label, independent of your trades.
        """

        for i in range(len(self.rows) - horizon):
            mid_now = self.rows[i]["mid"]
            mid_future = self.rows[i + horizon]["mid"]

            self.rows[i]["future_return"] = (
                mid_future - mid_now
            ) / mid_now

    def add_trade_impact_labels(self, horizon=10):
        """
        Adds future return label to each trade
        """
        for i in range(len(self.trades) - horizon):
            p_now = self.trades[i]["price"]
            p_future = self.trades[i + horizon]["price"]

            self.trades[i]["future_return"] = (p_future - p_now) / p_now

    def label_toxicity(self, horizon=5):
        """
        Marks trades as toxic if future price moves against the trade direction,
        i.e. measures adverse future price movement relative to aggressor side

        “toxic = trade was followed by adverse price movement relative to its direction”

        Labels trades based on ex-post directional correctness of order flow
        (negative = uninformed / adverse movement)
        """
        for i in range(len(self.trades) - horizon):

            t = self.trades[i]

            p_now = t["price"]
            p_future = self.trades[i + horizon]["price"]

            if t["side"] == "BUY":
                move = p_future - p_now
            else:
                move = p_now - p_future

            t["signed_impact"] = move
            t["signed_impact_bps"] = 10000 * move / p_now

            t["toxic"] = move < 0

    def align_events(self):

        """
        Efficient event alignment:
        O(n + m) instead of O(n²)
        """

        snapshots = self.rows
        trades = self.trades

        j = 0
        n = len(snapshots)

        for trade in trades:
            ts = trade["ts"]

            while j < n - 1 and snapshots[j]["ts"] < ts:
                j += 1

            if j == 0:
                closest = snapshots[0]
            else:
                prev = snapshots[j - 1]
                curr = snapshots[j]

                closest = prev if abs(prev["ts"] - ts) <= abs(curr["ts"] - ts) else curr

            trade["snapshot_mid"] = closest["mid"]
            trade["snapshot_best_bid"] = closest["best_bid"]
            trade["snapshot_best_ask"] = closest["best_ask"]
            trade["snapshot_spread"] = closest["best_ask"] - closest["best_bid"]

    def label_adverse_selection(self, horizon=5):

        self.quotes.sort(key=lambda x: x["ts"])
        self.fills.sort(key=lambda x: x["ts"])

        j = 0
        n = len(self.quotes)

        for f in self.fills:

            ts = f["ts"]

            # move forward once
            while j < n and self.quotes[j]["ts"] <= ts:
                j += 1

            # pick future horizon quote safely
            idx = min(j + horizon, n - 1)

            if idx <= 0 or idx >= n:
                continue

            q = self.quotes[idx]

            if f["side"] == "BUY":
                f["adverse_selection"] = q["price"] - f["price"]
            else:
                f["adverse_selection"] = f["price"] - q["price"]

    def label_fill_markouts(self, horizon_list=(1000, 5000)):

        self.fills.sort(key=lambda x: x["ts"])
        self.rows.sort(key=lambda x: x["ts"])

        j = 0
        n = len(self.rows)

        for f in self.fills:

            ts = f["ts"]
            fill_price = f["price"]

            while j < n and self.rows[j]["ts"] <= ts:
                j += 1

            for h in horizon_list:

                idx = min(j + h, n - 1)
                future_mid = self.rows[idx]["mid"]

                f[f"markout_{h}ms"] = future_mid - fill_price

    def export_raw_events_parquet(self, path="raw_events.parquet"):
        df = pd.DataFrame(self.raw_events)
        df.to_parquet(path, index=False)

    def export_parquet(self,
                    snapshot_path="snapshots.parquet",
                    trades_path="trades.parquet",
                    quotes_path="quotes.parquet",
                    fills_path="fills.parquet"):

        pd.DataFrame(self.rows).to_parquet(snapshot_path, index=False)
        pd.DataFrame(self.trades).to_parquet(trades_path, index=False)
        pd.DataFrame(self.quotes).to_parquet(quotes_path, index=False)
        pd.DataFrame(self.fills).to_parquet(fills_path, index=False)

# Execution Layer
class Order:
    def __init__(self, order_id, side, price, qty, ts, owner, signal, queue_ahead_at_join):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.qty = qty
        self.remaining = qty
        self.status = "LIVE"
        self.timestamp = ts
        self.owner = owner
        self.metadata = {
            "my_bid": signal["my_bid"],
            "my_ask": signal["my_ask"],
            "fair": signal["fair"],
            "skew": signal["skew"],
            "reservation": signal["reservation"],
            "alpha": signal["alpha"],
            "queue_ahead_at_join": queue_ahead_at_join
        }


class ExecutionEngine:
    def __init__(self, config, state, recorder, weights_path="fill_model_weights.json"):
        self.config = config
        self.state = state

        self.open_orders = {}  # order_id -> Order
        self.id_counter = itertools.count()

        self.last_bid = None
        self.last_ask = None
        self.current_bid_size = 0.0
        self.current_ask_size = 0.0

        # -------------------------
        # latency model (ms)

        # priority queue: (execute_ts, action)
        # -------------------------
        self.latency_ms = 50
        self.latency_queue = []

        self.recorder = recorder

        # with open(weights_path, "r") as f: #### OPEN THIS
        #     self.weights = json.load(f)

    """
    Execution-level last_bid/last_ask

    Used for:

    order state reconciliation
    cancel/replace minimization
    exchange action control

    This is order state memory
    """
    
    def _new_order_id(self):
        return next(self.id_counter)
    

    # =========================================================
    # PUBLIC API (called by Engine)
    # =========================================================

    def process_place_quotes(self, signal):
        """
        Called by Engine.
        DOES NOT execute immediately.
        Instead schedules execution after latency.
        """

        execute_ts = time.time() * 1000 + self.latency_ms

        heapq.heappush(
            self.latency_queue,
            (execute_ts, ("PLACE_QUOTES", signal))
        )

    # =========================================================
    # CALLED EVERY TICK (Engine.execution_cycle)
    # =========================================================

    def process_latency_queue(self):

        now = time.time() * 1000

        while self.latency_queue and self.latency_queue[0][0] <= now:

            execute_ts, (action_type, signal) = heapq.heappop(self.latency_queue)

            if action_type == "PLACE_QUOTES":
                self.place_quotes(signal)
    
    # -------------------------
    # ORDER MANAGEMENT
    # -------------------------

    """
    Now your MM behaves like:

    High volatility → smaller size
    avoids getting chopped up

    Low volatility → larger size
    captures spread more aggressively

    High inventory → reduces exposure
    natural risk balancing
    """

    # def _compute_order_size(self): # Symmetrical Quoting
    #     state = self.state

    #     inv = state.inventory
    #     vol = state.get_vol()

    #     base_size = 1.0

    #     # 1. inventory penalty (risk control)
    #     inv_penalty = math.exp(-0.5 * (inv ** 2))

    #     # 2. volatility scaling (trade smaller in high vol)
    #     vol_penalty = 1 / (1 + 50 * vol)

    #     size = base_size * inv_penalty * vol_penalty

    #     # clamp to avoid extreme values
    #     return max(0.1, min(size, 2.0))

    def _compute_order_size(self): # Asymmetrical Quoting
        state = self.state

        inv = state.inventory
        vol = state.get_vol()

        base_size = 1.0

        # -------------------------
        # 1. volatility scaling
        # -------------------------
        vol_penalty = 1 / (1 + 50 * vol)

        # -------------------------
        # 2. asymmetric inventory pressure
        # -------------------------
        inv_scale = 5.0  # sensitivity knob

        # bounded asymmetry in [-1, 1]
        inv_signal = math.tanh(inv / inv_scale)

        # convert to directional size multiplier
        # long inventory → sell bias → reduce bid size more than ask
        bid_multiplier = math.exp(-inv_signal)
        ask_multiplier = math.exp(inv_signal)

        # -------------------------
        # 3. convex risk aversion (kept but softened)
        # -------------------------
        risk_penalty = math.exp(-0.2 * (inv ** 2))

        # -------------------------
        # 4. combine
        # -------------------------
        base = base_size * vol_penalty * risk_penalty

        # optional: keep symmetric size baseline but asymmetric quoting power
        size = base

        # clamp
        size = max(0.05, min(size, 2.0))

        bid_size = size * bid_multiplier
        ask_size = size * ask_multiplier

        order_sizes = {
            "bid_size": bid_size,
            "ask_size": ask_size
        }

        return order_sizes
    
    def place_quotes(self, signal):
        """
        Selective repricing logic:
        - preserve queue priority when possible
        - only cancel when necessary
        """

        bid = signal["my_bid"]
        ask = signal["my_ask"]

        order_sizes = self._compute_order_size()
        ts = self.config.now_ms()

        # -------------------------
        # FIRST TIME PLACING
        # -------------------------
        if self.last_bid is None or self.last_ask is None:
            bid_order = self._place_limit(side="BUY", price=bid, size=order_sizes["bid_size"], ts=ts, signal=signal)
            ask_order = self._place_limit(side="SELL", price=ask, size=order_sizes["ask_size"], ts=ts, signal=signal)

            self.last_bid = bid
            self.last_ask = ask

            self.current_bid_size = order_sizes["bid_size"]
            self.current_ask_size = order_sizes["ask_size"]

            self.recorder.log_quote(
                ts=ts,
                side="BID",
                event_type="NEW",
                price=bid,
                size=order_sizes["bid_size"],
                trade_imbalance=self.state.trade_imbalance,
                queue_ahead_at_join=bid_order.metadata["queue_ahead_at_join"],
                inventory=self.state.inventory,
                volatility=self.state.get_vol(),
                features=signal
            )

            self.recorder.log_quote(
                ts=ts,
                side="ASK",
                event_type="NEW",
                price=ask,
                size=order_sizes["ask_size"],
                trade_imbalance=self.state.trade_imbalance,
                queue_ahead_at_join=ask_order.metadata["queue_ahead_at_join"],
                inventory=self.state.inventory,
                volatility=self.state.get_vol(),
                features=signal
            )

            return

        tick = self.config.tick_size

        # -------------------------
        # DETERMINE IF WE NEED TO UPDATE EACH SIDE
        # -------------------------
        # bid_change = abs(bid - self.last_bid) > tick more than 1 tick move
        # ask_change = abs(ask - self.last_ask) > tick

        bid_change = abs(bid - self.last_bid) >= tick # bid might move 1 tick also, its possible
        ask_change = abs(ask - self.last_ask) >= tick

        # ---------------------------------------------------------------------------
        # CANCEL ONLY WHAT IS NECESSARY, REPLACE ONLY STALE SIDE(S), UPDATE STATE
        # ---------------------------------------------------------------------------
        if bid_change:
            self._cancel_side("BUY")

            bid_order = self._place_limit(side="BUY", price=bid, size=order_sizes["bid_size"], ts=ts, signal=signal)

            self.last_bid = bid

            self.recorder.log_quote(
                ts=ts,
                side="BID",
                event_type="NEW",
                price=bid,
                size=order_sizes["bid_size"],
                trade_imbalance=self.state.trade_imbalance,
                queue_ahead_at_join=bid_order.metadata["queue_ahead_at_join"],
                inventory=self.state.inventory,
                volatility=self.state.get_vol(),
                features=signal
            )

        if ask_change:
            self._cancel_side("SELL")

            ask_order = self._place_limit(side="SELL", price=ask, size=order_sizes["ask_size"], ts=ts, signal=signal)

            self.last_ask = ask

            self.recorder.log_quote(
                ts=ts,
                side="ASK",
                event_type="NEW",
                price=ask,
                size=order_sizes["ask_size"],
                trade_imbalance=self.state.trade_imbalance,
                queue_ahead_at_join=ask_order.metadata["queue_ahead_at_join"],
                inventory=self.state.inventory,
                volatility=self.state.get_vol(),
                features=signal
            )

        self.current_bid_size = order_sizes["bid_size"]
        self.current_ask_size = order_sizes["ask_size"]

    # def place_quotes(self, bid, ask):
    #     """
    #     Selective repricing logic:
    #     - preserve queue priority when possible
    #     - only cancel when necessary
    #     """

    #     bid = self.config.round_price(bid)
    #     ask = self.config.round_price(ask)

    #     size = self._compute_order_size()
    #     ts = time.time()

    #     # -------------------------
    #     # FIRST TIME PLACING
    #     # -------------------------
    #     if self.last_bid is None or self.last_ask is None:
    #         self._place_limit("BUY", bid)
    #         self._place_limit("SELL", ask)

    #         self.last_bid = bid
    #         self.last_ask = ask
    #         self.current_size = size
    #         return

    #     tick = self.config.tick_size

    #     # -------------------------
    #     # DETERMINE IF WE NEED TO UPDATE EACH SIDE
    #     # -------------------------
    #     bid_change = abs(bid - self.last_bid) > tick
    #     ask_change = abs(ask - self.last_ask) > tick

    #     # bid_change = abs(bid - self.last_bid) >= tick # bid might move 1 tick also, its possible
    #     # ask_change = abs(ask - self.last_ask) >= tick

    #     # -------------------------
    #     # LOG QUOTES (always log intent)
    #     # -------------------------

    #     bid_tick, bid_size = self.state.market_book.best_bid()
    #     ask_tick, ask_size = self.state.market_book.best_ask()

    #     best_bid = self.config.from_tick(bid_tick)
    #     best_ask = self.config.from_tick(ask_tick)

    #     mid = (best_bid + best_ask) / 2

    #     if bid_change:
    #         self.recorder.log_quote(
    #             ts=ts,
    #             side="BID",
    #             price=bid,
    #             size=size,
    #             inventory=self.state.inventory,
    #             best_bid=best_bid,
    #             best_ask=best_ask,
    #             volatility=self.state.get_vol(),
    #             fair=self.compute_fair(self.state),
    #             skew=self.compute_skew(self.state)
    #         )

    #     if ask_change:
    #         self.recorder.log_quote(
    #             ts=ts,
    #             side="ASK",
    #             price=ask,
    #             size=size,
    #             inventory=self.state.inventory,
    #             best_bid=best_bid,
    #             best_ask=best_ask,
    #             volatility=self.state.get_vol(),
    #             fair=self.compute_fair(self.state),
    #             skew=self.compute_skew(self.state)
    #         )

    #     # -------------------------
    #     # CANCEL ONLY WHAT IS NECESSARY
    #     # -------------------------
    #     if bid_change:
    #         self._cancel_side("BUY")

    #     if ask_change:
    #         self._cancel_side("SELL")

    #     # -------------------------
    #     # REPLACE ONLY STALE SIDE(S)
    #     # -------------------------
    #     if bid_change:
    #         self._place_limit("BUY", bid)

    #     if ask_change:
    #         self._place_limit("SELL", ask)

    #     # -------------------------
    #     # UPDATE STATE
    #     # -------------------------
    #     if bid_change:
    #         self.last_bid = bid

    #     if ask_change:
    #         self.last_ask = ask

    #     self.current_size = size

    def _place_limit(self, side, price, size, ts, signal):
        order_id = self._new_order_id()

        tick_price = self.config.to_tick(price)

        # 🔥 capture current book depth BEFORE your order joins
        book_side = (
            self.state.market_book.bids
            if side == "BUY"
            else self.state.market_book.asks
        )

        book_size = book_side.get(tick_price, 0.0)

        # 🔥 record estimated queue position
        self.state.my_queue_position["bids" if side == "BUY" else "asks"][tick_price] = book_size

        order = Order(
            order_id=order_id,
            side=side,
            price=tick_price,
            qty=size,
            ts=ts,
            owner="self",
            signal=signal,
            queue_ahead_at_join=book_size
        )

        self.open_orders[order_id] = order

        return order

    def _cancel_side(self, side):
        for oid, order in list(self.open_orders.items()):
            if order.side == side:
                order.status = "CANCELED"

                self.state.last_order_update = order

                del self.open_orders[oid]

    # -------------------------
    # MARKET EVENTS (FILL LOGIC)
    # -------------------------

    def process_trade(self, trade):
        
        self._update_trade_flow(trade)

        self._process_fills(trade)

    def _update_trade_flow(self, trade):
        side = trade["side"]
        flow = 1 if side == "BUY" else -1

        alpha_flow = 0.2
        self.state.trade_imbalance = (
            alpha_flow * flow + (1 - alpha_flow) * self.state.trade_imbalance
        )

    # -------------------------
    # FILL ENGINE (simplified but correct directionally)
    # -------------------------
    
    def _sigmoid(self, x):
        return 1 / (1 + math.exp(-x))
    
    def _fill_probability(self, features, w):
        """
        features: dict or vector of market state
        w: learned weights
        """

        x = (
            w["queue"] * features["queue_ahead"] +
            w["flow"] * features["liquidity_flow"] +
            w["vol"] * features["volatility"] +
            w["spread"] * features["spread"] +
            w["inv"] * features.get("inventory", 0.0) +
            w["bias"]
        )

        return self._sigmoid(x)

    def _process_fills(self, trade):
        
        # 🔥 convert incoming trade price into tick domain
        price = self.config.to_tick(trade["price"])
        qty = trade["qty"]

        # BUY trade -> aggressor buys -> hits your ASK
        if trade["side"] == "BUY":
            self._match_side("SELL", price, qty)

        # SELL trade -> aggressor sells -> hits your BID
        else:
            self._match_side("BUY", price, qty)

    def _match_side(self, side, price, qty):

        orders = [
            o for o in self.open_orders.values()
            if o.side == side and o.price == price and o.status == "LIVE"
        ]

        self.state.fill_candidates = orders

        queue_ahead = self.state.compute_queue_ahead(
            "bids" if side == "BUY" else "asks",
            price
        )

        # features = { # for ML fill model ####################################################
        #     "queue_ahead": queue_ahead,
        #     "liquidity_flow": self.state.liquidity_flow,
        #     "volatility": self.state.get_vol(),
        #     "spread": self.state.compute_spread(),
        #     "inventory": self.state.inventory
        # }

        # # THIS is your line
        # p_fill = self._fill_probability(features, self.weights)

        # threshold = 0.5  # or tune later

        # if p_fill < threshold:
        #     return  # no fill happens

        # if qty <= queue_ahead:
        #     return  # you are not reached in the queue

        # remaining_flow = qty

        # BASIC FILL MODEL (queue-based) — REPLACE WITH ML MODEL LOGIC LATER
        # STEP 1: determine if incoming flow is sufficient
        # to reach YOUR queue position
        # ----------------------------------------------------

        # if queue_ahead > 0: # If I still estimate liquidity ahead of me, I cannot be filled yet.
        #     return
        
        pressure = qty / (queue_ahead + qty + 1e-9) # Stochastic partial fills
        p_fill = pressure

        if p_fill < random.random():
            return
        
        # This creates a value between:

        # 0 → almost no chance of fill
        # 1 → almost guaranteed fill

        remaining_flow = qty

        # ----------------------------------------------------
        # STEP 2: apply fills to your orders
        # ----------------------------------------------------

        for order in orders:
            if remaining_flow <= 0:
                break

            fill = min(order.remaining, remaining_flow)

            order.remaining -= fill
            remaining_flow -= fill

            self.state.on_fill(self.config.from_tick(order.price), fill, order.side)

            self.state.last_order_update = order

            # RECORDER

            bid_tick, bid_size = self.state.market_book.best_bid()
            ask_tick, ask_size = self.state.market_book.best_ask()

            best_bid = self.config.from_tick(bid_tick)
            best_ask = self.config.from_tick(ask_tick)

            self.recorder.log_fill(
                ts=self.state.last_trade_ts,
                qty=fill,
                order=order,
                inventory=self.state.inventory,
                volatility=self.state.get_vol(),
                best_bid=best_bid,
                best_ask=best_ask,
                features=self.state.last_signal,
                is_maker=True
            )

            if order.remaining == 0:
                order.status = "FILLED"
                del self.open_orders[order.order_id]

# Orchestration Layer
class Engine:
    def __init__(self, config, state, strategy, execution, recorder):
        self.config = config
        self.state = state
        self.strategy = strategy
        self.execution = execution
        self.recorder = recorder
        self.feed = None

        self.console = Console()
        self.live = None
        self.last_dashboard_update = 0

    def on_market_data(self):

        # HARD SAFETY: do not log or trade before sync
        if not self.state.initialized:
            return
    
        signal = self.strategy.on_market_update(self.state)

        if signal is None:
            return

        # my_bid = signal["my_bid"]
        # my_ask = signal["my_ask"]

        # store intent ONLY (no execution)
        self.state.last_signal = signal

        self.recorder.log_snapshot(
            ts=self.state.last_depth_ts,
            features=signal,
            execution=self.execution,
            symbol="BTCUSDT"
        )

    def on_trade_event(self, trade):

        # Dataset recording
        self.recorder.log_trade(trade=trade, 
                                book=self.state.market_book, 
                                symbol="BTCUSDT")

        self.execution.process_trade(trade)

    def execution_cycle(self):

        if not self.state.initialized:
            return

        signal = self.state.last_signal

        if signal is None:
            return

        self.execution.place_quotes(signal)

        # self.execution.process_place_quotes(signal) # for latency simulation
    
    def color_pnl(self, value):

        if value > 0:
            return f"[green]▲ {value:.4f}[/green]"
        
        elif value < 0:
            return f"[red]▼ {value:.4f}[/red]"
        
        else:
            return f"— {value:.4f}"
        
    def color_risk(self, inventory, limit=10):
        
        intensity = min(abs(inventory) / limit, 1.0)

        if intensity < 0.3:
            return f"[green] {inventory:.4f}[/green]"
        
        elif intensity < 0.7:
            return f"[yellow]▲ {inventory:.4f}[/yellow]"
        
        else:
            return f"[red]▲ {inventory:.4f}[/red]"
    
    def start_dashboard(self):
        self.live = Live(
            self.render_dashboard(),
            refresh_per_second=60,
            transient=False
        )
        self.live.start()

    def render_dashboard(self):

        state = self.state
        book = state.market_book
        execution = self.execution
        strategy = self.strategy

        # -----------------------------
        # SNAPSHOT
        # -----------------------------

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        mid = (self.config.from_tick(bid_tick) + self.config.from_tick(ask_tick)) / 2
        fair = strategy.compute_fair(state)
        skew = strategy.compute_skew(state)
        reservation = fair + skew

        alpha = state.last_signal["alpha"] if state.last_signal is not None else 0.0

        pnl_pct = state.get_pnl() / state.initial_cash * 100
        pnl_pct_sign = "+" if pnl_pct > 0 else ""

        snapshot = {
            "bid": self.config.from_tick(bid_tick),
            "ask": self.config.from_tick(ask_tick),

            "inventory": state.inventory,
            "realized_pnl": state.realized_pnl,
            "unrealized_pnl": state.get_unrealized_pnl(mid),
            "total_pnl": state.get_pnl(),

            "ewma_vol": state.get_vol(),
            "order_imbalance": state.order_imbalance,
            "trade_imbalance": state.trade_imbalance,
            "trade": state.last_trade,

            "fair_value": fair,
            "skew": skew,
            "reservation": reservation,
            "alpha": alpha,

            "my_bid": execution.last_bid or 0.0,
            "my_ask": execution.last_ask or 0.0,
            "current_bid_size": execution.current_bid_size,
            "current_ask_size": execution.current_ask_size,
            "orders": execution.open_orders.values(),
            "fill_candidates": state.fill_candidates,
            "last_order_update": state.last_order_update,

            "last_trade_ts": state.last_trade_ts or 0.0,
            "last_depth_ts": state.last_depth_ts or 0.0
        }

        # -------------------------
        # FIXED WIDTH TABLE
        # -------------------------

        table = Table(
            title=f"mm-core | LIVE | binance | btcusdt | pnl={pnl_pct_sign}{pnl_pct:.4f}%",
            box=box.SQUARE,
            expand=False
        )

        table.add_column(
            "Metric",
            style="bold cyan",
            width=20,
            no_wrap=True
        )

        table.add_column(
            "Value",
            style="white",
            width=60,
            no_wrap=True
        )

        # -------------------------
        # MARKET
        # -------------------------

        table.add_row(
            "[bold yellow]MARKET[/bold yellow]",
            ""
        )

        spread = snapshot["ask"] - snapshot["bid"]

        table.add_row(
            "Mid",
            f"{mid:<15.4f}"
        )

        table.add_row(
            "Spread",
            f"{spread:<15.4f}"
        )

        table.add_row(
            "Best Bid / Size",
            f"{snapshot["bid"]:<10.4f} ({bid_size:<6.4f})"
        )

        table.add_row(
            "Best Ask / Size",
            f"{snapshot["ask"]:<10.4f} ({ask_size:<6.4f})"
        )

        table.add_row(
            "EWMA Vol",
            f"{snapshot["ewma_vol"]:.2e}"
        )

        table.add_row(
            "Order Imbalance",
            f"{snapshot["order_imbalance"]:<15.4f}"
        )

        table.add_row(
            "Trade Imbalance",
            f"{snapshot["trade_imbalance"]:<15.4f}"
        )

        trade = snapshot["trade"]

        trade_side = str(trade.get("side", "—"))
        trade_price = float(trade.get("price", 0))
        trade_qty = float(trade.get("qty", 0))

        trade_str = f"{trade_side:<5} | {trade_price:>10.4f} | {trade_qty:>8.6f}"

        table.add_row(
            "Last Trade",
            trade_str
        )

        table.add_row("", "")

        # -------------------------
        # SIGNALS
        # -------------------------

        table.add_row(
            "[bold yellow]SIGNALS[/bold yellow]",
            ""
        )

        table.add_row(
            "Fair Value",
            f"{snapshot["fair_value"]:<15.4f}"
        )

        table.add_row(
            "Inventory Skew",
            f"{snapshot["skew"]:<15.4f}"
        )

        table.add_row(
            "Reservation",
            f"{snapshot["reservation"]:<15.4f}"
        )

        table.add_row(
            "Alpha",
            f"{snapshot["alpha"]:<15.4f}"
        )

        table.add_row("", "")

        # -------------------------
        # QUOTES
        # -------------------------

        table.add_row(
            "[bold yellow]QUOTES[/bold yellow]",
            ""
        )

        table.add_row(
            "My Bid / Size",
            f"{snapshot["my_bid"]:<10.4f} ({snapshot["current_bid_size"]:<6.4f})"
        )

        table.add_row(
            "My Ask / Size",
            f"{snapshot["my_ask"]:<10.4f} ({snapshot["current_ask_size"]:<6.4f})"
        )

        orders = sorted(
            snapshot["orders"],
            key=lambda o: 0 if o.side == "BUY" else 1
        )

        orders = [
            f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]"
            for o in orders
        ]

        # force exactly 2 rows
        orders = (orders + ["—", "—"])[:2]

        open_orders_str = "\n".join(orders)

        table.add_row(
            "Open Orders",  
            f"{open_orders_str}"
        )

        fill_candidates_str = "\n".join([
            f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]"
            for o in snapshot["fill_candidates"]
        ]) or "—"

        table.add_row(
            "Fill Candidates",
            f"{fill_candidates_str}"
        )

        last_order_update = snapshot["last_order_update"] if snapshot["last_order_update"] else None
        last_order_update_str = f"{last_order_update.side:<5} | {self.config.from_tick(last_order_update.price):>10.4f} | {last_order_update.remaining:>8.6f} [{last_order_update.status}]" if last_order_update is not None else "—"
        
        table.add_row(
            "Last Order Update",
            last_order_update_str
        )

        table.add_row("", "")

        # -------------------------
        # EXECUTION
        # -------------------------

        table.add_row(
            "[bold yellow]EXECUTION[/bold yellow]",
            ""
        )

        bid_queue = state.compute_queue_ahead("bids", snapshot["bid"])
        ask_queue = state.compute_queue_ahead("asks", snapshot["ask"])

        bid_pressure = bid_queue / (bid_size + 1e-9)
        ask_pressure = ask_queue / (ask_size + 1e-9)

        table.add_row(
            "Queue Ahead / Bid",
            f"{bid_queue:<10.4f}"
        )

        table.add_row(
            "Queue Ahead / Ask",
            f"{ask_queue:<10.4f}"
        )

        table.add_row(
            "Queue Pressure / Bid",
            f"{bid_pressure:<10.4f}"
        )

        table.add_row(
            "Queue Pressure / Ask",
            f"{ask_pressure:<10.4f}"
        )

        table.add_row("", "")

        # -------------------------
        # RISK
        # -------------------------

        # constant-width risk bar
        risk_blocks = min(20, int(abs(snapshot["inventory"]) * 2))

        inv_bar = (
            "█" * risk_blocks +
            " " * (20 - risk_blocks)
        )

        table.add_row(
            "[bold yellow]RISK[/bold yellow]",
            ""
        )

        table.add_row(
            "Inventory",
            self.color_risk(snapshot["inventory"], limit=10)
        )

        table.add_row(
            "Realized PnL",
            self.color_pnl(snapshot["realized_pnl"])
        )

        table.add_row(
            "Unrealized PnL",
            self.color_pnl(snapshot["unrealized_pnl"])
        )

        table.add_row(
            "Total PnL",
            self.color_pnl(snapshot["total_pnl"])
        )

        table.add_row(
            "Risk",
            f"[red]{inv_bar}[/red]"
        )

        table.add_row("", "")

        # -------------------------
        # SYSTEM
        # -------------------------

        table.add_row(
            "[bold yellow]SYSTEM[/bold yellow]",
            ""
        )

        table.add_row("Time", self.config.format_ms_precise(self.config.now_ms()))

        table.add_row("Last Trade ts", self.config.format_ms_precise(snapshot["last_trade_ts"]))

        table.add_row("Last Depth ts", self.config.format_ms_precise(snapshot["last_depth_ts"]))


        return Panel(
            table,
            border_style="bright_blue"
        )
    
    def update_dashboard(self):
        now = time.time()
        if self.live and now - self.last_dashboard_update > 0.01:
            self.live.update(self.render_dashboard())
            self.last_dashboard_update = now


# Global State Layer
class State:
    def __init__(self, config):
        
        # Market Data Layer
        self.config = config
        self.market_book = OrderBook(config=self.config)
        self.last_mid = None

        # Strategy Layer
        self.last_signal = None

        # Derived Signals Layer
        self.order_imbalance = 0.0
        self.trade_imbalance = 0.0
        self.ewma_var = 0.0

        # Risk Layer
        self.inventory = 0.0
        self.cash = 100000.0
        self.initial_cash = 100000.0
        self.realized_pnl = 0.0
        self.avg_entry_price = 0.0

        # Microstructure Simulation Layer
        self.queue_flow = {
            "bids": {},
            "asks": {}
        }

        self.my_queue_position = {
            "bids": {},
            "asks": {}
        }

        # Event Observation Layer
        self.last_trade_ts = None
        self.last_trade = {}
        self.last_depth_ts = None
        self.fill_candidates = []
        self.last_order_update = None

        # Control Layer
        self.initialized = False

    
    def get_vol(self):
        return np.sqrt(self.ewma_var)
    
    def update_vol(self):
        alpha = 0.90

        mid = self.market_book.mid()

        if self.last_mid is not None:
            r = np.log(mid / self.last_mid)
            self.ewma_var = alpha * self.ewma_var + (1 - alpha) * r * r

        self.last_mid = mid

    def compute_order_imbalance(self):

        bid_tick, bid_size = self.market_book.best_bid()
        ask_tick, ask_size = self.market_book.best_ask()
        
        self.order_imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1e-9)

    def on_fill(self, price, qty, side):

        fill_value = price * qty

        # -------------------------
        # Snapshot old state FIRST
        # -------------------------
        old_inv = self.inventory
        old_avg = self.avg_entry_price

        # -------------------------
        # BUY: you gain inventory
        # -------------------------
        if side == "BUY":

            new_inv = old_inv + qty

            # If you were short → realize PnL
            if old_inv < 0:
                closed_qty = min(qty, abs(old_inv))
                self.realized_pnl += closed_qty * (old_avg - price)

            # update cash
            self.cash -= fill_value

            # update inventory
            self.inventory = new_inv

            # update avg entry price ONLY if position exists
            if new_inv != 0:
                self.avg_entry_price = (
                    old_avg * old_inv + price * qty
                ) / new_inv
            else:
                self.avg_entry_price = 0.0

        # -------------------------
        # SELL: you reduce inventory / go short
        # -------------------------
        else:

            new_inv = old_inv - qty

            # If you were long → realize PnL
            if old_inv > 0:
                closed_qty = min(qty, old_inv)
                self.realized_pnl += closed_qty * (price - old_avg)

            # update cash
            self.cash += fill_value

            # update inventory
            self.inventory = new_inv

            # update avg entry price ONLY if position exists
            if new_inv != 0:
                self.avg_entry_price = (
                    old_avg * old_inv - price * qty
                ) / new_inv
            else:
                self.avg_entry_price = 0.0


    def get_unrealized_pnl(self, mid):
        return self.inventory * (mid - self.avg_entry_price)

    
    def get_pnl(self):

        mid = self.market_book.mid()

        unrealized = self.inventory * (mid - self.avg_entry_price)

        return self.realized_pnl + unrealized
    
    
    def compute_queue_ahead(self, side, price):
        price = self.config.to_tick(price) if isinstance(price, float) else price

        my_pos = self.my_queue_position[side].get(price, 0.0) # your position in the queue at this price level
        flow = self.queue_flow.get(side, {}).get(price, 0.0) # estimated queue depletion since then

        ahead = max(0.0, my_pos - flow)
        return ahead
    

    def update_queue_from_depth(self, bids, asks):

    # -------------------------
    # BIDS
    # -------------------------

        for p, q in bids:

            tick = self.config.to_tick(float(p))

            old = self.market_book.bids.get(tick, 0.0)

            new = float(q)

            if old > 0:
                depletion = max(0.0, old - new) # simple assumption: any size reduction is due to queue depletion

                self.queue_flow["bids"][tick] = (
                    self.queue_flow["bids"].get(tick, 0.0)
                    + depletion
                )

        # -------------------------
        # ASKS
        # -------------------------

        for p, q in asks:

            tick = self.config.to_tick(float(p))

            old = self.market_book.asks.get(tick, 0.0)

            new = float(q)

            if old > 0:
                depletion = max(0.0, old - new)

            self.queue_flow["asks"][tick] = (
                self.queue_flow["asks"].get(tick, 0.0)
                + depletion
            )

# Simulation Layer
class ExecutionAdapter:
    """
    Bridges LIVE and BACKTEST without changing ExecutionEngine.
    """

    def __init__(self, execution, replay=None):
        self.execution = execution
        self.replay = replay
        self.latency_ms = execution.latency_ms

    # -------------------------
    # LIVE MODE ENTRYPOINT
    # -------------------------
    def place_quotes(self, bid, ask):
        self.execution.place_quotes(bid, ask)

    def cancel_all(self):
        self.execution.cancel_all()

    # -------------------------
    # BACKTEST MODE ENTRYPOINT
    # -------------------------
    def place_quotes_replay(self, bid, ask):
        return {
            "type": "PLACE_QUOTE",
            "bid": bid,
            "ask": ask
        }

# class ReplayEngine:
#     def __init__(self, events, state, strategy, execution, execution_adapter):

#         self.events = sorted(events, key=lambda x: x["ts"])

#         self.state = state
#         self.strategy = strategy
#         self.execution = execution
#         self.execution_adapter = execution_adapter

#         self.current_time = 0

#         # future scheduled actions (latency simulation)
#         self.action_queue = []  # (ts, action)

#         self.i = 0  # pointer into historical events

#     def schedule(self, ts, action):
#         heapq.heappush(self.action_queue, (ts, action))

#     def process_actions(self):

#         while self.action_queue and self.action_queue[0][0] <= self.current_time:

#             ts, action = heapq.heappop(self.action_queue)

#             self.execute_action(action)

#     def execute_action(self, action):

#         if action["type"] == "PLACE_QUOTE":

#             self.execution_adapter.place_quotes(
#                 action["bid"],
#                 action["ask"]
#             )

#         elif action["type"] == "CANCEL_ALL":

#             self.execution_adapter.cancel_all()

#     def process_market_event(self, event):

#         if event["type"] == "depth":
#             self.state.market_book.apply_delta(
#                 event["data"]["b"],
#                 event["data"]["a"],
#                 self.state
#             )
#             self.state.update_vol()

#             self.state.compute_order_imbalance()

#             if self.state.initialized:
#                 self.strategy_step()

#         elif event["type"] == "trade":
#             self.execution.process_trade(event["data"])

#     def strategy_step(self):

#         signal = self.strategy.on_market_update(self.state)

#         if signal is None:
#             return

#         action = self.execution_adapter.place_quotes_replay(
#             signal["bid"],
#             signal["ask"]
#         )

#         # schedule execution with latency
#         self.schedule(
#             self.current_time + self.execution.latency_ms,
#             action
#         )

#     def run(self):

#         while self.i < len(self.events):

#             event = self.events[self.i]

#             # advance time
#             self.current_time = event["ts"]

#             # 1. execute delayed actions that are now due
#             self.process_actions()

#             # 2. process market event
#             self.process_market_event(event)

#             # 3. move forward
#             self.i += 1

#         # flush remaining actions
#         while self.action_queue:
#             self.current_time = self.action_queue[0][0]
#             self.process_actions()

# class ReplayEngine:
#     """
#     Drop-in replacement for BinanceFeed.

#     Responsibilities:
#     - replay historical events in time order
#     - maintain simulated clock
#     - call Engine (NOT Strategy / Execution directly)
#     """

#     def __init__(self, events, engine, latency_ms=20):
#         self.events = sorted(events, key=lambda x: (x["ts"], x["type"]))

#         self.engine = engine
#         self.state = engine.state
#         self.latency_ms = latency_ms

#         self.i = 0
#         self.current_time = 0

#         # latency simulation queue
#         self.latency_queue = []  # (execute_ts, fn)

#         self.running = False

#     # -------------------------
#     # CORE DISPATCH
#     # -------------------------
#     def _handle_depth(self, event):
#         data = event["data"]

#         self.state.market_book.apply_delta(
#             data["b"],
#             data["a"],
#             self.state
#         )

#         self.state.update_vol()
#         self.state.compute_order_imbalance()

#         self.state.last_depth_ts = event["ts"]

#         self.engine.on_market_data()
#         self.engine.execution_cycle()

#     def _handle_trade(self, event):
#         trade = event["data"]

#         self.state.last_trade_ts = trade["timestamp"]
#         self.state.last_trade = trade

#         self.engine.on_trade_event(trade)

#     # -------------------------
#     # LATENCY SIMULATION
#     # -------------------------
#     def _process_latency_queue(self):
#         while self.latency_queue and self.latency_queue[0][0] <= self.current_time:
#             _, fn = heapq.heappop(self.latency_queue)
#             fn()

#     def _schedule(self, fn):
#         heapq.heappush(
#             self.latency_queue,
#             (self.current_time + self.latency_ms, fn)
#         )

#     # -------------------------
#     # MAIN LOOP
#     # -------------------------
#     def run(self):
#         self.running = True

#         while self.i < len(self.events):

#             event = self.events[self.i]
#             self.current_time = event["ts"]

#             # 1. execute delayed actions (if any)
#             self._process_latency_queue()

#             # 2. process market event
#             if event["type"] == "depth":
#                 self._handle_depth(event)

#             elif event["type"] == "trade":
#                 self._handle_trade(event)

#             self.i += 1

#         # flush remaining latency actions
#         while self.latency_queue:
#             self.current_time = self.latency_queue[0][0]
#             self._process_latency_queue()

#         self.running = False

class ReplayEngine:
    """
    ReplayEngine = drop-in replacement for BinanceFeed

    It emits the SAME callbacks:
        - on_market_data(state)
        - on_trade_event(trade)
    """

    def __init__(self, events, state, config,
                 on_market_data,
                 on_trade_event):

        self.events = sorted(events, key=lambda x: x["ts"])
        self.state = state
        self.config = config

        self.on_market_data = on_market_data
        self.on_trade_event = on_trade_event

        self.i = 0
        self.running = False

        # optional speed control
        self.speed_multiplier = 1.0
        self.last_ts = None

    # -------------------------
    # CORE LOOP
    # -------------------------
    def start(self):
        self.running = True
        threading.Thread(target=self.run, daemon=True).start()

    def stop(self):
        self.running = False

    def run(self):
        while self.running and self.i < len(self.events):

            event = self.events[self.i]
            ts = event["ts"]

            # optional real-time pacing
            if self.last_ts is not None:
                dt = (ts - self.last_ts) / 1000.0
                time.sleep(max(0.0, dt / self.speed_multiplier))

            self.last_ts = ts

            self.process_event(event)
            self.i += 1

    # -------------------------
    # EVENT ROUTER
    # -------------------------
    def process_event(self, event):

        if event["type"] == "depth":
            self._on_depth(event)

        elif event["type"] == "trade":
            self._on_trade(event)

    # -------------------------
    # DEPTH EVENT
    # -------------------------
    def _on_depth(self, event):

        data = event["data"]

        bids = data["b"]
        asks = data["a"]

        # update order book
        self.state.market_book.apply_delta(bids, asks, self.state)

        # update derived features
        self.state.update_vol()
        self.state.compute_order_imbalance()

        # mark system initialized after first depth
        if not self.state.initialized:
            self.state.initialized = True

        # trigger engine callback (THIS replaces BinanceFeed callback)
        self.on_market_data()

    # -------------------------
    # TRADE EVENT
    # -------------------------
    def _on_trade(self, event):

        trade = event["data"]

        # update state if needed
        self.state.last_trade = trade
        self.state.last_trade_ts = event["ts"]

        # IMPORTANT: do NOT touch execution engine here
        self.on_trade_event(trade)

# Intialization and Websocket Handling Layer
class TradingSystem:

    def __init__(self, mode="live", events=None):

        self.config = MarketConfig(tick_size=0.01)
        self.state = State(config=self.config)
        self.strategy = MarketMakingStrategy(config=self.config)
        self.recorder = DatasetRecorder(config=self.config)

        self.execution = ExecutionEngine(
            config=self.config,
            state=self.state,
            recorder=self.recorder
        )

        self.engine = Engine(
            config=self.config,
            state=self.state,
            strategy=self.strategy,
            execution=self.execution,
            recorder=self.recorder
        )

        # -------------------------
        # SWITCH HERE
        # -------------------------
        if mode == "live":

            self.feed = BinanceFeed(
                state=self.state,
                on_market_data=self.engine.on_market_data,
                on_trade_event=self.engine.on_trade_event
            )

        elif mode == "replay":

            self.feed = ReplayEngine(
                events=events,
                state=self.state,
                config=self.config,
                on_market_data=self.engine.on_market_data,
                on_trade_event=self.engine.on_trade_event
            )

        self.mode = mode
        self.running = False
        self.threads = []

    def start(self):
        self.running = True
        self.engine.start_dashboard()
        self.feed.start()

        self.start_execution_loop()
        self.start_dashboard_loop()

    def start_execution_loop(self):

        def exec_loop():
            while self.running:
                self.engine.execution_cycle()
                time.sleep(0.05)  # 20Hz execution

        t = threading.Thread(target=exec_loop, daemon=True)
        t.start()

        self.threads.append(t)

    def start_dashboard_loop(self):

        def dashboard_loop():
            while self.running:
                self.engine.update_dashboard()
                time.sleep(0.02) # 50Hz dashboard refresh

        t = threading.Thread(target=dashboard_loop,daemon=True)
        t.start()

        self.threads.append(t)

    def run_forever(self):

        try:
            while self.running:
                time.sleep(1)

        except KeyboardInterrupt: # lets you stop with CTRL+C (standard in HFT backtests too)
            print("INTERRUPT RECEIVED - SHUTTING DOWN")

        finally:
            self.shutdown()

    def generate_labels(self):

        # 1. trade-only labels
        self.recorder.label_adverse_selection(horizon=10)
        self.recorder.add_trade_impact_labels(horizon=10)
        self.recorder.label_toxicity(horizon=5)

        # 2. snapshot alignment FIRST (important)
        self.recorder.align_events()

        # 3. fill-level labels (depends on snapshots)
        self.recorder.label_fill_markouts(horizon_list=(1000, 5000))

        # 4. snapshot-based returns
        self.recorder.finalize_returns(horizon=10)

    def export_data(self):
        self.recorder.export_parquet()
        print("DATASET SAVED SUCCESSFULLY")

    def shutdown(self):

        self.running = False
        self.feed.stop()

        for t in self.threads:
            t.join(timeout=1)

        self.generate_labels()
        self.export_data()


# class TradingSystem:

#     def __init__(self):
#         self.config = MarketConfig(tick_size=0.01)
#         self.state = State(config=self.config)
#         self.strategy = MarketMakingStrategy(config=self.config)
#         self.recorder = DatasetRecorder(config=self.config)
        
#         self.execution = ExecutionEngine(
#             config=self.config,
#             state=self.state,
#             recorder=self.recorder
#         )

#         self.engine = Engine(
#             config=self.config,
#             state=self.state,
#             strategy=self.strategy,
#             execution=self.execution,
#             recorder=self.recorder
#         )

#         self.feed = BinanceFeed(
#             state=self.state,
#             on_market_data=self.engine.on_market_data,
#             on_trade_event=self.engine.on_trade_event
#         )

#         self.running = False
#         self.threads = []

#     def start(self):
#         self.running = True
#         self.engine.start_dashboard()
#         self.feed.start()

#         self.start_execution_loop()
#         self.start_dashboard_loop()

#     def start_execution_loop(self):

#         def exec_loop():
#             while self.running:
#                 self.engine.execution_cycle()
#                 time.sleep(0.05)  # 20Hz execution

#         t = threading.Thread(target=exec_loop, daemon=True)
#         t.start()

#         self.threads.append(t)

#     def start_dashboard_loop(self):

#         def dashboard_loop():
#             while self.running:
#                 self.engine.update_dashboard()
#                 time.sleep(0.02) # 50Hz dashboard refresh

#         t = threading.Thread(target=dashboard_loop,daemon=True)
#         t.start()

#         self.threads.append(t)

#     def run_forever(self):

#         try:
#             while self.running:
#                 time.sleep(1)

#         except KeyboardInterrupt: # lets you stop with CTRL+C (standard in HFT backtests too)
#             print("INTERRUPT RECEIVED - SHUTTING DOWN")

#         finally:
#             self.shutdown()

#     def generate_labels(self):

#         # 1. trade-only labels
#         self.recorder.label_adverse_selection(horizon=10)
#         self.recorder.add_trade_impact_labels(horizon=10)
#         self.recorder.label_toxicity(horizon=5)

#         # 2. snapshot alignment FIRST (important)
#         self.recorder.align_events()

#         # 3. fill-level labels (depends on snapshots)
#         self.recorder.label_fill_markouts(horizon_list=(1000, 5000))

#         # 4. snapshot-based returns
#         self.recorder.finalize_returns(horizon=10)

#     def export_data(self):
#         self.recorder.export_parquet()
#         print("DATASET SAVED SUCCESSFULLY")

#     def shutdown(self):

#         self.running = False
#         self.feed.stop()

#         for t in self.threads:
#             t.join(timeout=1)

#         self.generate_labels()
#         self.export_data()
    
if __name__ == "__main__":

    system = TradingSystem(mode="live", events=None)

    system.start()

    system.run_forever()