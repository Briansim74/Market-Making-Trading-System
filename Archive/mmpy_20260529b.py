import os
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
from datetime import datetime, timezone
from sortedcontainers import SortedDict

import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket
from typing import Set

from rich import box
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel

# # =========================
# # MARKET MICROSTRUCTURE UTILS
# # =========================

class MarketConfig:
    def __init__(self, params):
        self.tick_size = params["tick_size"]

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

        return self.last_update_id, snapshot
    
    def set_orderbook_snapshot(self, snapshot):

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

        return self.last_update_id, snapshot

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
    def __init__(self, state, on_market_data, on_trade_event, logger, params):
        self.state = state
        self.instrument = params["instrument"]

        self.on_market_data = on_market_data
        self.on_trade_event = on_trade_event

        self.log_event = logger["log_event"]
        self.log_orderbook_snapshot = logger["log_orderbook_snapshot"]

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

        # depth_socket = f"wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
        # trade_socket = f"wss://stream.binance.com:9443/ws/btcusdt@trade"

        depth_socket = f"wss://stream.binance.com:9443/ws/{self.instrument}@depth@100ms"
        trade_socket = f"wss://stream.binance.com:9443/ws/{self.instrument}@trade"

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

        last_update_id, snapshot = self.state.market_book.initialize_from_binance(
            self.instrument.upper(),
            1000
        )

        self.log_orderbook_snapshot(snapshot)

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

    # -------------------------
    # DEPTH EVENT
    # -------------------------

    def _parse_book(self, message):
        bids = [(float(p), float(q)) for p, q in message.get("b", [])]
        asks = [(float(p), float(q)) for p, q in message.get("a", [])]

        return bids, asks

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
        self.log_event({
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

    # -------------------------
    # TRADE EVENT
    # -------------------------

    def _parse_trade(self, message):

        return {
            "side": "SELL" if message["m"] else "BUY", # message["m"] is a boolean flag called “isBuyerMaker”, if true, SELLER is aggressive taker, if false, BUYER is aggressive taker
            "price": float(message["p"]),
            "qty": float(message["q"]),
            "timestamp": message["T"]
        }

    def _on_trade_message(self, ws, message):
        if not self.running:
            return
        
        msg = json.loads(message)

        # -------------------------
        # RAW LOGGING (BEST PLACE)
        # -------------------------
        self.log_event({
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
    def __init__(self, config, params):
        self.config = config
        self.params = params
        
        self.rows = []
        self.trades = []
        self.quotes = []
        self.fills = []

        # for replayfeed
        self.events = [] # or buffer for parquet
        self.orderbook_snapshot = None

    def log_event(self, event):
        """
        Stores raw exchange event BEFORE any processing.
        This is your replay ground truth.
        """

        self.events.append({
            "type": event["type"],
            "ts": event["ts"],
            "message": event["message"]
        })

    def log_orderbook_snapshot(self, snapshot):

        self.orderbook_snapshot = snapshot

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

    def create_run_dir(self, base="data/runs"):
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_path = os.path.join(base, f"run_{run_id}")

        os.makedirs(run_path, exist_ok=True)

        return run_path, run_id

    def export_run(self, base_path=r"data\runs"):

        run_path, run_id = self.create_run_dir(base_path)

        # 1. export files
        snapshot_path = os.path.join(run_path, "snapshots.parquet")
        trades_path = os.path.join(run_path, "trades.parquet")
        quotes_path = os.path.join(run_path, "quotes.parquet")
        fills_path = os.path.join(run_path, "fills.parquet")
        events_path = os.path.join(run_path, "events.parquet")
        orderbook_snapshot_path = os.path.join(run_path, "orderbook_snapshot.json")
        manifest_path = os.path.join(run_path, "manifest.json")

        pd.DataFrame(self.rows).to_parquet(snapshot_path, index=False)
        pd.DataFrame(self.trades).to_parquet(trades_path, index=False)
        pd.DataFrame(self.quotes).to_parquet(quotes_path, index=False)
        pd.DataFrame(self.fills).to_parquet(fills_path, index=False)
        pd.DataFrame(self.events).to_parquet(events_path, index=False)

        with open(orderbook_snapshot_path, "w") as f:
            json.dump(self.orderbook_snapshot, f)

        # (mode="replay", 
        #                    model="mm-core",
        #                    instrument="btcusdt", 
        #                    exchange="binance",
        #                    replay_events={
        #                        "events": "events.parquet",
        #                        "orderbook_snapshot": "orderbook_snapshot.json"
        #                    })

        # params
    # "run_id": 0,
    # "mode": "replay",
    # "model": "mm-core",
    # "instrument": "btcusdt",
    # "exchange": "binance",
    # "tick_size": 0.01,

    # # files
    # "files": {
    #     "trades": "",
    #     "snapshots": "",
    #     "quotes": "",
    #     "fills": "",
    #     "replay_events": {
    #         "events": "",
    #         "orderbook_snapshot": ""
    #     }
    # },

    # # dashboard config
    # "server_config": {
    #     "host": "0.0.0.0",
    #     "port": 8000
    # }
        
        # 2. build manifest
        # manifest = {
            
        #     # params
        #     "run_id": run_id,
        #     "mode": "replay",
        #     "model": self.params["model"],
        #     "instrument": self.params["instrument"],
        #     "exchange": self.params["exchange"],
        #     "tick_size": self.params["tick_size"],

        #     # files
        #     "trades": "trades.parquet",
        #     "snapshots": "snapshots.parquet",
        #     "quotes": "quotes.parquet",
        #     "fills": "fills.parquet",
        #     "replay_events": {
        #         "events": "events.parquet",
        #         "orderbook_snapshot": "orderbook_snapshot.json"
        #     }
        # }

        manifest = params
        
        params["run_id"] = run_id
        params["mode"] = "replay"

        # 3. write manifest
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        print("DATASETS SAVED SUCCESSFULLY")
        print(f"Saved run → {run_path}")

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


class Execution:
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
                event_type="REPLACE",
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
                event_type="REPLACE",
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
    def __init__(self, config, state, strategy, execution, recorder, dashboard, params):
        self.config = config
        self.state = state
        self.strategy = strategy
        self.execution = execution
        self.recorder = recorder

        # System Configuration
        self.model = params["model"]
        self.mode = params["mode"]
        self.exchange = params["exchange"]
        self.instrument = params["instrument"]
        
        # React dashboard
        self.dashboard = dashboard
        
        # Rich dashboard
        self.live = None
        self.last_dashboard_update = 0

    def on_market_data(self):

        # HARD SAFETY: do not log or trade before sync
        if not self.state.initialized:
            return
    
        signal = self.strategy.on_market_update(self.state)

        if signal is None:
            return

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
    
    def build_snapshot(self):
        state = self.state
        book = state.market_book
        execution = self.execution
        strategy = self.strategy

        pnl_pct = state.get_pnl() / state.initial_cash * 100

        # -------------------------
        # MARKET
        # -------------------------
        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        bid = self.config.from_tick(bid_tick)
        ask = self.config.from_tick(ask_tick)

        mid = (bid + ask) / 2
        spread = ask - bid

        trade = state.last_trade if state.last_trade else None
        trade_str = f"{trade["side"]:<5} | {trade["price"]:>10.4f} | {trade["qty"]:>8.6f}" if trade else "—"

        # -------------------------
        # SIGNALS
        # -------------------------
        fair = strategy.compute_fair(state)
        skew = strategy.compute_skew(state)
        reservation = fair + skew
        alpha = state.last_signal["alpha"] if state.last_signal else 0.0

        # -------------------------
        # EXECUTION
        # -------------------------
        bid_queue = state.compute_queue_ahead("bids", bid)
        ask_queue = state.compute_queue_ahead("asks", ask)
        bid_pressure = bid_queue / (bid_size + 1e-9)
        ask_pressure = ask_queue / (ask_size + 1e-9)

        orders = sorted(execution.open_orders.values(), key=lambda o: 0 if o.side == "BUY" else 1)
        orders = [f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]" for o in orders]
        orders_str = (orders + ["—", "—"])[:2]

        fill_candidates_str = "\n".join([f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]"
            for o in state.fill_candidates
        ]) or "—"

        last_order_update = state.last_order_update if state.last_order_update else None
        last_order_update_str = f"{last_order_update.side:<5} | {self.config.from_tick(last_order_update.price):>10.4f} | {last_order_update.remaining:>8.6f} [{last_order_update.status}]" if last_order_update else "—"

        # -------------------------
        # SYSTEM
        # -------------------------
        ts = self.config.format_ms_precise(self.config.now_ms())
        
        last_trade_ts = state.last_trade_ts or 0.0
        last_depth_ts = state.last_depth_ts or 0.0
        last_trade_ts = self.config.format_ms_precise(last_trade_ts)
        last_depth_ts = self.config.format_ms_precise(last_depth_ts)

        snapshot = {
            "title": {
                "model": self.model,
                "mode": self.mode,
                "exchange": self.exchange,
                "instrument": self.instrument,
                "pnl_pct": pnl_pct
            },

            "market": {
                "mid": mid,
                "spread": spread,
                "bid": bid,
                "ask": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,

                "ewma_vol": state.get_vol(),
                "order_imbalance": state.order_imbalance,
                "trade_imbalance": state.trade_imbalance,
                "trade": trade_str
            },

            "signals": {
                "fair_value": fair,
                "skew": skew,
                "reservation": reservation,
                "alpha": alpha
            },

            "quotes": {
                "my_bid": execution.last_bid or 0.0,
                "my_ask": execution.last_ask or 0.0,
                "current_bid_size": execution.current_bid_size,
                "current_ask_size": execution.current_ask_size,
            },

            "execution": {
                "bid_queue": bid_queue,
                "ask_queue": ask_queue,
                "bid_pressure": bid_pressure,
                "ask_pressure": ask_pressure,
                "open_order_one": orders_str[0],
                "open_order_two": orders_str[1],
                "fill_candidates": fill_candidates_str,
                "last_order_update": last_order_update_str
            },

            "risk": {
                "inventory": state.inventory,
                "realized_pnl": state.realized_pnl,
                "unrealized_pnl": state.get_unrealized_pnl(mid),
                "total_pnl": state.get_pnl()
            },

            "system": {
                "time": ts,
                "last_trade_ts": last_trade_ts,
                "last_depth_ts": last_depth_ts
            }
        }

        return snapshot
    
    def start_rich_dashboard(self):
        self.live = Live(
            self.render_dashboard(),
            refresh_per_second=60,
            transient=False
        )
        self.live.start()

    def render_dashboard(self):

        # -----------------------------
        # SNAPSHOT
        # -----------------------------
        snapshot = self.build_snapshot()

        # -------------------------
        # TITLE
        # -------------------------
        pnl_pct = snapshot["title"]["pnl_pct"]
        pnl_pct_sign = "+" if pnl_pct > 0 else ""
        title=f"{snapshot["title"]["model"]} | {snapshot["title"]["mode"]} | {snapshot["title"]["exchange"]} | \
            {snapshot["title"]["instrument"]} | pnl={pnl_pct_sign}{pnl_pct:.4f}%"

        # -------------------------
        # FIXED WIDTH TABLE
        # -------------------------
        table = Table(title=title, box=box.SQUARE, expand=False)
        table.add_column("Metric", style="bold cyan", width=20, no_wrap=True)
        table.add_column("Value", style="white",  width=50, no_wrap=True)

        # -------------------------
        # MARKET
        # -------------------------
        table.add_row("[bold yellow]MARKET[/bold yellow]", "")
        table.add_row("Mid", f"{snapshot["market"]["mid"]:<15.4f}")
        table.add_row("Spread", f"{snapshot["market"]["spread"]:<15.4f}")
        table.add_row("Best Bid / Size", f"{snapshot["market"]["bid"]:<10.4f} ({snapshot["market"]["bid_size"]:<6.4f})")
        table.add_row("Best Ask / Size", f"{snapshot["market"]["ask"]:<10.4f} ({snapshot["market"]["ask_size"]:<6.4f})")

        table.add_row("EWMA Vol", f"{snapshot["market"]["ewma_vol"]:.2e}")
        table.add_row("Order Imbalance", f"{snapshot["market"]["order_imbalance"]:<15.4f}")
        table.add_row("Trade Imbalance", f"{snapshot["market"]["trade_imbalance"]:<15.4f}")
        table.add_row("Last Trade", snapshot["market"]["trade"])
        table.add_row("", "")

        # -------------------------
        # SIGNALS
        # -------------------------
        table.add_row("[bold yellow]SIGNALS[/bold yellow]", "")
        table.add_row("Fair Value", f"{snapshot["signals"]["fair_value"]:<15.4f}")
        table.add_row("Inventory Skew", f"{snapshot["signals"]["skew"]:<15.4f}")
        table.add_row("Reservation", f"{snapshot["signals"]["reservation"]:<15.4f}")
        table.add_row("Alpha", f"{snapshot["signals"]["alpha"]:<15.4f}")
        table.add_row("", "")

        # -------------------------
        # QUOTES
        # -------------------------
        table.add_row("[bold yellow]QUOTES[/bold yellow]", "")
        table.add_row("My Bid / Size", f"{snapshot["quotes"]["my_bid"]:<10.4f} ({snapshot["quotes"]["current_bid_size"]:<6.4f})")
        table.add_row("My Ask / Size", f"{snapshot["quotes"]["my_ask"]:<10.4f} ({snapshot["quotes"]["current_ask_size"]:<6.4f})")
        table.add_row("", "")

        # -------------------------
        # EXECUTION
        # -------------------------
        table.add_row("[bold yellow]EXECUTION[/bold yellow]", "")
        table.add_row("Queue Ahead / Bid", f"{snapshot["market"]["bid"]:<10.4f} ({snapshot["execution"]["bid_queue"]:<6.4f})")
        table.add_row("Queue Ahead / Ask", f"{snapshot["market"]["ask"]:<10.4f} ({snapshot["execution"]["ask_queue"]:<6.4f})")
        table.add_row("Queue Pressure / Bid", f"{snapshot["market"]["bid"]:<10.4f} ({snapshot["execution"]["bid_pressure"]:<6.4f})")
        table.add_row("Queue Pressure / Ask", f"{snapshot["market"]["ask"]:<10.4f} ({snapshot["execution"]["ask_pressure"]:<6.4f})")

        table.add_row("Open Orders", snapshot["execution"]["open_order_one"])
        table.add_row("", snapshot["execution"]["open_order_two"])
        table.add_row("Fill Candidates", snapshot["execution"]["fill_candidates"])
        table.add_row("Last Order Update", snapshot["execution"]["last_order_update"])
        table.add_row("", "")

        # -------------------------
        # RISK
        # -------------------------
        table.add_row("[bold yellow]RISK[/bold yellow]", "")
        table.add_row("Inventory", self.color_risk(snapshot["risk"]["inventory"], limit=10))
        table.add_row("Realized PnL", self.color_pnl(snapshot["risk"]["realized_pnl"]))
        table.add_row("Unrealized PnL", self.color_pnl(snapshot["risk"]["unrealized_pnl"]))
        table.add_row("Total PnL", self.color_pnl(snapshot["risk"]["total_pnl"]))
        table.add_row("Risk", self.centered_inventory_bar(inv=snapshot["risk"]["inventory"], max_inv=10, width=21))
        table.add_row("", "")

        # -------------------------
        # SYSTEM
        # -------------------------
        table.add_row("[bold yellow]SYSTEM[/bold yellow]","")
        table.add_row("Time", snapshot["system"]["time"])
        table.add_row("Last Trade ts", snapshot["system"]["last_trade_ts"])
        table.add_row("Last Depth ts", snapshot["system"]["last_depth_ts"])

        return Panel(table, border_style="bright_blue")
    
    def update_dashboard(self):
        now = time.time()
        if self.live and now - self.last_dashboard_update > 0.01:
            self.live.update(self.render_dashboard())
            self.last_dashboard_update = now

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

    def centered_inventory_bar(self, inv, max_inv=10, width=21): 

        half = width // 2

        scaled = int((inv / max_inv) * half)

        chars = [" "] * width

        chars[half] = "|"

        if scaled > 0:
            for i in range(half + 1, half + scaled + 1):
                chars[i] = "█"

        elif scaled < 0:
            for i in range(half - 1, half + scaled - 1, -1):
                chars[i] = "█"

        left_half = "".join(chars[:half])
        right_half = "".join(chars[half + 1:])

        inv_bar = (
            f"[red]{left_half}[/red]"
            f"[white]{chars[half]}[/white]"
            f"[green]{right_half}[/green]"
        )

        return inv_bar


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

        my_pos = self.my_queue_position[side].get(price, 0.0) # Initial size ahead of you when you joined
        flow = self.queue_flow.get(side, {}).get(price, 0.0) # Estimated amount of queue depletion since you joined

        ahead = max(0.0, my_pos - flow) # Estimated queue position = Initial size ahead - estimated queue depletion
        
        return ahead
    

    def update_queue_from_depth(self, bids, asks):

    # Tracks the size reduction, it is a positive number

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

# Simulation Feed Layer
class ReplayFeed:
    """
    ReplayFeed = drop-in replacement for BinanceFeed

    It emits the SAME callbacks:
        - on_market_data(state)
        - on_trade_event(trade)
    """

    def __init__(self, state, on_market_data, on_trade_event, logger, params):

        self.events = params["replay_events"]["events"]
        self.orderbook_snapshot = params["replay_events"]["orderbook_snapshot"]
        self.state = state

        # -------------------------
        # IMPORTANT FIX
        # -------------------------

        self.on_market_data = on_market_data
        self.on_trade_event = on_trade_event

        self.log_event = logger["log_event"]
        self.log_orderbook_snapshot = logger["log_orderbook_snapshot"]

        self.i = 0
        self.running = False

        # optional speed control
        self.speed_multiplier = 1.0 # to speed up market events for backtesting
        self.last_ts = None

    # -------------------------
    # CORE LOOP
    # -------------------------
    def start(self):

        # -------------------------
        # INITIALIZE RAW EVENTS
        # -------------------------

        print("INITIALIZING RAW EVENTS")

        events_df = pd.read_parquet(self.events)

        events_df = events_df.sort_values("ts")

        self.events = events_df.to_dict("records")

        with open(self.orderbook_snapshot, "r") as f:
            self.orderbook_snapshot = json.load(f)

        self.running = True

        print("REPLAY SOCKETS STARTED")

        # -------------------------
        # FETCH SNAPSHOT
        # -------------------------

        last_update_id, snapshot = self.state.market_book.set_orderbook_snapshot(self.orderbook_snapshot)

        self.state.initialized = True

        self.log_orderbook_snapshot(snapshot)

        print("SNAPSHOT FETCHED:", last_update_id)

        print("BOOK SYNCHRONIZED")

        # -------------------------
        # LIVE MODE
        # -------------------------

        threading.Thread(target=self.run, daemon=True).start()

        print("LIVE BOOK RUNNING")

    def stop(self):
        print("STOPPING BINANCE REPLAY FEED")

        self.running = False

        print("BINANCE FEED REPLAY STOPPED")

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
            self._on_depth_message(ws=None, message=event["message"])

        elif event["type"] == "trade":
            self._on_trade_message(ws=None, message=event["message"])

    # -------------------------
    # DEPTH EVENT
    # -------------------------

    def _parse_book(self, message):
        bids = [(float(p), float(q)) for p, q in message.get("b", [])]
        asks = [(float(p), float(q)) for p, q in message.get("a", [])]

        return bids, asks
    
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
        self.log_event({
            "type": "depth",
            "ts": msg["E"],
            "message": message
        })

        # -------------------------
        # LIVE PROCESSING
        # -------------------------

        self.state.last_depth_ts = msg["E"]

        self._on_depth(msg)

    # -------------------------
    # TRADE EVENT
    # -------------------------

    def _parse_trade(self, message):

        return {
            "side": "SELL" if message["m"] else "BUY", # message["m"] is a boolean flag called “isBuyerMaker”, if true, SELLER is aggressive taker, if false, BUYER is aggressive taker
            "price": float(message["p"]),
            "qty": float(message["q"]),
            "timestamp": message["T"]
        }

    def _on_trade_message(self, ws, message):
        if not self.running:
            return
        
        msg = json.loads(message)

        # -------------------------
        # RAW LOGGING (BEST PLACE)
        # -------------------------
        self.log_event({
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

# Monitoring Layer
class DashboardServer:
    def __init__(self, params):
        self.host = params["server_config"]["host"]
        self.port = params["server_config"]["port"]

        self.app = FastAPI()
        self.clients: Set[WebSocket] = set()

        self.loop = None
        self.thread = None

        self._setup_routes()

    def _setup_routes(self):

        @self.app.websocket("/ws")
        async def ws_endpoint(ws: WebSocket):
            await ws.accept()
            self.clients.add(ws)

            try:
                while True:
                    # IMPORTANT: actually wait for disconnect
                    await ws.receive_text()

            except Exception:
                pass
            finally:
                self.clients.discard(ws)

    def start(self):
        def run():
            
            config = uvicorn.Config(
                self.app,
                host=self.host,
                port=self.port,
                log_level="info"
            )

            server = uvicorn.Server(config)

            # capture event loop correctly
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            print(f"UVICORN RUNNING ON http://localhost:{self.port}")

            self.loop.run_until_complete(server.serve())

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def publish(self, event: dict):
        if self.loop is None:
            return

        msg = json.dumps(event)

        asyncio.run_coroutine_threadsafe(
            self._broadcast(msg),
            self.loop
        )

    async def _broadcast(self, msg: str):
        dead = []

        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except:
                dead.append(ws)

        for ws in dead:
            self.clients.discard(ws)


# Intialization and Websocket Handling Layer
class TradingSystem:
    def __init__(self, params):

        # Monitoring Layer
        self.react_dashboard = DashboardServer(params=params)
        
        # Market Configuration
        self.config = MarketConfig(params=params)

        # Market State
        self.state = State(config=self.config)

        # Strategy Layer
        self.strategy = MarketMakingStrategy(config=self.config)

        # Data Recording Layer
        self.recorder = DatasetRecorder(config=self.config, params=params)

        # Execution Layer
        self.execution = Execution(
            config=self.config,
            state=self.state,
            recorder=self.recorder
        )

        # Orchestration Layer
        self.engine = Engine(
            config=self.config,
            state=self.state,
            strategy=self.strategy,
            execution=self.execution,
            recorder=self.recorder,
            dashboard=self.react_dashboard,
            params=params
        )

        # Market Data Layer
        FEEDS = {
            ("live", "binance"): BinanceFeed,
            ("replay", "binance"): ReplayFeed,
        }

        FeedClass = FEEDS[(params["mode"], params["exchange"])]

        self.feed = FeedClass(
            state=self.state,
            on_market_data=self.engine.on_market_data,
            on_trade_event=self.engine.on_trade_event,
            logger = {
                "log_event": self.recorder.log_event,
                "log_orderbook_snapshot": self.recorder.log_orderbook_snapshot
            },
            params=params
        )

        # Runtime Control
        self.running = False
        self.threads = []

    def start(self):
        self.running = True
        
        self.react_dashboard.start()

        self.engine.start_rich_dashboard()
        self.feed.start()

        self.start_execution_loop()

        self.start_react_dashboard_loop()
        self.start_rich_dashboard_loop()

    def start_execution_loop(self):

        def exec_loop():
            while self.running:
                self.engine.execution_cycle()
                time.sleep(0.05)  # 20Hz execution

        t = threading.Thread(target=exec_loop, daemon=True)
        t.start()

        self.threads.append(t)

    def start_rich_dashboard_loop(self):

        def rich_loop():
            while self.running:
                self.engine.update_dashboard()
                time.sleep(0.02) # 50Hz dashboard refresh

        t = threading.Thread(target=rich_loop,daemon=True)
        t.start()

        self.threads.append(t)

    def start_react_dashboard_loop(self):

        def react_loop():
            while self.running:
                snapshot = self.engine.build_snapshot()

                self.react_dashboard.publish({
                    "type": "snapshot",
                    "data": snapshot
                })

                time.sleep(0.02) # 50 Hz dashboard websockets

        t = threading.Thread(target=react_loop, daemon=True)
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

    def generate_datasets(self):

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

        # 5. export datasets
        self.recorder.export_run()


    def shutdown(self):

        self.running = False
        self.feed.stop()

        for t in self.threads:
            t.join(timeout=1)

        self.generate_datasets()


def load_manifest(path):
    with open(path, "r") as f:
        return json.load(f)
    
if __name__ == "__main__":

    params = load_manifest(r"data\manifest_live.json")
    # params = load_manifest(r"data\runs\run_20260529_073307\manifest.json")

    system = TradingSystem(params=params)
    
    system.start()

    system.run_forever()