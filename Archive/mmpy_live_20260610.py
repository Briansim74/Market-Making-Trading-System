import os
import time
import json
import math
import heapq
import queue
import random
import joblib
import asyncio
import uvicorn
import requests
import threading
import websocket
import itertools
import numpy as np
import pandas as pd
import xgboost as xgb
from typing import Set
from scipy.stats import spearmanr
from fastapi import FastAPI, WebSocket
from datetime import datetime, timezone
from sortedcontainers import SortedDict
from collections import deque, defaultdict
from sklearn.preprocessing import StandardScaler

from rich import box
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

import time
import hmac
import hashlib
import requests
import websocket
import threading
import json


import os
import time
import json
import math
import heapq
import queue
import random
import joblib
import asyncio
import uvicorn
import requests
import threading
import websocket
import itertools
import numpy as np
import pandas as pd
import xgboost as xgb
from typing import Set
from scipy.stats import spearmanr
from fastapi import FastAPI, WebSocket
from datetime import datetime, timezone
from sortedcontainers import SortedDict
from collections import deque, defaultdict
from sklearn.preprocessing import StandardScaler

from rich import box
from rich.live import Live
from rich.table import Table
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
    
    # def initialize_from_binance(self, symbol, limit):

    #     # url = (
    #     #     f"https://api.binance.com/api/v3/depth" # spot
    #     #     f"?symbol={symbol}&limit={limit}"
    #     # )

    #     url = (
    #         f"https://testnet.binancefuture.com/fapi/v1/depth" # testnet futures
    #         f"?symbol={symbol}&limit={limit}"
    #     )

    #     snapshot = requests.get(url).json()

    #     self.last_update_id = snapshot["lastUpdateId"]

    #     bids = {
    #         self.config.to_tick(float(p)): float(q)
    #         for p, q in snapshot["bids"]
    #     }

    #     asks = {
    #         self.config.to_tick(float(p)): float(q)
    #         for p, q in snapshot["asks"]
    #     }

    #     self.bids = SortedDict(bids)
    #     self.asks = SortedDict(asks)

    #     print("ORDER BOOK INITIALIZED")

    #     return self.last_update_id, snapshot
    def initialize_from_binance(self, symbol, limit):

        url = (
            f"https://testnet.binancefuture.com/fapi/v1/depth"
            f"?symbol={symbol}&limit={limit}"
        )

        snapshot = requests.get(url, timeout=3).json()

        if "lastUpdateId" not in snapshot:
            raise RuntimeError("Snapshot invalid")

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
        self.last_update_id = self.last_update_id

        print("SNAPSHOT READY:", self.last_update_id)

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
import threading

class SyncState:
    WAIT_SNAPSHOT = "WAIT_SNAPSHOT"
    BUFFERING = "BUFFERING"
    LIVE = "LIVE"
    RESYNC = "RESYNC"


class BinanceFSM:
    def __init__(self, state, on_market_data, logger):
        self.state = state
        self.on_market_data = on_market_data

        self.log_event = logger["log_event"]

        # snapshot / sequencing
        self.snapshot_id = None
        self.last_update_id = None
        self.expected_U = None

        # state machine
        self.sync_state = SyncState.WAIT_SNAPSHOT

        # buffering BEFORE anchor only
        self.buffer = []
        self.buffer_lock = threading.Lock()

        # resync control
        self.resync_counter = 0
        self.max_resync = 5

    # -------------------------
    # SNAPSHOT INITIALIZATION
    # -------------------------
    def initialize_snapshot(self, snapshot):
        self.snapshot_id = snapshot["lastUpdateId"]

        self.last_update_id = self.snapshot_id
        self.expected_U = self.snapshot_id + 1

        self.sync_state = SyncState.BUFFERING

        print(f"[FSM] Snapshot loaded: {self.snapshot_id}")

    # -------------------------
    # ENTRY POINT FROM FEED
    # -------------------------
    def on_depth(self, msg):
        U = msg["U"]
        u = msg["u"]

        # 1. WAIT STATE (should never last long)
        if self.sync_state == SyncState.WAIT_SNAPSHOT:
            print(1)
            return

        # 2. BUFFER UNTIL ANCHOR FOUND
        if self.sync_state == SyncState.BUFFERING:
            print(2)
            if self.last_update_id is None:
                print(3)
                return

            # drop stale
            if u <= self.last_update_id:
                print(4)
                return

            # store until we find anchor
            if U <= self.last_update_id + 1 <= u:
                self._apply(msg)
                self.last_update_id = u
                self.expected_U = u + 1

                self.sync_state = SyncState.LIVE

                print("[FSM] BOOK ANCHORED → LIVE")
                self.on_market_data()
                return
            print(5)
            return

        # 3. LIVE MODE (strict sequence)
        if self.sync_state == SyncState.LIVE:

            # stale drop
            if u <= self.last_update_id:
                return

            # STRICT GAP CHECK
            if U != self.expected_U:
                print(f"[FSM] GAP DETECTED U={U} expected={self.expected_U}")
                self.trigger_resync()
                return

            self._apply(msg)

            self.last_update_id = u
            self.expected_U = u + 1

            self.on_market_data()
            return

        # 4. RESYNC MODE (ignore everything)
        if self.sync_state == SyncState.RESYNC:
            return

    # -------------------------
    # APPLY ORDERBOOK DELTA
    # -------------------------
    def _apply(self, msg):
        bids, asks = self._parse(msg)

        self.state.update_queue_from_depth(
            bids=bids,
            asks=asks
        )

        self.state.market_book.apply_delta(bids, asks, self.state)

        self.state.update_market_feature_state()
        self.state.update_vol()
        self.state.compute_order_imbalance()

    # -------------------------
    # PARSER
    # -------------------------
    def _parse(self, msg):
        bids = [(float(p), float(q)) for p, q in msg.get("b", [])]
        asks = [(float(p), float(q)) for p, q in msg.get("a", [])]
        return bids, asks

    # -------------------------
    # RESYNC LOGIC
    # -------------------------
    def trigger_resync(self):
        self.resync_counter += 1

        print(f"[FSM] RESYNC #{self.resync_counter}")

        if self.resync_counter > self.max_resync:
            raise RuntimeError("Too many resyncs → feed killed")

        self.sync_state = SyncState.RESYNC

        # reset sequencing
        self.last_update_id = None
        self.expected_U = None

        # go back to buffering
        self.sync_state = SyncState.WAIT_SNAPSHOT

    # -------------------------
    # OPTIONAL: external snapshot reset
    # -------------------------
    def reset_with_snapshot(self, snapshot):
        self.initialize_snapshot(snapshot)

class BinanceFeed:
    def __init__(self, state, fsm, instrument, logger):
        self.state = state
        self.fsm = fsm
        self.instrument = instrument

        self.log_event = logger["log_event"]

        self.depth_socket = None
        self.trade_socket = None

        self.depth_thread = None
        self.trade_thread = None

    # -------------------------
    # START SYSTEM
    # -------------------------
    def start(self):

        print("[FEED] Starting sockets...")

        depth_socket = f"wss://stream.binancefuture.com/ws/{self.instrument}@depth@100ms"
        trade_socket = f"wss://stream.binancefuture.com/ws/{self.instrument}@trade"

        self.depth_socket = websocket.WebSocketApp(
            depth_socket,
            on_message=self._on_depth_message
        )

        self.trade_socket = websocket.WebSocketApp(
            trade_socket,
            on_message=self._on_trade_message
        )

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

        print("[FEED] WebSockets running")

    # -------------------------
    # DEPTH CALLBACK
    # -------------------------
    def _on_depth_message(self, ws, message):
        msg = json.loads(message)

        self.log_event({
            "type": "depth",
            "ts": msg["E"],
            "message": message
        })

        self.fsm.on_depth(msg)

    # -------------------------
    # TRADE CALLBACK (optional)
    # -------------------------
    def _on_trade_message(self, ws, message):
        msg = json.loads(message)
        # forward if needed
        pass

    # -------------------------
    # STOP
    # -------------------------
    def stop(self):
        print("[FEED] Stopping...")

        if self.depth_socket:
            self.depth_socket.close()

        if self.trade_socket:
            self.trade_socket.close()

        if self.depth_thread:
            self.depth_thread.join(timeout=2)

        if self.trade_thread:
            self.trade_thread.join(timeout=2)

        print("[FEED] Stopped")


# Strategy Layer
class RegimeModel:
    def __init__(self, artifact):
        self.scaler = artifact["scaler"]
        self.model = artifact["model"]
        self.feature_cols = artifact["feature_cols"]
        self.n_regimes = artifact["n_regimes"]
        self.window = artifact["window"]
        self.regime_labels = artifact["regime_labels"]
    
    def predict(self, X):

        # X must be dict with same keys as feature_cols
        x_vec = np.array([[X[col] for col in self.feature_cols]])

        x_scaled = self.scaler.transform(x_vec)

        regime_id = self.model.predict(x_scaled)[0]
        regime = self.regime_labels[regime_id]
        prob = np.max(self.model.predict_proba(x_scaled)[0])

        return regime, regime_id, prob

class MicroSignalModel:
    def __init__(self, artifact):
        self.model = artifact["model"]
        self.target = artifact["target"]
        self.horizon_ms = artifact["horizon_ms"]
        self.beta = artifact["beta"] # beta = 14.44 # 100ms signal
        self.ic = artifact["ic"] # sqrt(r2), shrinkage factor for beta

    def predict(self, features):

        micro_signal = (features["microprice"] - features["mid"]) / features["mid"]

        fair_bias = self.ic * self.beta * micro_signal # dimensionless micro_signal scaled with beta

        return fair_bias

class MLModel:
    def __init__(self, artifact):
        self.model = artifact["model"]
        self.feature_cols = artifact["feature_cols"]
        self.target = artifact["target"]
        self.horizon_ms = artifact["horizon_ms"]

    def predict(self, features):

        X = np.empty((1, len(self.feature_cols)), dtype=np.float32)

        for i, k in enumerate(self.feature_cols):
            X[0, i] = features[k]

        # X = np.array(
        #     [[features[k] for k in self.feature_cols]],
        #     dtype=np.float32
        # )

        return float(self.model.predict(X)[0]) # json reads float, not np.float32

    # def predict(self, features):

    #     X = [features[k] for k in self.feature_cols]

    #     return self.model.predict([X])[0] # Important: XGBoost expects 2D input.

class MarketMakingStrategy:
    def __init__(self, config, params):
        self.config = config
        self.params = params
        self.gamma = self.params["gamma"]

        # Micro Signal Model (LinearRegression), Edge Model (XGBoost), Regime Model (GaussianMixture)
        self.struct_model = self.params["models"]["struct_model"]
        self.micro_signal_model = self._load_model("micro_signal_model")
        self.edge_model = self._load_model("edge_model")
        self.regime_model = self._load_model("regime_model")
        self.toxicity_model = self._load_model("toxicity_model")

    def _load_model(self, model):

        MODELS = {
            "micro_signal_model": MicroSignalModel,
            "edge_model": MLModel,
            "regime_model": RegimeModel,
            "toxicity_model": MLModel,
        }

        if self.params["models"][model] == "":
            return None

        path = f"{os.path.join(self.params["folder_path"], self.params["models"][model])}.pkl"

        artifact = joblib.load(path)

        print('INITIALIZED', model)

        return MODELS[model](artifact)

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
    
    def compute_fair_and_micro(self, state, policy):

        book = state.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)

        microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9)

        order_imbalance = state.order_imbalance # order imbalance, resting flow
        trade_imbalance = state.trade_imbalance # trade imbalance, aggressitve trade flow

        # Your best estimate of where the mid should move next, given order flow
        fair = microprice + policy["alpha_order_imb"] * order_imbalance + policy["alpha_trade_imb"] * trade_imbalance

        return fair, microprice
    
    def compute_spread(self, features, policy, toxicity):

        sigma = features["volatility"]

        base = 0.03
        vol_component = 3 * sigma

        raw_spread = max(base, vol_component)
        
        spread = raw_spread * policy["spread_multiplier"] * (1.0 + toxicity["k1"] * toxicity["tox"])

        return spread
    
    def compute_skew(self, state, policy):

        sigma = state.get_vol()

        mid = state.market_book.mid()

        effective_inventory = state.inventory - policy["inventory_target"]

        # long inv, skew pushes fair downward, more willing to sell
        return -effective_inventory * self.gamma * sigma * mid

    def compute_signal_quality(self, state):

        mfs = state.market_feature_state

        if len(mfs.ml_signal_log) < 20:
            return 1.0  # neutral until enough data

        data = list(mfs.ml_signal_log)

        preds = np.array([x["pred"] for x in data])
        rets  = np.array([x["realized"] for x in data])

        # --------------------------
        # 1. correlation (true IC)
        # --------------------------
        if np.std(preds) < 1e-8 or np.std(rets) < 1e-8:
            ic = 0.0 # make it 0.0 instead of nan
        else:
            ic = np.corrcoef(preds, rets)[0, 1]
            ic = 0.0 if np.isnan(ic) else ic

        # --------------------------
        # 2. rank IC (more robust)
        # --------------------------
        if np.std(preds) < 1e-8 or np.std(rets) < 1e-8:
            rank_ic = 0.0
        else:
            rank_ic = spearmanr(preds, rets).statistic
            rank_ic = 0.0 if np.isnan(rank_ic) else rank_ic

        # --------------------------
        # 3. directional accuracy
        # --------------------------
        hit_rate = np.mean(np.sign(preds) == np.sign(rets))
        directional_score = (hit_rate - 0.5) * 2  # [-1, 1]

        # --------------------------
        # 4. stability penalty (important in MM)
        # --------------------------
        pred_vol = np.std(preds)
        stability_penalty = np.exp(-pred_vol * 50)  # tune factor

        # --------------------------
        # 5. final score
        # --------------------------
        raw = 0.5 * ic + 0.3 * rank_ic + 0.2 * directional_score

        signal_quality = stability_penalty * np.tanh(3 * raw)

        # map to usable range
        return 0.3 + 1.4 * ((signal_quality + 1) / 2)
    
    def detect_regime(self, features):
        
        if self.regime_model == None:

            policy = {
                "regime": "no_model",
                "regime_id": -1.0,
                "regime_prob": 0.0,
                "alpha_order_imb": 0.2, # Trust in order-book imbalance
                "alpha_trade_imb": 0.05, # Trust in trade flow
                "alpha_struct": 0.3, # Trust in reservation price
                "spread_multiplier": 1.0, # How aggressively to provide liquidity
                "k0": 0.5,
                "inventory_target": 0.0 # Desired directional inventory
            }

            return policy
        
        """
        Option C: HMM (more advanced, very good)

        Hidden Markov Model:

        state = regime
        emissions = market features

        This is widely used in real trading systems.

        """

        regime, regime_id, prob = self.regime_model.predict(features)

        if regime == "trending": # Regime 0
            alpha_order_imb = 0.6
            alpha_trade_imb = 0.2
            alpha_struct = 0.8 # trust fair value model strongly
            spread_multiplier = 2.0
            k0 = 1.2
            inventory_target = np.sign(features["trade_imbalance"]) # stay long if long imbalance, etc, controlled directional LP
        
        elif regime == "toxic": # Regime 1 — LOW VOL (silent competition regime)
            alpha_order_imb = 0.05
            alpha_trade_imb = 0.01
            alpha_struct = 0.4 # rely more on model, less on microstructure
            spread_multiplier = 1.5  # widen aggressively
            k0 = 0.5
            inventory_target = 0.7 # reduce model aggressiveness
        
        elif regime == "low_vol": # Regime 2 — low-vol (your money regime), This is where MM should be most active and balanced
            alpha_order_imb = 0.15
            alpha_trade_imb = 0.05
            alpha_struct = 0.2 # default is 0.3
            spread_multiplier = 0.7 # tight spreads
            k0 = 1.0 # allow ml to participate fully
            inventory_target = 0.0

        policy = {
            "regime": regime,
            "regime_id": regime_id,
            "regime_prob": prob,
            "alpha_order_imb": alpha_order_imb,
            "alpha_trade_imb": alpha_trade_imb,
            "alpha_struct": alpha_struct,
            "spread_multiplier": spread_multiplier,
            "k0": k0,
            "inventory_target": inventory_target,
            "residual_mid": 0.0,
            "micro_residual":  0.0
        }
    
        return policy
    
    def compute_struct_delta(self, features, policy):

        if self.struct_model != "blended_AS":
            return 0.0

        """
        struct_delta = adjustment to fair value from market microstructure + risk

        It represents:
        - inventory risk (gamma/vol skew)
        - order book imbalance pressure
        - trade flow / short-term liquidity pressure
        - queue / execution asymmetry effects

        Interpretation:
        “Given current market structure, where should the fair price be right now?”

        Time horizon:
        → instantaneous / very short-term equilibrium (0-horizon)

        Role:
        → builds the baseline reservation price for quoting
        → controls adverse selection + inventory risk

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
        
        Why A-S works here

        Because:
        The best predictor of short-term price is still “fair value + inventory pressure”

        Blended Avellaneda Stoikov model
        """
        reservation = features["fair"] + features["skew"] # fair value + inventory risk to adjust quoting center

        struct_center = features["mid"] + policy["alpha_struct"] * (reservation - features["mid"])

        struct_delta = struct_center - features["mid"]  # new center with mid as baseline

        return struct_delta
        
    def compute_micro_signal_delta(self, features):

        if self.micro_signal_model == None:
            return 0.0

        fair_bias = self.micro_signal_model.predict(features)

        micro_signal_delta = features["mid"] * fair_bias

        return micro_signal_delta
    
    def compute_ml_delta(self, state, struct_delta, micro_signal_delta, features, policy):

        """
        ml_delta = expected directional drift from current equilibrium

        It represents:
        - learned short-horizon price movement
        - residual inefficiencies not explained by MM-core
        - predictive imbalance in order flow dynamics

        Interpretation:
        “Given current market state, how will price move away from current center?”
        
        """

        if self.edge_model == None: # no edge_model active
            return 0.0, 0.0
        
        reservation = features["mid"] + struct_delta + micro_signal_delta
        
        expected_return = self.edge_model.predict(features) # expected return given market state in log space

        signal_quality = self.compute_signal_quality(state) # get signal accuracy

        # store prediction
        state.market_feature_state.ml_predictions.append({
            "ts": state.last_depth_ts,
            "pred": expected_return,
            "reservation": reservation,
        })

        effective_k = policy["k0"] * signal_quality

        ml_center = reservation * np.exp(expected_return * effective_k) # regime multiplier, k > 1, scale up

        ml_delta = ml_center - reservation

        return ml_delta, signal_quality

    
    def compute_toxicity(self, features):

        toxicity = {
            "tox": 0.0,
            "k1": 0.2, # spread tox multiplier k1 = 0.15 to 0.30 (0.2)
            "k2": 0.357  # order size tox multiplier k2 = 0.2 to 0.5 (0.357)
        }

        if self.toxicity_model == None: # no toxicity_model active
            return toxicity
        
        prediction = self.toxicity_model.predict(features) # E[markout | state]

        toxicity["tox"] = -prediction # bad markout < 0, tox > 0

        return toxicity
    
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

        # 1. Regime detection
        regime = state.get_regime()
        policy = self.detect_regime(regime)

        fair, microprice = self.compute_fair_and_micro(state, policy)
        skew = self.compute_skew(state, policy)

        features = {
            "mid": mid,
            "fair": fair,
            "skew": skew,
            "microprice": microprice,
            "microprice_dev": microprice - mid,
            "spread": best_ask - best_bid,
            "order_imbalance": state.order_imbalance,
            "trade_imbalance": state.trade_imbalance,
            "inventory": state.inventory,
            "volatility": state.get_vol(),
            "queue_ahead_bid": state.compute_queue_ahead("bids", self.config.to_tick(best_bid)),
            "queue_ahead_ask": state.compute_queue_ahead("asks", self.config.to_tick(best_ask)),
        }

        # Alpha Generation
        struct_delta = self.compute_struct_delta(features, policy) # blended AS model
        micro_signal_delta = self.compute_micro_signal_delta(features) # adjusted to 100ms microprice adjustment signal
        ml_delta, signal_quality = self.compute_ml_delta(state, struct_delta, micro_signal_delta, features, policy) # alpha model, any residual mispricing

        center = mid + struct_delta + micro_signal_delta + ml_delta # mid value anchored, more stable than microprice as an anchor

        # Toxicity filter, to adjust spread and quoting size
        toxicity = self.compute_toxicity(features) # adverse selection

        # 3. Spread (your model or fallback to market spread)
        spread = self.compute_spread(features, policy, toxicity)

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

        bid_delta = abs(best_bid - state.market_feature_state.prev_best_bid)
        ask_delta = abs(best_ask - state.market_feature_state.prev_best_ask)

        signal = {
            # market
            "mid": mid,
            "microprice": microprice,
            "spread": best_ask - best_bid,
            "best_bid": best_bid,
            "best_ask": best_ask,

            # microstructure
            "order_imbalance": features["order_imbalance"],
            "trade_imbalance": features["trade_imbalance"],
            "volatility": features["volatility"],
            "queue_ahead_bid": features["queue_ahead_bid"],
            "queue_ahead_ask": features["queue_ahead_ask"],

            # risk
            "inventory": features["inventory"],
            "realized_pnl": state.realized_pnl,
            "unrealized_pnl": state.get_unrealized_pnl(mid),
            "total_pnl": state.get_pnl(),
            "equity": state.cash + state.inventory * mid,

            # model internals
            "fair": fair,
            "skew": skew,
            "struct_delta": struct_delta,
            "micro_signal_delta": micro_signal_delta,
            "reservation": center,

            # policy
            "regime": policy["regime"],
            "regime_id": policy["regime_id"],
            "regime_prob": policy["regime_prob"],  # optional if your model outputs probabilities
            "alpha_order_imb": policy["alpha_order_imb"],
            "alpha_trade_imb": policy["alpha_trade_imb"],
            "alpha_struct": policy["alpha_struct"], # policy["alpha"] # to check how to get last_signal since we dont use this anymore
            "k0": policy["k0"],
            "spread_multiplier": policy["spread_multiplier"],
            "inventory_target": policy["inventory_target"],
            "signal_quality": signal_quality,
            "toxicity": toxicity,

            # market feature state
            "bid_delta": bid_delta, # check why quote churn is 0.0
            "ask_delta": ask_delta,
            "quote_churn": bid_delta + ask_delta,

            # action
            "my_bid": bid,
            "my_ask": ask
        }

        return signal
    
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
class DatasetRecorder:
    def __init__(self, config, state, params):
        self.config = config
        self.state = state
        self.params = params
        
        self.rows = []
        self.trades = []
        self.quotes = []
        self.fills = []

        # for replayfeed
        self.events = []
        self.orderbook_snapshot = None

    def log_event(self, event):
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

    def log_snapshot(self, ts, signal, symbol):

        self.rows.append({
            "ts": ts,
            "symbol": symbol,

            # market
            "best_bid": signal["best_bid"],
            "best_ask": signal["best_ask"],
            "mid": signal["mid"],
            "microprice": signal["microprice"],
            
            "best_bid_tick": self.config.to_tick(signal["best_bid"]),
            "best_ask_tick": self.config.to_tick(signal["best_ask"]),
            "mid_tick": self.config.to_tick(signal["mid"]),

            "spread": signal["best_ask"] - signal["best_bid"],

            # microstructure
            "order_imbalance": signal["order_imbalance"],
            "trade_imbalance": signal["trade_imbalance"],
            "volatility": signal["volatility"],
            "queue_ahead_bid": signal["queue_ahead_bid"],
            "queue_ahead_ask": signal["queue_ahead_ask"],

            # risk
            "inventory": signal["inventory"],
            "realized_pnl": signal["realized_pnl"],
            "unrealized_pnl": signal["unrealized_pnl"],
            "total_pnl": signal["total_pnl"],
            "equity": signal["equity"],

            # model internals
            "fair": signal["fair"],
            "skew": signal["skew"],
            "struct_delta": signal["struct_delta"],
            "micro_signal_delta": signal["micro_signal_delta"],
            "reservation": signal["reservation"],
            "alpha_order_imb": signal["alpha_order_imb"],
            "alpha_trade_imb": signal["alpha_trade_imb"],
            "alpha_struct": signal["alpha_struct"],

            # action
            "my_bid": signal["my_bid"],
            "my_ask": signal["my_ask"],

            "my_bid_tick": self.config.to_tick(signal["my_bid"]),
            "my_ask_tick": self.config.to_tick(signal["my_ask"]),

            # execution geometry
            # aggressiveness vs touch
            "bid_distance_touch": signal["my_bid"] - signal["best_bid"],
            "ask_distance_touch": signal["my_ask"] - signal["best_ask"],

            # position inside spread
            "bid_distance_spread": signal["my_bid"] - signal["best_ask"],
            "ask_distance_spread": signal["my_ask"] - signal["best_bid"],

            # quote churn
            "bid_delta": signal["bid_delta"],
            "ask_delta": signal["ask_delta"],
            "quote_churn": signal["quote_churn"],
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

    def log_trade(self, trade, symbol):

        book = self.state.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)
        mid = (best_bid + best_ask) / 2

        microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9)

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
            "microprice": microprice,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": best_ask - best_bid,

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

    def log_quote(self, ts, order, side, event_type, price):

        signal = order.metadata["signal"]
        
        self.quotes.append({
            # Event
            "ts": ts,
            "order_id": order.order_id,
            "side": side,
            "event_type": event_type,

            # Quote
            "price": price,
            "price_tick": order.price,
            "qty": order.qty,

            # Market State
            "mid": signal["mid"],
            "microprice": signal["microprice"],
            "spread": signal["spread"],
            "best_bid": signal["best_bid"],
            "best_ask": signal["best_ask"],
            "order_imbalance": signal["order_imbalance"],
            "trade_imbalance": signal["trade_imbalance"],
            "volatility": signal["volatility"],

            # Quote Geometry
            "distance_to_mid": price - signal["mid"],
            "distance_to_touch": (
                price - signal["best_bid"]
                if order.side == "BUY"
                else price - signal["best_ask"]
            ),

            # Execution / Microstructure
            "queue_ahead_at_join": order.metadata["queue_ahead_at_join"],

            # Risk
            "inventory": signal["inventory"],

            # Model Internals
            "fair": signal["fair"],
            "skew": signal["skew"],
            "reservation": signal["reservation"],
            "alpha_order_imb": signal["alpha_order_imb"],
            "alpha_trade_imb": signal["alpha_trade_imb"],
            "alpha_struct": signal["alpha_struct"]
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

    def log_fill(self, qty, order, is_maker):

        book = self.state.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)

        volatility_at_fill = self.state.get_vol()
        signal = order.metadata["signal"]

        self.fills.append({
            # execution
            "ts": self.state.last_trade_ts,
            "side": order.side,
            "price": self.config.from_tick(order.price),
            "price_tick": order.price,
            "qty": qty,

            # execution classification
            "is_maker": is_maker,
            "fill_type": "BID_HIT" if order.side == "BUY" else "ASK_LIFT",
            "fill_status": "PARTIAL" if order.remaining > 0 else "FULL",

            # portfolio state
            "inventory": self.state.inventory,

            # decision time snapshot from last signal, for toxicity modelling
            "mid": signal["mid"],
            "microprice": signal["microprice"],
            "microprice_dev": signal["microprice"] - signal["mid"],
            "spread": signal["spread"],
            "order_imbalance": signal["order_imbalance"],
            "trade_imbalance": signal["trade_imbalance"],
            "volatility": signal["volatility"],
            "volatility_bps": signal["volatility"] * 10000,
            "queue_ahead_bid": signal["queue_ahead_bid"],
            "queue_ahead_ask": signal["queue_ahead_ask"],

            # market state at fill
            "mid_at_fill": (best_bid + best_ask) / 2,
            "spread_at_fill": best_ask - best_bid,
            "volatility_at_fill": volatility_at_fill,
            "volatility_at_fill_bps": volatility_at_fill * 10000,

            # quote geometry
            "my_bid": signal["my_bid"],
            "my_ask": signal["my_ask"],
            "bid_distance_touch": signal["my_bid"] - best_bid,
            "ask_distance_touch": signal["my_ask"] - best_ask,
            "bid_distance_spread": signal["my_bid"] - best_ask,
            "ask_distance_spread": signal["my_ask"] - best_bid,

            # microstructure
            "queue_ahead_at_join": order.metadata["queue_ahead_at_join"],
    })
        
    # "ts": ts,
    #         "symbol": symbol,

    #         # market
    #         "best_bid": signal["best_bid"],
    #         "best_ask": signal["best_ask"],
    #         "mid": signal["mid"],
    #         "microprice": signal["microprice"],
            
    #         "best_bid_tick": self.config.to_tick(signal["best_bid"]),
    #         "best_ask_tick": self.config.to_tick(signal["best_ask"]),
    #         "mid_tick": self.config.to_tick(signal["mid"]),

    #         "spread": signal["best_ask"] - signal["best_bid"],

    #         # microstructure
    #         "order_imbalance": signal["order_imbalance"],
    #         "trade_imbalance": signal["trade_imbalance"],
    #         "volatility": signal["volatility"],
    #         "queue_ahead_bid": signal["queue_ahead_bid"],
    #         "queue_ahead_ask": signal["queue_ahead_ask"],

    #         # risk
    #         "inventory": signal["inventory"],
    #         "realized_pnl": signal["realized_pnl"],
    #         "unrealized_pnl": signal["unrealized_pnl"],
    #         "total_pnl": signal["total_pnl"],
    #         "equity": signal["equity"],

    #         # model internals
    #         "fair": signal["fair"],
    #         "skew": signal["skew"],
    #         "reservation": signal["reservation"],
    #         "alpha_order_imb": signal["alpha_order_imb"],
    #         "alpha_trade_imb": signal["alpha_trade_imb"],
    #         "alpha_struct": signal["alpha_struct"],

    #         # action
    #         "my_bid": signal["my_bid"],
    #         "my_ask": signal["my_ask"],

    #         "my_bid_tick": self.config.to_tick(signal["my_bid"]),
    #         "my_ask_tick": self.config.to_tick(signal["my_ask"]),

    #         # execution geometry
    #         # aggressiveness vs touch
    #         "bid_distance_touch": signal["my_bid"] - signal["best_bid"],
    #         "ask_distance_touch": signal["my_ask"] - signal["best_ask"],

    #         # position inside spread
    #         "bid_distance_spread": signal["my_bid"] - signal["best_ask"],
    #         "ask_distance_spread": signal["my_ask"] - signal["best_bid"],

    #         # quote churn
    #         "bid_delta": signal["bid_delta"],
    #         "ask_delta": signal["ask_delta"],
    #         "quote_churn": signal["quote_churn"],

    def create_run_dir(self, base="data/runs"):
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_path = os.path.join(base, f"run_{run_id}")

        os.makedirs(run_path, exist_ok=True)

        return run_path, run_id

    def export_run(self, base_path=r"data\runs"):
        self.state.update_performance()

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

        manifest = dict(self.params)
        
        manifest["run_id"] = run_id
        manifest["folder_path"] = run_path
        manifest["performance"]["pnl"] = round(self.state.get_pnl(), 4)
        manifest["performance"]["sharpe"] = round(self.state.compute_sharpe(), 4)
        manifest["performance"]["fees_paid"] = round(self.state.fees_paid, 4)
        manifest["performance"]["fees_per_fill"] = round(self.state.fees_paid / len(self.fills), 4)
        manifest["performance"]["pnl_per_fill"] = round(self.state.get_pnl() / len(self.fills), 4)

        # 3. write manifest
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        print("DATASETS SAVED SUCCESSFULLY")
        print(f"Saved run → {run_path}")


# 1. Binance Broker (REAL API layer) 100–2000ms variable
class BinanceBroker:
    def __init__(self, config, params):
        self.config = config

        self.api_key = params["api"]["api_key"]
        self.api_secret = params["api"]["base_url"]
        self.base_url = params["api"]["base_url"] # "https://testnet.binancefuture.com"

        self.instrument = params["instrument"].upper()

        self.listen_key = None
        self.keepalive_running = False

    # -------------------------
    # listenKey
    # -------------------------
    def start_user_stream(self):
        url = self.base_url + "/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}

        r = requests.post(url, headers=headers)
        self.listen_key = r.json()["listenKey"]

        # start keepalive thread automatically
        self.start_keepalive_loop()

        return self.listen_key
    
    def keepalive_listen_key(self, listen_key):
        url = self.base_url + "/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}

        requests.put(url, headers=headers, params={"listenKey": listen_key})

    def start_keepalive_loop(self):
        self.keepalive_running = True

        def loop():
            while self.keepalive_running:
                try:
                    time.sleep(20 * 60)  # 20 minutes (safe margin)

                    url = self.base_url + "/fapi/v1/listenKey"
                    headers = {"X-MBX-APIKEY": self.api_key}

                    requests.put(
                        url,
                        headers=headers,
                        params={"listenKey": self.listen_key},
                        timeout=5
                    )

                    print("keepalive sent")

                except Exception as e:
                    print("keepalive error:", e)

        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def stop_keepalive(self):
        self.keepalive_running = False

    # -------------------------
    # REST signing
    # -------------------------
    def _sign(self, params: dict):
        query = "&".join([f"{k}={v}" for k, v in params.items()])

        # query = "&".join( # Binance sometimes rejects unsorted signatures under load.
        #     [f"{k}={params[k]}" for k in sorted(params)]
        # )
        
        return hmac.new(
            self.api_secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

    # -------------------------
    # Place order
    # -------------------------
    def place_limit(self, side, price, size, ts, signal):
        endpoint = "/fapi/v3/order"

        params = {
            "symbol": self.instrument,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": size,
            "price": price,
            "timestamp": ts,
            "recvWindow": 5000
        }

        params["signature"] = self._sign(params)

        headers = {"X-MBX-APIKEY": self.api_key}

        r = requests.post(self.base_url + endpoint, params=params, headers=headers)

        return r.json()

    def cancel_order(self, order):
        endpoint = "/fapi/v3/order"

        params = {
            "symbol": order.metadata["resp"]["symbol"],
            "orderId": order.order_id,
            "timestamp": int(time.time() * 1000)
        }

        params["signature"] = self._sign(params)

        headers = {"X-MBX-APIKEY": self.api_key}

        r = requests.delete(self.base_url + endpoint, params=params, headers=headers)
        return r.json()
    
    def place_market(self, side, size, ts):
        endpoint = "/fapi/v1/order"

        params = {
            "symbol": self.instrument,
            "side": side,
            "type": "MARKET",
            "quantity": size,
            "timestamp": ts,
            "recvWindow": 5000
        }

        params["signature"] = self._sign(params)
        headers = {"X-MBX-APIKEY": self.api_key}

        r = requests.post(self.base_url + endpoint, params=params, headers=headers)
        return r.json()

    # -------------------------
    # Open orders sync
    # -------------------------
    def get_open_orders(self, symbol):
        endpoint = "/fapi/v3/openOrders"

        params = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000)
        }

        params["signature"] = self._sign(params)

        headers = {"X-MBX-APIKEY": self.api_key}

        r = requests.get(self.base_url + endpoint, params=params, headers=headers)
        return r.json()

# 3. Live Execution Layer (your MM brain)
class LiveExecution:
    # def __init__(self, config, state, recorder):
    def __init__(self, config, state, broker, recorder, params):
        self.config = config
        self.params = params
        self.state = state
        self.broker = broker

        self.open_orders = {"BUY": None, "SELL": None}  # order_id -> Order
        self.open_orders_id = {}

        self.last_bid = None
        self.last_ask = None
        self.current_bid_size = 0.0
        self.current_ask_size = 0.0

        self.max_inv = 10
        
        self.recorder = recorder

        self.lock = threading.Lock() # Lock all mutations

        # # -------------------------
        # # latency model (ms)

        # # priority queue: (execute_ts, action)
        # # -------------------------
        # self.latency_ms = 50
        # self.latency_queue = []

        self.recorder = recorder


    def get_open_order(self, side):
        return self.open_orders[side]

    # -------------------------
    # RISK GUARD
    # -------------------------
    def risk_check(self):
        if abs(self.state.inventory) > self.max_inv:
            return False
        return True

    # -------------------------
    # MAIN QUOTE ENGINE
    # -------------------------
    def place_quotes(self, signal):

        """
        Selective repricing logic:
        - preserve queue priority when possible
        - only cancel when necessary
        """

        if not self.risk_check():
            return

        desired_bid = signal["my_bid"]
        desired_ask = signal["my_ask"]

        tick = self.config.tick_size

        order_sizes = self._compute_order_size(signal)
        ts = self.config.now_ms()

        bid_order = self.get_open_order("BUY")
        ask_order = self.get_open_order("SELL")

        bid_change = abs(desired_bid - self.last_bid) >= tick # bid might move 1 tick also, its possible
        ask_change = abs(desired_ask - self.last_ask) >= tick

        # -------------------------
        # BID SIDE
        # -------------------------
        if bid_order is None: # quote new   place_limit(self, side, price, size, ts, signal):
            bid_resp = self.broker.place_limit("BUY", price=desired_bid, size=order_sizes["bid_size"], ts=ts, signal=signal)

            order = Order(
                order_id=bid_resp["orderId"],
                side="BUY",
                price=self.config.to_tick(desired_bid),
                qty=order_sizes["bid_size"],
                status="PENDING_NEW",
                ts=bid_resp["transactTime"],
                owner="self",
                signal=signal,
                queue_ahead_at_join=None,
                resp=bid_resp
            )

            self.recorder.log_quote(
                ts=ts,
                order=bid_order,
                side="BID",
                event_type="NEW_SUBMITTED",
                price=desired_bid,
            )

            with self.lock:
                self.open_orders["BUY"] = order
                self.open_orders_id[order.order_id] = order

        elif bid_change and bid_order.status not in ("PENDING_NEW", "PENDING_CANCEL"): # if we need to requote and our order is currently live already
            bid_resp = self.broker.cancel_order(bid_order)

            self.recorder.log_quote(
                ts=ts,
                order=bid_order,
                side="BID",
                event_type="CANCEL_SUBMITTED",
                price=desired_bid,
            )

            with self.lock:
                bid_order.status = "PENDING_CANCEL"

        # -------------------------
        # ASK SIDE
        # -------------------------
        if ask_order is None:
            # self.broker.place_limit(self.config.symbol, "SELL", qty=1, price=desired_ask)
            ask_resp = self.broker.place_limit("SELL", price=desired_ask, size=order_sizes["ask_size"], ts=ts, signal=signal)

            order = Order(
                order_id=ask_resp["orderId"],
                side="SELL",
                price=self.config.to_tick(desired_ask),
                qty=order_sizes["ask_size"],
                status="PENDING_NEW",
                ts=ask_resp["transactTime"],
                owner="self",
                signal=signal,
                queue_ahead_at_join=None,
                resp=ask_resp
            )

            self.recorder.log_quote(
                ts=ts,
                order=ask_order,
                side="ASK",
                event_type="NEW_SUBMITTED",
                price=desired_ask,
            )

            with self.lock:
                self.open_orders["SELL"] = order
                self.open_orders_id[order.order_id] = order

        elif ask_change and ask_order.status not in ("PENDING_NEW", "PENDING_CANCEL"): # if we need to requote
            ask_resp = self.broker.cancel_order(ask_order)

            self.recorder.log_quote(
                ts=ts,
                order=ask_order,
                side="ASK",
                event_type="CANCEL_SUBMITTED",
                price=desired_ask,
            )

            with self.lock:
                ask_order.status = "PENDING_CANCEL"

        # self.last_bid = desired_bid
        # self.last_ask = desired_ask to put in binanceuserstream

# 4. User Data Stream (THIS is what makes it “real”)
class BinanceUserStream:
    def __init__(self, state, execution, broker, recorder):
        self.state = state
        self.execution = execution
        self.broker = broker
        self.recorder = recorder


    def start(self):
        listen_key = self.broker.start_user_stream()

        url = f"wss://stream.binancefuture.com/ws/{listen_key}"

        ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message
        )

        # NON-BLOCKING FIX (put it here)
        threading.Thread(
            target=ws.run_forever,
            daemon=True
        ).start()

    def on_message(self, ws, msg):
        data = json.loads(msg)

        if data["e"] == "ORDER_TRADE_UPDATE":
            o = data["o"]

            oid = o["i"]
            side = o["S"]
            status = o["X"]
            exec_type = o["x"]
            ts = o["T"]

            order = self.execution.open_orders_id.get(oid)

            if not order:
                return

            # -------------------------
            # NEW (LIVE CONFIRMATION)
            # -------------------------
            if exec_type == "NEW":
                
                order.status = "LIVE"

                self.state.last_order_update = order
                price = float(o["p"])
                qty = o["q"]

                if side == "BUY": # update last bid here for confirmed live orders
                    self.recorder.log_quote(
                        ts=ts,
                        order=order,
                        side="BUY",
                        event_type="NEW",
                        price=price,
                    )
                    
                    self.execution.last_bid = price
                    self.execution.current_bid_size = qty

                elif side == "SELL":
                    self.recorder.log_quote(
                        ts=ts,
                        order=order,
                        side="ASK",
                        event_type="CANCEL_SUBMITTED",
                        price=price,
                    )
                    
                    self.execution.last_ask = price
                    self.execution.current_ask_size = qty

            # -------------------------
            # CANCELED (REMOVE SLOT)
            # -------------------------
            elif exec_type == "CANCELED":

                order.status = "CANCELED"
                self.state.last_order_update = order

                self.execution.orders_by_id.pop(oid, None)

                if self.execution.open_orders[side] and self.execution.open_orders[side].order_id == oid:
                    self.execution.open_orders[side] = None

            elif exec_type == "REJECTED":

                order.status = "REJECTED"
                self.state.last_order_update = order
        
                self.execution.open_orders[side] = None

                if self.execution.open_orders[side] and self.execution.open_orders[side].order_id == oid:
                    self.execution.open_orders[side] = None

            elif exec_type == "TRADE":
                if status == "PARTIALLY_FILLED":
                
                    fill_price = float(o["L"])
                    fill_qty = float(o["l"])

                    order.remaining -= fill_qty

                    self.state.on_fill(
                        price=fill_price,
                        qty=fill_qty,
                        side=side
                    )

                    self.state.last_order_update = order
                    self.recorder.log_fill(qty=fill_qty, order=order, is_maker=True)

                # -------------------------
                # FILLED (REMOVE + PnL)
                # -------------------------
                elif status == "FILLED": # either live order gets filled or pending_cancel order gets filled
                    fill_price = float(o["L"])
                    fill_qty = float(o["l"])

                    order.remaining = 0
                    order.status = "FILLED"

                    self.state.on_fill(
                        price=fill_price,
                        qty=fill_qty,
                        side=side
                    )
                    
                    self.state.last_order_update = order
                    self.recorder.log_fill(qty=fill_qty, order=order, is_maker=True)

                    self.execution.orders_by_id.pop(oid, None)

                    if self.execution.open_orders[side] and self.execution.open_orders[side].order_id == oid:
                        self.execution.open_orders[side] = None

# Execution Layer
class Order:
    def __init__(self, order_id, side, price, qty, status, ts, owner, signal, queue_ahead_at_join, resp=None):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.qty = qty
        self.remaining = qty
        self.status = status
        self.timestamp = ts
        self.owner = owner
        self.metadata = { # used for toxicity ML training
            "signal": signal,
            "queue_ahead_at_join": queue_ahead_at_join,
            "resp": resp
        }

class Execution:
    def __init__(self, config, state, recorder):
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

        self.lock = threading.Lock() # Lock all mutations

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

    def _compute_order_size(self, signal): # Asymmetrical Quoting
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
        bid_multiplier = math.exp(-inv_signal) # scale size relative, 0.684 bid, 1.462 ask
        ask_multiplier = math.exp(inv_signal)

        # -------------------------
        # 3. convex risk aversion (kept but softened) risk penalty (quadratic inventory decay)
        # -------------------------
        risk_penalty = math.exp(-0.2 * (inv ** 2))

        toxicity_penalty = np.exp(-signal["toxicity"]["k2"] * signal["toxicity"]["tox"])

        # -------------------------
        # 4. combine
        # -------------------------
        base = base_size * vol_penalty * risk_penalty

        # optional: keep symmetric size baseline but asymmetric quoting power
        size = base * toxicity_penalty

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

        order_sizes = self._compute_order_size(signal)
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
                order=bid_order,
                side="BID",
                event_type="NEW",
                price=bid,
            )

            self.recorder.log_quote(
                ts=ts,
                order=ask_order,
                side="ASK",
                event_type="NEW",
                price=ask,
            )

            return

        tick = self.config.tick_size

        # -------------------------
        # DETERMINE IF WE NEED TO UPDATE EACH SIDE
        # -------------------------

        bid_change = abs(bid - self.last_bid) >= tick # bid might move 1 tick also, its possible
        ask_change = abs(ask - self.last_ask) >= tick

        # ---------------------------------------------------------------------------
        # CANCEL ONLY WHAT IS NECESSARY, REPLACE ONLY STALE SIDE(S), UPDATE STATE
        # ---------------------------------------------------------------------------
        if bid_change:
            self._cancel_side("BUY")

            self.state.reset_queue_ahead("bids", self.config.to_tick(self.last_bid)) # reset queue position and queue flow

            bid_order = self._place_limit(side="BUY", price=bid, size=order_sizes["bid_size"], ts=ts, signal=signal)

            self.last_bid = bid

            self.recorder.log_quote(
                ts=ts,
                order=bid_order,
                side="BID",
                event_type="REPLACE",
                price=bid,
            )

        if ask_change:
            self._cancel_side("SELL")

            self.state.reset_queue_ahead("asks", self.config.to_tick(self.last_ask)) # reset queue position and queue flow

            ask_order = self._place_limit(side="SELL", price=ask, size=order_sizes["ask_size"], ts=ts, signal=signal)

            self.last_ask = ask

            self.recorder.log_quote(
                ts=ts,
                order=ask_order,
                side="ASK",
                event_type="REPLACE",
                price=ask,
            )

        self.current_bid_size = order_sizes["bid_size"]
        self.current_ask_size = order_sizes["ask_size"]

    def _place_limit(self, side, price, size, ts, signal):
        order_id = self._new_order_id()

        tick_price = self.config.to_tick(price)

        # capture current book depth BEFORE your order joins
        book_side = (
            self.state.market_book.bids
            if side == "BUY"
            else self.state.market_book.asks
        )

        book_size = book_side.get(tick_price, 0.0)

        # record estimated queue position
        self.state.my_queue_position["bids" if side == "BUY" else "asks"][tick_price] = book_size

        order = Order(
            order_id=order_id,
            side=side,
            price=tick_price,
            qty=size,
            status="LIVE",
            ts=ts,
            owner="self",
            signal=signal,
            queue_ahead_at_join=book_size
        )

        with self.lock:
            self.open_orders[order_id] = order

        return order

    def _cancel_side(self, side):
        for oid, order in list(self.open_orders.items()):
            if order.side == side:
                order.status = "CANCELED"

                self.state.last_order_update = order

                with self.lock:
                    del self.open_orders[oid]

    def place_market(self):
        
        ts = self.config.now_ms()
        side = "SELL" if self.state.inventory > 0 else "BUY"
        qty = abs(self.state.inventory)

        order_id = self._new_order_id()

        order = Order(
            order_id=order_id,
            side=side,
            price=None, # market order has no price level
            qty=qty,
            status="LIVE",
            ts=ts,
            owner="self",
            signal=self.state.last_signal,
            queue_ahead_at_join=0.0
        )

        with self.lock:
            self.open_orders[order_id] = order

        # Immediately execute against book
        self._execute_market(order)

        return order

    # -------------------------
    # MARKET EVENTS (FILL LOGIC)
    # -------------------------

    def process_trade(self, trade):

        self.update_trade_flow_buckets(trade)
        
        self._update_trade_flow(trade)

        self._match_side(trade)

    def _update_trade_flow(self, trade):
        
        side = trade["side"]
        
        flow = 1 if side == "BUY" else -1

        alpha_flow = 0.2

        self.state.trade_imbalance = alpha_flow * flow + (1 - alpha_flow) * self.state.trade_imbalance # EWMA trade_imbalance

    def update_trade_flow_buckets(self, trade):

        side = trade["side"]
        price = self.config.to_tick(trade["price"])

        trade_event = {
            "side": side,
            "price": price,
            "size": trade["qty"],
            "timestamp": trade["timestamp"]
        }

        trade_bucket = self.state.trade_buckets[side][price]
        trade_bucket.append(trade_event)

        while trade_bucket and trade_event["timestamp"] - trade_bucket[0]["timestamp"] > self.state.window_ms:
            trade_bucket.popleft()

    def get_trade_rate(self, trade):
        
        ts = trade["timestamp"]
        price = self.config.to_tick(trade["price"])

        trade_bucket = self.state.trade_buckets[trade["side"]][price]
        volume = 0.0

        for trade_event in reversed(trade_bucket):
            if ts - trade_event["timestamp"] > self.state.window_ms:
                break
            
            volume += trade_event["size"]

        return volume / (self.state.window_ms / 1000.0) # ensure its volume / s

    # -------------------------
    # FILL ENGINE
    # -------------------------

    def _match_side(self, trade):
        """
        Why lock the WHOLE function?

        Because this entire block is logically atomic.
        """
        with self.lock:

            side = "BUY" if trade["side"] == "SELL" else "SELL"
            price = self.config.to_tick(trade["price"])
            qty = trade["qty"]

            orders = [
                o for o in self.open_orders.values()
                if o.side == side and o.price == price and o.status == "LIVE"
            ]

            if not orders:
                return
            
            self.state.last_fill_candidate = orders
            # -------------------------
            # Poisson arrival of liquidity, much more stable statistically maybe to hawkes in future
            # -------------------------

            queue_ahead = self.state.compute_queue_ahead( # passive world
                "bids" if side == "BUY" else "asks",
                price
            )

            trade_rate = self.get_trade_rate(trade) # aggressive world
 
            lambda_fill = trade_rate / (queue_ahead + 1e-9) # hazard rate (/ s), stochasitc estimation to model cancels / trades from l2 data

            p_fill = 1 - np.exp(-lambda_fill * 0.1) # fill prob of at least 1 event in the next 100ms (dt = 0.1)

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

                self.state.on_fill(self.config.from_tick(order.price), fill, order.side, "maker")

                self.state.last_order_update = order

                self.recorder.log_fill(qty=fill, order=order, is_maker=True)

                if order.remaining == 0:
                    order.status = "FILLED"
                    del self.open_orders[order.order_id]

                    self.state.reset_queue_ahead("bids" if side == "BUY" else "asks", price) # reset queue position and queue flow

    def _execute_market(self, order):

        with self.lock:

            book = self.state.market_book

            if order.side == "BUY":
                levels = list(book.asks.items())  # lowest ask first
            
            else:
                levels = list(book.bids.items())[::-1]  # highest bid first

            for price, size in levels:

                if order.remaining <= 0:
                    break

                if size <= 0:
                    continue

                fill = min(order.remaining, size)

                order.remaining -= fill

                order.price = price # for dashboard UI for market orders
                self.state.last_fill_candidate = [order]
                self.state.last_order_update = order

                print(f"{order.side} | {self.config.from_tick(price):>10.4f} | {fill:>8.6f} | {order.status}")

                # update book liquidity
                if order.side == "BUY":
                    new_size = book.asks[price] - fill
                    
                    if new_size <= 0:
                        del book.asks[price]

                    else:
                        book.asks[price] = new_size
                else:
                    new_size = book.bids[price] - fill
                    
                    if new_size <= 0:
                        del book.bids[price]

                    else:
                        book.bids[price] = new_size

                self.state.on_fill(
                    self.config.from_tick(price),
                    fill,
                    order.side,
                    "taker"
                )

            if order.remaining == 0:
                order.status = "FILLED"

# Orchestration Layer
class Engine:
    def __init__(self, config, state, strategy, execution, recorder, dashboard, params, signal_queue):
        self.config = config
        self.params = params
        self.state = state
        self.strategy = strategy
        self.execution = execution
        self.recorder = recorder

        # Event driven
        self.signal_queue = signal_queue

        # System Configuration
        self.struct_model = self.params["models"]["struct_model"]
        self.edge_model = self.params["models"]["edge_model"]
        self.mode = self.params["mode"]
        self.exchange = self.params["exchange"]
        self.instrument = self.params["instrument"]
        
        # React dashboard
        self.dashboard = dashboard
        
        # Rich dashboard
        self.live = None
        self.last_dashboard_update = 0

    def on_market_data(self): # ASYNC PUSH

        # HARD SAFETY: do not log or trade before sync
        if not self.state.initialized:
            return
    
        signal = self.strategy.on_market_update(self.state)

        if signal is None:
            return
    
        try:
            self.signal_queue.put_nowait(signal)
        
        except queue.Full:
            pass  # drop stale signals (important in HFT)

        self.recorder.log_snapshot(
            ts=self.state.last_depth_ts,
            signal=signal,
            symbol=self.instrument.upper()
        )

    def on_trade_event(self, trade):

        # Dataset recording
        self.recorder.log_trade(trade=trade, symbol=self.instrument.upper())

        self.execution.process_trade(trade)
    
    def fmt(self, o):
        return f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]" if o else "—"
    
    def build_snapshot(self):

        """
        mm-core | live | btcusdt |
        regime=TRENDING |
        vol=high | spread=wide | imbalance=+0.62 |
        alpha=1.4x | k=0.8x | pnl=+0.0935%

        Now you can answer:

        “Is my PnL coming from correct regime adaptation or luck?”
        
        """
        state = self.state
        book = state.market_book
        execution = self.execution

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
        # POLICY
        # -------------------------
        regime = state.last_signal["regime"] if state.last_signal else "no model"

        # -------------------------
        # SIGNALS
        # -------------------------
        fair = state.last_signal["fair"] if state.last_signal else 0.0
        microprice = state.last_signal["microprice"] if state.last_signal else 0.0
        skew = state.last_signal["skew"] if state.last_signal else 0.0
        reservation = state.last_signal["reservation"] if state.last_signal else 0.0
        alpha_order_imb = state.last_signal["alpha_order_imb"] if state.last_signal else 0.0
        alpha_trade_imb = state.last_signal["alpha_trade_imb"] if state.last_signal else 0.0
        alpha_struct = state.last_signal["alpha_struct"] if state.last_signal else 0.0
        k0 = state.last_signal["k0"] if state.last_signal else 0.0
        spread_multiplier = state.last_signal["spread_multiplier"] if state.last_signal else 0.0
        inventory_target = state.last_signal["inventory_target"] if state.last_signal else 0.0
        signal_quality = state.last_signal["signal_quality"] if state.last_signal else 0.0
        toxicity = state.last_signal["toxicity"] if state.last_signal else {}

        # -------------------------
        # EXECUTION
        # -------------------------
        bid_queue = state.compute_queue_ahead("bids", self.config.to_tick(bid))
        ask_queue = state.compute_queue_ahead("asks", self.config.to_tick(ask))
        bid_pressure = bid_queue / (bid_size + 1e-9)
        ask_pressure = ask_queue / (ask_size + 1e-9)

        with execution.lock: # Another lock
            #orders = sorted(execution.open_orders, key=lambda o: 0 if o.side == "BUY" else 1)
            buy_order = execution.open_orders.get("BUY", None)
            sell_order = execution.open_orders.get("SELL", None)

        # orders = [f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.qty:>8.6f} [{o.status}]" for o in orders]
        # orders_str = (orders + ["—", "—"])[:2]

        buy_order_str = self.fmt(buy_order)
        sell_order_str = self.fmt(sell_order)
        

        # last_fill_candidate_str = "\n".join([f"{o.side:<5} | {self.config.from_tick(o.price):>10.4f} | {o.remaining:>8.6f} [{o.status}]"
        #     for o in state.last_fill_candidate
        # ]) or "—"

        last_fill_candidate_str = self.fmt(state.last_fill_candidate)

        #last_order_update = state.last_order_update if state.last_order_update else None
        # last_order_update = state.last_order_update
        last_order_update_str = self.fmt(state.last_order_update)
        # last_order_update_str = f"{last_order_update.side:<5} | {self.config.from_tick(last_order_update.price):>10.4f} | {last_order_update.remaining:>8.6f} [{last_order_update.status}]" if last_order_update else "—"

        state.update_performance() # update sharpe
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
                "struct_model": self.struct_model,
                "mode": self.mode,
                "exchange": self.exchange,
                "instrument": self.instrument,
                "regime": regime,
                "pnl_pct": pnl_pct
            },

            "market": {
                "mid": mid,
                "microprice": microprice,
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

            "regime": {
                "regime": regime[:1].upper() + regime[1:],
                "confidence": state.last_signal["regime_prob"] if state.last_signal else 0.0
            },

            "signals": {
                "fair_value": fair,
                "skew": skew,
                "reservation": reservation,
                "alpha_order_imb": alpha_order_imb,
                "alpha_trade_imb": alpha_trade_imb,
                "alpha_struct": alpha_struct,
                "k0": k0,
                "spread_multiplier": spread_multiplier,
                "inventory_target": inventory_target,
                "signal_quality": signal_quality,
                "tox" : toxicity.get("tox", 0.0),
                "k1" : toxicity.get("k1", 0.0),
                "k2" : toxicity.get("k2", 0.0),
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
                # "open_order_one": orders_str[0],
                # "open_order_two": orders_str[1],
                "open_order_one": buy_order_str,
                "open_order_two": sell_order_str,
                "last_fill_candidate": last_fill_candidate_str,
                "last_order_update": last_order_update_str
            },

            "risk": {
                "inventory": state.inventory,
                "realized_pnl": state.realized_pnl,
                "unrealized_pnl": state.get_unrealized_pnl(mid),
                "fees_paid": state.fees_paid,
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
        title=f"{snapshot["title"]["struct_model"]} | {snapshot["title"]["mode"]} | {snapshot["title"]["exchange"]} | {snapshot["title"]["instrument"]} | {snapshot["title"]["regime"]:<7} | pnl={pnl_pct_sign}{pnl_pct:.4f}%"

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
        # REGIME
        # -------------------------
        table.add_row("[bold yellow]REGIME[/bold yellow]", "")
        table.add_row("Regime", f"{snapshot["regime"]["regime"]}")
        table.add_row("Confidence", f"{snapshot["regime"]["confidence"]:<15.2f}")
        table.add_row("", "")

        # -------------------------
        # SIGNALS
        # -------------------------
        table.add_row("[bold yellow]SIGNALS[/bold yellow]", "")
        table.add_row("Fair Value", f"{snapshot["signals"]["fair_value"]:<15.4f}")
        table.add_row("Inventory Skew", f"{snapshot["signals"]["skew"]:<15.4f}")
        table.add_row("Reservation", f"{snapshot["signals"]["reservation"]:<15.4f}")
        table.add_row("Alpha Struct", f"{snapshot["signals"]["alpha_struct"]:<15.2f}")
        table.add_row("ML Signal Quality", f"{snapshot["signals"]["signal_quality"]:<15.2f}")
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
        table.add_row("Last Fill Candidate", snapshot["execution"]["last_fill_candidate"])
        table.add_row("Last Order Update", snapshot["execution"]["last_order_update"])
        table.add_row("", "")

        # -------------------------
        # RISK
        # -------------------------
        table.add_row("[bold yellow]RISK[/bold yellow]", "")
        table.add_row("Inventory", self.color_risk(snapshot["risk"]["inventory"], limit=10))
        table.add_row("Realized PnL", self.color_pnl(snapshot["risk"]["realized_pnl"]))
        table.add_row("Unrealized PnL", self.color_pnl(snapshot["risk"]["unrealized_pnl"]))
        table.add_row("Fees Paid", self.color_pnl(-snapshot["risk"]["fees_paid"]))
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

        inv_bar = f"[red]{left_half}[/red][white]{chars[half]}[/white][green]{right_half}[/green]"

        return inv_bar


# Global State Layer
class MarketFeatureState:
    def __init__(self, maxlen=10):
        self.mid_returns = deque(maxlen=maxlen)
        self.spread = deque(maxlen=maxlen)
        self.order_imbalance = deque(maxlen=maxlen)
        self.trade_imbalance = deque(maxlen=maxlen)
        self.quote_churn = deque(maxlen=maxlen)
        self.inventory = deque(maxlen=maxlen)
        self.microprice_error = deque(maxlen=maxlen)

        # ML k signal quality
        self.ml_predictions = deque(maxlen=200)
        self.ml_signal_log = deque(maxlen=5000)
        self.ml_horizon_ms = 0.0

        # for delta tracking
        self.prev_best_bid = None
        self.prev_best_ask = None

class State:
    def __init__(self, config, params):
        
        # Market Data Layer
        self.config = config
        self.params = params
        self.market_book = OrderBook(config=self.config)
        self.last_mid = None

        # Market Regime Layer
        self.market_feature_state = MarketFeatureState(maxlen=10)

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

        self.equity_history = deque(maxlen=10000)
        self.return_history = deque(maxlen=10000)
        self.last_equity = None

        # Exchange fees
        self.maker_fee_rate = self.params["fees"]["maker_fee_rate"] # 0.0002 0.02%
        self.taker_fee_rate = self.params["fees"]["taker_fee_rate"] # 0.0002 0.02%
        self.fees_paid = 0.0

        # Microstructure Simulation Layer
        self.queue_flow = {
            "bids": {}, # to change to defaultdict?
            "asks": {}
        }

        self.my_queue_position = {
            "bids": {},
            "asks": {}
        }

        self.trade_buckets = { # aggressor / trade side
            "BUY": defaultdict(lambda: deque()),
            "SELL": defaultdict(lambda: deque())
        }

        self.window_ms = 1000 # trade flow window

        # Event Observation Layer
        self.last_trade_ts = None
        self.last_trade = {}
        self.last_depth_ts = None
        self.last_fill_candidate = []
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

    def on_fill(self, price, qty, side, liquidity="maker"):
        """
        side: "BUY" or "SELL"
        price: execution price
        qty: executed quantity
        """

        old_inv = self.inventory
        old_avg = self.avg_entry_price

        fill_value = price * qty

        fee_rate = self.maker_fee_rate if liquidity == "maker" else self.taker_fee_rate # fee rate

        fee = fill_value * fee_rate

        self.fees_paid += fee

        # -----------------------------
        # BUY FLOW
        # -----------------------------
        if side == "BUY":

            # 1. If we are SHORT → close short first
            if old_inv < 0:
                close_qty = min(qty, abs(old_inv))

                # realized PnL from short
                self.realized_pnl += close_qty * (old_avg - price)

                old_inv += close_qty
                qty -= close_qty

            self.inventory = old_inv

            # 2. Remaining BUY opens/increases LONG
            if qty > 0:
                new_inv = old_inv + qty

                if old_inv > 0:
                    # averaging long
                    self.avg_entry_price = (
                        old_avg * old_inv + price * qty
                    ) / new_inv
                else:
                    # fresh long
                    self.avg_entry_price = price

                self.inventory = new_inv

            # cash always decreases on buy
            self.cash -= (fill_value + fee) # maker/taker fees

        # -----------------------------
        # SELL FLOW
        # -----------------------------
        else:

            # 1. If we are LONG → close long first
            if old_inv > 0:
                close_qty = min(qty, old_inv)

                # realized PnL from long
                self.realized_pnl += close_qty * (price - old_avg)

                old_inv -= close_qty
                qty -= close_qty

            self.inventory = old_inv

            # 2. Remaining SELL opens/increases SHORT
            if qty > 0:
                new_inv = old_inv - qty

                if old_inv < 0:
                    # averaging short
                    self.avg_entry_price = (
                        old_avg * abs(old_inv) + price * qty
                    ) / abs(new_inv)
                else:
                    # fresh short
                    self.avg_entry_price = price

                self.inventory = new_inv

            # cash always increases on sell
            self.cash += (fill_value - fee) # maker/taker fees

        # -----------------------------
        # RESET avg price if flat
        # -----------------------------
        if self.inventory == 0:
            self.avg_entry_price = 0.0

    def get_unrealized_pnl(self, mid):
        return self.inventory * (mid - self.avg_entry_price)

    def get_pnl(self):
        mid = self.market_book.mid()

        unrealized = self.inventory * (mid - self.avg_entry_price)

        return self.realized_pnl + unrealized
    
    def compute_queue_ahead(self, side, price):

        my_pos = self.my_queue_position[side].get(price, 0.0) # Initial size ahead of you when you joined
        flow = self.queue_flow.get(side, {}).get(price, 0.0) # Estimated amount of queue depletion since you joined

        ahead = max(0.0, my_pos - flow) # Estimated queue position = Initial size ahead - estimated queue depletion
        
        return ahead
    
    def reset_queue_ahead(self, side, price): #reset queue position and queue flow for new fill probability estimate

        self.my_queue_position[side].pop(price, None)
        self.queue_flow[side].pop(price, None)
    
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

    def update_market_feature_state(self):

        mfs = self.market_feature_state

        book = self.market_book

        bid_tick, bid_size = book.best_bid()
        ask_tick, ask_size = book.best_ask()

        best_bid = self.config.from_tick(bid_tick)
        best_ask = self.config.from_tick(ask_tick)

        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9)

        last_mid = self.last_mid if self.last_mid else mid
        mid_return = (mid - last_mid) / last_mid

        # quote churn for regime_signal
        if mfs.prev_best_bid is None:
            mfs.prev_best_bid = best_bid
            mfs.prev_best_ask = best_ask
            quote_churn = 0.0

        else:
            bid_delta = abs(best_bid - mfs.prev_best_bid)
            ask_delta = abs(best_ask - mfs.prev_best_ask)
            quote_churn = bid_delta + ask_delta

        mfs.prev_best_bid = best_bid
        mfs.prev_best_ask = best_ask

        mfs.mid_returns.append(mid_return)
        mfs.spread.append(spread)
        mfs.order_imbalance.append(self.order_imbalance)
        mfs.trade_imbalance.append(self.trade_imbalance)
        mfs.quote_churn.append(quote_churn)
        mfs.inventory.append(self.inventory)
        mfs.microprice_error.append(mid - microprice)

    def get_regime(self):

        mfs = self.market_feature_state

        regime = {
            "volatility": np.std(mfs.mid_returns) if mfs.mid_returns else 0.0,
            "spread": np.mean(mfs.spread) if mfs.spread else 0.0,
            "order_imbalance": np.mean(mfs.order_imbalance) if mfs.order_imbalance else 0.0,
            "trade_imbalance": np.mean(mfs.trade_imbalance) if mfs.trade_imbalance else 0.0,
            "quote_churn": np.mean(mfs.quote_churn) if mfs.quote_churn else 0.0,
            "inventory": np.mean(mfs.inventory) if mfs.inventory else 0.0,
            "inventory_vol": np.std(mfs.inventory) if mfs.inventory else 0.0,
            "microprice_error": np.mean(mfs.microprice_error) if mfs.microprice_error else 0.0
        }

        return regime

    def update_ml_realization(self):

        mfs = self.market_feature_state

        now = self.config.now_ms()

        while mfs.ml_predictions and now - mfs.ml_predictions[0]["ts"] >= mfs.ml_horizon_ms:
            
            entry = mfs.ml_predictions.popleft()

            realized = np.log(self.last_mid / entry["reservation"])

            mfs.ml_signal_log.append({
                "pred": entry["pred"],
                "realized": realized
            })

    def update_performance(self):

        bid_tick, bid_size = self.market_book.best_bid()
        ask_tick, ask_size = self.market_book.best_ask()

        if bid_size <= 0 or ask_size <= 0:
            return  # skip update safely

        bid = self.config.from_tick(bid_tick)
        ask = self.config.from_tick(ask_tick)

        mid = (bid + ask) / 2

        equity = self.cash + self.inventory * mid

        if self.last_equity is not None and self.last_equity > 0 and equity > 0:
            r = np.log(equity / self.last_equity)

            if np.isfinite(r):
                self.return_history.append(r)

        self.last_equity = equity
        self.equity_history.append(equity)

    def compute_sharpe(self):

        if len(self.return_history) < 30:
            return 0.0

        returns = np.array(self.return_history)
        returns = returns[np.isfinite(returns)]

        if len(returns) < 30:
            print('SHARPE - len(returns) < 30:', np.nan)

            return np.nan

        std = np.std(returns)
        if std == 0 or np.isnan(std):
            print('SHARPE - std == 0 / isnan(std):', np.nan)

            return np.nan

        sharpe_ratio = np.mean(returns) / np.std(returns) + 1e-9
        
        print('SHARPE:', sharpe_ratio)
        
        return sharpe_ratio

# Simulation Feed Layer
class ReplayFeed:
    """
    ReplayFeed = drop-in replacement for BinanceFeed

    It emits the SAME callbacks:
        - on_market_data(state)
        - on_trade_event(trade)
    """

    def __init__(self, state, on_market_data, on_trade_event, logger, params):

        self.events = os.path.join(params["folder_path"], params["files"]["replay_events"]["events"])
        self.orderbook_snapshot = os.path.join(params["folder_path"], params["files"]["replay_events"]["orderbook_snapshot"])
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
        self.speed_multiplier = 5.0 # to speed up market events for backtesting
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
        state.update_market_feature_state() # regime update

        state.update_vol()

        state.compute_order_imbalance()

        state.update_ml_realization() # compute ml signal quality

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

        # store latest exchange timestamp globally
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
        self.params = params

        # NEW ASYNC
        self.signal_queue = queue.Queue(maxsize=1000) # thread-safe FIFO queue

        # Monitoring Layer
        self.react_dashboard = DashboardServer(params=self.params)
        
        # Market Configuration
        self.config = MarketConfig(params=self.params)

        # Strategy Layer
        self.strategy = MarketMakingStrategy(config=self.config, params=self.params)

        # Market State
        self.state = State(config=self.config, params=self.params)
        self.state.market_feature_state.ml_horizon_ms = self.strategy.edge_model.horizon_ms if self.strategy.edge_model is not None else 0.0 # for ML signal horizon

        # Data Recording Layer
        self.recorder = DatasetRecorder(config=self.config, state=self.state, params=self.params)

        # Execution Layer
        self.broker = BinanceBroker(config=self.config, params=self.params)

        # self.execution = Execution(
        #     config=self.config,
        #     state=self.state,
        #     recorder=self.recorder
        # )

        self.execution = LiveExecution(
            config=self.config,
            state=self.state,
            broker=self.broker,
            recorder=self.recorder,
            params=self.params
        )

        self.user_stream = BinanceUserStream(
            broker=self.broker,
            state=self.state,
            execution=self.execution,
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
            params=self.params,
            signal_queue=self.signal_queue
        )

        # Market Data Layer
        FEEDS = {
            ("live", "binance"): BinanceFeed,
            ("replay", "binance"): ReplayFeed,
        }

        FeedClass = FEEDS[(self.params["mode"], self.params["exchange"])]

        # 1. create FSM first
        self.fsm = BinanceFSM(
            state=self.state,
            on_market_data=self.engine.on_market_data,
            logger = {
                "log_event": self.recorder.log_event,
                "log_orderbook_snapshot": self.recorder.log_orderbook_snapshot
            },
        )

        # 2. create feed with FSM injected
        self.feed = BinanceFeed(
            state=self.state,
            fsm=self.fsm,
            instrument="btcusdt",
            logger = {
                "log_event": self.recorder.log_event,
                "log_orderbook_snapshot": self.recorder.log_orderbook_snapshot
            },
        )

        # 3. snapshot FIRST (blocking)
        last_update_id, snapshot = self.state.market_book.initialize_from_binance("BTCUSDT", 1000)

        # 4. initialize FSM
        self.fsm.initialize_snapshot(snapshot)
        self.fsm.last_update_id = last_update_id

        # 5. start websocket feed
        self.feed.start()

        # Runtime Control
        self.engine_running = False
        self.dashboard_running = False
        self.threads = []

    def start(self):
        self.engine_running = True
        self.dashboard_running = True
        
        self.react_dashboard.start()

        self.engine.start_rich_dashboard()
        self.feed.start()
        self.user_stream.start()

        self.start_execution_loop()

        self.start_react_dashboard_loop()
        self.start_rich_dashboard_loop()

    def start_execution_loop(self): # EVENT DRIVEN

        def exec_loop():
            while self.engine_running:
                try:
                    signal = self.signal_queue.get(timeout=0.1)

                    self.state.last_signal = signal # just for logging purposes, should put here, since this is the last execution signal, which shows truth in execution engine

                    self.execution.place_quotes(signal)

                    # self.execution.process_place_quotes(signal) # for latency simulation

                except queue.Empty:
                    continue

        t = threading.Thread(target=exec_loop, daemon=True)
        t.start()
        self.threads.append(t)

    def start_rich_dashboard_loop(self):

        def rich_loop():
            while self.dashboard_running:
                self.engine.update_dashboard()
                time.sleep(0.02) # 50Hz dashboard refresh

        t = threading.Thread(target=rich_loop,daemon=True)
        t.start()
        self.threads.append(t)

    def start_react_dashboard_loop(self):

        def react_loop():
            while self.dashboard_running:
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
            # while self.running:
            while self.engine_running:
                time.sleep(1)

        except KeyboardInterrupt: # lets you stop with CTRL+C (standard in HFT backtests too)
            print("INTERRUPT RECEIVED - SHUTTING DOWN")

        finally:
            self.shutdown()

    def shutdown(self):

        # 1. stop feed
        self.engine_running = False
        self.feed.stop()

        # 2. cancel all orders
        print("CLOSING OPEN POSITIONS")
        print("")
        self.execution._cancel_side("BUY")
        self.execution._cancel_side("SELL")

        # 3. wait for fill completion
        while self.execution.open_orders:
            time.sleep(0.05)

        # 4. flatten inventory
        self.execution.place_market()
        print("")

        # IMPORTANT: allow fills to settle
        while abs(self.state.inventory) > 1e-9: 
            time.sleep(0.05)

        self.dashboard_running = False

        for t in self.threads:
            t.join(timeout=1)

        # 5. final stats
        self.recorder.export_run()

def load_manifest(path):
    with open(path, "r") as f:
        return json.load(f)
    
if __name__ == "__main__":
    path = input("Enter manifest path: ").strip('"').strip("'")
    params = load_manifest(path)
    
    system = TradingSystem(params=params)
    
    system.start()
    system.run_forever()