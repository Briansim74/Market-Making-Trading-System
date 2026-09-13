#pragma once //include this file once per compile
#include <set>
#include <map>
#include <deque>
#include <queue>
#include <mutex>
#include <ctime>
#include <cmath>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <ranges>
#include <random>
#include <format>
#include <cctype>
#include <memory>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <utility>
#include <cstdint>
#include <optional>
#include <iostream>
#include <algorithm>
#include <filesystem>
#include <functional>
#include <filesystem>
#include <unordered_map>
#include <condition_variable>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>

#include <cpr/cpr.h>
#include "simdjson.h"
#include <nlohmann/json.hpp>
#include <xgboost/c_api.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <ftxui/dom/table.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>

using std::cout;
using json = nlohmann::json;
using ordered_json = nlohmann::ordered_json;
using std_string = std::string;

using namespace std;
using namespace arrow;
using namespace ftxui;
using namespace std::chrono;

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
namespace fs = filesystem;
using tcp = asio::ip::tcp;
using ssl_stream = asio::ssl::stream<tcp::socket>;
using ws_stream  = websocket::stream<ssl_stream>;

struct Trade {
    int64_t ts;
    int64_t local_ts;
    std_string side;
    double price;
    double qty;
    int64_t latency;
};

struct Depth {
    int64_t ts;
    int64_t local_ts;
    int64_t U;
    int64_t u;
    int64_t pu;
    int64_t latency;
    vector<pair<int64_t, double>> bid_delta;
    vector<pair<int64_t, double>> ask_delta;
};

struct Stream {
    std_string client_oid;
    std_string side;
    std_string status;
    std_string exec_type;
    std_string order_type;
    double price;
    double qty;
    double fill_price;
    double fill_qty;
    double fees_paid;
    int64_t exchange_ts;
    int64_t local_ts;
};

struct Hawkes {
    int64_t last_ts = 0;
    double excitation = 0.0;
    double lambda = 3.0; // decay rate
    double multiplier = 0.0; // 2.0; // inactive hawkes
    double beta_raw = 0.6;
    double beta = 0.0;

    void update(const double& depletion, const int64_t& ts){
        if(last_ts != 0){
            double dt = (ts - last_ts) / 1000.0; // seconds

            // Ht ​= H_t−1 ​* e^(−λΔt) + qt​
            excitation *= exp(-lambda * dt);
        }

        excitation += depletion;
        last_ts = ts;

        // compensate for hidden churn, since there might be many cancellations/additions
        if(depletion < 0.05) beta_raw = 15.0;
        else if(depletion < 0.5) beta_raw = 7.0;
        else beta_raw = 0.6; // large depletion is probably real

        beta = beta_raw + multiplier * excitation;
    }
};

struct EventNotifier {
    mutex signal_mtx;
    condition_variable signal_cv;
    bool signal_pending = false;
};

enum class ExecutionEventType{
    TRADE_UPDATE,
    DEPTH_UPDATE_SPOT,
    DEPTH_UPDATE_FUTURES,
    STREAM_UPDATE,
    CANCEL_UPDATE,
    MARK_PRICE_UPDATE
};

struct ExecutionEvent{
    ExecutionEventType type;
    Trade trade;
    Depth depth;
    Stream stream;
};

struct ExecutionEventQueue {
    mutex mtx;
    condition_variable cv;
    queue<ExecutionEvent> queue;

    void push(ExecutionEvent ev){
        {
            lock_guard<mutex> lock(mtx);
            queue.push(move(ev));
        }
        cv.notify_one();
    }

    bool pop(ExecutionEvent& ev, atomic<bool>& running){

        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [&]{return !queue.empty() || !running;});

        if(queue.empty() && !running) return false;

        ev = move(queue.front());
        queue.pop();

        return true;
    }

    bool pop_timeout(ExecutionEvent& ev, atomic<bool>& running, milliseconds timeout){
        
        unique_lock<mutex> lock(mtx);
        cv.wait_for(lock, timeout, [&]{return !queue.empty() || !running;});

        if(queue.empty() && !running) return false;

        if(queue.empty()) return false; // timeout, no event

        ev = move(queue.front());
        queue.pop();

        return true;
    }
};

struct Regime {
    double volatility;
    double spread;
    double order_imbalance;
    double trade_imbalance;
    double quote_churn;
    double inventory;
    double inventory_vol;
    double microprice_error;
};

struct Features {
    double mid;
    double fair;
    double skew;
    double microprice;
    double microprice_dev;
    double spread;

    double order_imbalance;
    double trade_imbalance;
    double inventory;
    double volatility;

    double queue_ahead_bid;
    double queue_ahead_ask;
};

struct Policy {
    std_string regime = "no_regime_model";
    int regime_id = -1;
    double regime_prob = 0.0;

    double alpha_trade_imb = 0.05;
    double alpha_struct = 0.3;
    double alpha_residual = 0.5;

    double spread_multiplier = 1.0;
    double inventory_target = 0.0;
};

struct Toxicity {
    double tox = 0.0;
    double k_spread = 0.2;
    double k_order_size = 0.357;
    double pred = 0.0;
};

struct Signal {
    int64_t ts;
    int64_t trade_latency;
    int64_t depth_latency;
    int64_t exchange_latency;
    double mid;
    double microprice;
    double microprice_dev;
    double microprice_error;
    double spread;
    double best_bid;
    double best_ask;

    double order_imbalance;
    double trade_imbalance;
    double volatility;
    double queue_ahead_bid;
    double queue_ahead_ask;

    double inventory;
    double realized_pnl;
    double unrealized_pnl;
    double total_pnl;
    double fees_paid;
    double equity;

    double fair;
    double skew;
    double struct_delta;
    double micro_signal_delta;
    double residual_delta;
    double reservation;

    std_string regime;
    int regime_id;
    double regime_prob;
    double alpha_trade_imb;
    double alpha_struct;
    double alpha_residual;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    Toxicity toxicity;

    double bid_delta;
    double ask_delta;
    double quote_churn;

    double my_bid;
    double my_ask;
};

struct Order {
    std_string client_oid; // changed to client_oid from order_id
    std_string side;
    int64_t price_tick;
    double qty;
    double remaining;
    std_string status;
    bool pending_cancel = false;
    int64_t ts;
    int64_t live_ts = 0;
    int64_t exchange_latency = 0;
    std_string owner;
    Signal signal;
    double queue_ahead_at_join;
    json resp;
};

struct LatencyEvent {
    int64_t execute_ts;
    std_string type;
    Signal signal;
};

struct Compare {
    bool operator()(const LatencyEvent& a, const LatencyEvent& b){
        return a.execute_ts > b.execute_ts; // "Does a have lower priority than b?" -> min heap
    }
};

struct ResidualPred {
    int64_t ts;
    int64_t horizon_ms;
    double pred;
    double reservation;
    double realized;
};

struct ToxicityPred {
    int64_t ts;
    int64_t horizon_ms;
    double pred;
    double fill_price;
    int fill_sign;
    double realized;
};

struct MarketFeatureState {
    deque<double> mid_returns;
    deque<double> spread;
    deque<double> order_imbalance;
    deque<double> trade_imbalance;
    deque<double> quote_churn;
    deque<double> inventory;
    deque<double> microprice_error;

    deque<ResidualPred> residual_predictions;
    deque<ResidualPred> residual_signal_log;

    deque<ToxicityPred> toxicity_predictions;
    deque<ToxicityPred> toxicity_signal_log;

    double prev_best_bid = 0.0;
    double prev_best_ask = 0.0;
};

struct EventRow {
    std_string type;
    int64_t ts;
    int64_t local_ts;
    int64_t latency;
    std_string msg;   // raw JSON string (same as Python)
};

struct Snapshot {
    struct {
        std_string header;
        std_string regime;
        double pnl_pct;
    } title;

    struct {
        double mid;
        double microprice;
        double spread;
        double best_bid;
        double best_ask;
        double bid_size;
        double ask_size;
        double ewma_vol;
        double order_imbalance;
        double trade_imbalance;
        std_string trade;
    } market;

    struct {
        std_string regime;
        double confidence;
    } regime;

    struct {
        double fair;
        double skew;
        double reservation;
        double alpha_trade_imb;
        double alpha_struct;
        double alpha_residual;
        double spread_multiplier;
        double inventory_target;
        double residual_signal_quality;
        double tox;
        double k_spread;
        double k_order_size;
    } signals;

    struct {
        double my_bid;
        double my_ask;
        double current_bid_size;
        double current_ask_size;
    } quotes;

    struct {
        double bid_queue;
        double ask_queue;
        double bid_pressure;
        double ask_pressure;
        std_string buy_order;
        std_string sell_order;
        std_string last_fill_candidate;
        std_string last_order_update;
    } execution;

    struct {
        double inventory;
        double realized_pnl;
        double unrealized_pnl;
        double fees_paid;
        double total_pnl;
    } risk;

    struct {
        std_string time;
        std_string last_trade_ts;
        std_string last_depth_ts;
        int64_t trade_latency;
        int64_t depth_latency;
        int64_t exchange_latency;
    } system;
};

struct SnapshotRow {
    int64_t ts;
    int64_t trade_latency;
    int64_t depth_latency;
    int64_t exchange_latency;
    std_string symbol;

    double mid;
    int64_t mid_tick;
    double spread;
    double microprice;
    double microprice_dev;
    double microprice_error;

    double best_bid;
    double best_ask;
    int64_t best_bid_tick;
    int64_t best_ask_tick;

    double order_imbalance;
    double trade_imbalance;
    double volatility;
    double queue_ahead_bid;
    double queue_ahead_ask;

    double inventory;
    double realized_pnl;
    double unrealized_pnl;
    double total_pnl;
    double fees_paid;
    double equity;

    double fair;
    double skew;
    double struct_delta;
    double micro_signal_delta;
    double residual_delta;
    double reservation;

    std_string regime;
    int regime_id;
    double regime_prob;
    double alpha_trade_imb;
    double alpha_struct;
    double alpha_residual;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k_spread;
    double k_order_size;

    double my_bid;
    double my_ask;
    int64_t my_bid_tick;
    int64_t my_ask_tick;

    double bid_distance_touch;
    double ask_distance_touch;
    double bid_distance_spread;
    double ask_distance_spread;

    double bid_delta;
    double ask_delta;
    double quote_churn;
};

struct TradeRow {
    int64_t ts;
    std_string symbol;

    double price;
    int64_t price_tick;
    double qty;
    std_string side;
    bool is_buyer_maker;

    double mid;
    double microprice;
    double microprice_dev;
    double microprice_error;
    double spread;
    double best_bid;
    double best_ask;
    int64_t best_bid_tick;
    int64_t best_ask_tick;

    double bid_size;
    double ask_size;

    double trade_to_mid;
    double trade_to_microprice;
    double price_to_best_bid;
    double price_to_best_ask;

    std_string trade_side;
    int trade_sign;

    double notional;
    double log_notional;
    double intensity;
};

struct QuoteRow {
    int64_t ts;
    int64_t exchange_latency;
    std_string symbol;
    std_string client_oid;
    std_string side;
    std_string event_type;

    double price;
    int64_t price_tick;
    double qty;

    double mid;
    double microprice;
    double microprice_dev;
    double microprice_error;
    double spread;
    double best_bid;
    double best_ask;
    int64_t best_bid_tick;
    int64_t best_ask_tick;
    
    double order_imbalance;
    double trade_imbalance;
    double volatility;
    double queue_ahead_bid;
    double queue_ahead_ask;

    double distance_to_mid;
    double distance_to_touch;
    double inventory;

    double fair;
    double skew;
    double struct_delta;
    double micro_signal_delta;
    double residual_delta;
    double reservation;

    std_string regime;
    int regime_id;
    double regime_prob;
    double alpha_trade_imb;
    double alpha_struct;
    double alpha_residual;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k_spread;
    double k_order_size;
};

struct FillRow {
    int64_t ts;
    int64_t exchange_latency;
    int64_t time_to_fill;
    std_string symbol;
    std_string side;
    double price;
    int64_t price_tick;
    double qty;
    double cum_qty;

    bool is_maker;
    int fill_sign;
    std_string fill_type;
    std_string fill_status;
    double queue_ahead_at_join;

    double mid_at_fill;
    double microprice_at_fill;
    double microprice_dev_at_fill;
    double microprice_error_at_fill;
    double spread_at_fill;
    double best_bid_at_fill;
    double best_ask_at_fill;
    double volatility_at_fill;
    double volatility_at_fill_bps;

    double mid;
    double microprice;
    double microprice_dev;
    double microprice_error;
    double spread;
    double best_bid;
    double best_ask;
    int64_t best_bid_tick;
    int64_t best_ask_tick;

    double order_imbalance;
    double trade_imbalance;
    double volatility;
    double volatility_bps;
    double queue_ahead_bid;
    double queue_ahead_ask;
    double inventory;

    double fair;
    double skew;
    double struct_delta;
    double micro_signal_delta;
    double residual_delta;
    double reservation;

    std_string regime;
    int regime_id;
    double regime_prob;
    double alpha_trade_imb;
    double alpha_struct;
    double alpha_residual;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k_spread;
    double k_order_size;

    double my_bid;
    double my_ask;
    int64_t my_bid_tick;
    int64_t my_ask_tick;

    double bid_distance_touch;
    double ask_distance_touch;
    double bid_distance_spread;
    double ask_distance_spread;
};

struct PerformanceMetrics {
    double sharpe = NAN;
    double annualized_sharpe = NAN;
    double sortino = NAN;
    double annualized_sortino = NAN;
};