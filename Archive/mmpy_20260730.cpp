#pragma warning(disable: 4834)
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

    void wake(){
        cv.notify_all();
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

    double alpha_order_imb = 0.2;
    double alpha_trade_imb = 0.05;
    double alpha_struct = 0.3;

    double spread_multiplier = 1.0;
    double k0 = 0.5;
    double inventory_target = 0.0;

    double residual_mid = 0.0;
    double micro_residual = 0.0;
};

struct Toxicity {
    double tox = 0.0;
    double k1 = 0.2;
    double k2 = 0.357;
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
    double alpha_order_imb;
    double alpha_trade_imb;
    double alpha_struct;
    double k0;
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
        double alpha_order_imb;
        double alpha_trade_imb;
        double alpha_struct;
        double k0;
        double spread_multiplier;
        double inventory_target;
        double residual_signal_quality;
        double tox;
        double k1;
        double k2;
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
    double alpha_order_imb;
    double alpha_trade_imb;
    double alpha_struct;
    double k0;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k1;
    double k2;

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
    double alpha_order_imb;
    double alpha_trade_imb;
    double alpha_struct;
    double k0;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k1;
    double k2;
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
    double alpha_order_imb;
    double alpha_trade_imb;
    double alpha_struct;
    double k0;
    double spread_multiplier;
    double inventory_target;
    double residual_signal_quality;
    double tox;
    double k1;
    double k2;

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

// =========================
// MARKET MICROSTRUCTURE UTILS
// =========================
class MarketConfig {
public:
    std_string exchange;
    std_string mode;
    std_string market;

    std_string struct_model;
    std_string regime_model;
    std_string micro_signal_model;
    std_string residual_model;
    std_string toxicity_model;

    std_string instrument;
    std_string instrument_upper;

    int64_t exchange_latency;
    double gamma;
    double base_size;
    double max_inv;

    double initial_cash;
    double maker_fee_rate;
    double taker_fee_rate;

    double tick_size = 0.0;
    double step_size = 0.0;
    size_t price_precision = 0;
    size_t qty_precision = 0;

    std_string folder_path;
    std_string host;
    int16_t port;

    std_string api_key;
    std_string api_secret;
    std_string base_url;
    std_string endpoint;
    std_string hostname;

    MarketConfig(const json& params) {initialize(params);}

    int get_precision(double step){
        int precision = 0;
        while(step < 1.0){
            step *= 10.0;
            precision++;
            if(precision > 18) break; // safety guard
        }
        return precision;
    }

    void initialize(const json& params){
        exchange = params["exchange"].get<std_string>();
        mode = params["mode"].get<std_string>();
        market = params["market"].get<std_string>();

        struct_model = params["models"]["struct_model"].get<std_string>();
        regime_model = params["models"]["regime_model"].get<std_string>();
        micro_signal_model = params["models"]["micro_signal_model"].get<std_string>();
        residual_model = params["models"]["residual_model"].get<std_string>();
        toxicity_model = params["models"]["toxicity_model"].get<std_string>();

        instrument = params["instrument"].get<std_string>();
        instrument_upper = instrument;
        transform(instrument_upper.begin(), instrument_upper.end(), instrument_upper.begin(), [](unsigned char c){return toupper(c);});

        exchange_latency = params["exchange_latency"].get<int64_t>();
        gamma = params["gamma"].get<double>();
        base_size = params["base_size"].get<double>();
        max_inv = params["max_inv"].get<double>();

        initial_cash = params["initial_cash"].get<double>();
        maker_fee_rate = params["fees"]["maker_fee_rate"].get<double>();
        taker_fee_rate = params["fees"]["taker_fee_rate"].get<double>();

        folder_path = params["folder_path"].get<std_string>();
        host = params["server_config"]["host"].get<std_string>();
        port = params["server_config"]["port"].get<int16_t>();

        api_key = params["api"]["api_key"].get<std_string>();
        api_secret = params["api"]["api_secret"].get<std_string>();
        hostname = params["api"]["hostname_" + market].get<std_string>();
        base_url = params["api"]["base_url_" + market].get<std_string>();
        endpoint = params["api"]["endpoint_" + market].get<std_string>();
        
        cout << "exchange: " << exchange + "_" + market << ", mode: " << mode << ", instrument: " << instrument << ", instrument_upper: " << instrument_upper << "\n";
        cout << "exchange_latency: " << exchange_latency << ", gamma: " << gamma << ", base_size: " << base_size << ", max_inv: " << max_inv << "\n";
        cout << "initial_cash: " << initial_cash << ", maker_fee_rate: " << maker_fee_rate << ", taker_fee_rate: " << taker_fee_rate << "\n";
        cout << "folder_path: " << folder_path << ", host: " << host << ", port: " << port << "\n";
        cout << "hostname: " << hostname << ", base_url: " << base_url << ", endpoint: " << endpoint << "\n";

        std_string url = "https://" + base_url + endpoint + "/exchangeInfo?symbol=" + instrument_upper;

        auto r = cpr::Get(cpr::Url{url});
        auto data = json::parse(r.text);
        auto filters = data["symbols"][0]["filters"];
        
        for(auto& f: filters){
            if(f["filterType"] == "PRICE_FILTER"){
                tick_size = stod(f["tickSize"].get<std_string>());
                price_precision = get_precision(tick_size);
                cout << "tick_size: " << tick_size << ", price_precision: " << price_precision << "\n";
            }

            if(f["filterType"] == "LOT_SIZE"){
                step_size = stod(f["stepSize"].get<std_string>());
                qty_precision   = get_precision(step_size);
                cout << "step_size: " << step_size << ", qty_precision: " << qty_precision << "\n";
            }
        }
    }

    int64_t to_tick(const double& price) const {
        return static_cast<int64_t>(llround(price / tick_size));
    }

    double from_tick(const int64_t& tick) const {
        return tick * tick_size;
    }

    double normalize_qty(const double& qty) const {
        double rounded = floor(qty / step_size) * step_size;

        if(rounded < step_size) return 0.0;

        return rounded;
    }

    double normalize_bid(const double& price) const {
        return floor(price / tick_size) * tick_size;
    }

    double normalize_ask(const double& price) const {
        return ceil(price / tick_size) * tick_size;
    }

    std_string format_ms_precise(const int64_t& ts) const{
        time_t t = ts / 1000;
        tm tm = *localtime(&t);

        int ms = ts % 1000;

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << setw(3) << setfill('0') << ms;

        return oss.str();
    }
};

class OrderBook {
public:
    MarketConfig& config;

    int64_t last_update_id = 0;

    std::map<int64_t, double, greater<>> bids;
    std::map<int64_t, double> asks;
    
    OrderBook(MarketConfig& config): config(config) {}

    pair<int64_t, double> best_bid(){
        if(bids.empty()) return {0, 0.0};
        return *bids.begin();
    }

    pair<int64_t, double> best_ask(){
        if(asks.empty()) return {0, 0.0};
        return *asks.begin();
    }

    double mid(){
        if(bids.empty() || asks.empty()) return 0.0;

        auto [bid_tick, bid_size] = best_bid();
        auto [ask_tick, ask_size] = best_ask();

        return config.from_tick((bid_tick + ask_tick) / 2);
    }

    pair<int64_t, json> initialize_from_binance(int limit = 1000){

        std_string url = "https://" + config.base_url + config.endpoint + "/depth?symbol=" + config.instrument_upper + "&limit=" + to_string(limit);
        cout << "initialize_from_binance url: " << url << "\n";
        auto r = cpr::Get(cpr::Url{url});
        auto snapshot = json::parse(r.text);

        last_update_id = snapshot["lastUpdateId"].get<int64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            bids[config.to_tick(p)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            asks[config.to_tick(p)] = q;
        }

        cout << "SNAPSHOT FETCHED: " << last_update_id << "\n";
        cout << "ORDER BOOK INITIALIZED\n";

        return {last_update_id, snapshot};
    }

    pair<int64_t, json> initialize_from_orderbook_snapshot(const json& snapshot){

        last_update_id = snapshot["lastUpdateId"].get<int64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            bids[config.to_tick(p)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            asks[config.to_tick(p)] = q;
        }

        cout << "SNAPSHOT FETCHED: " << last_update_id << "\n";
        cout << "ORDER BOOK INITIALIZED\n";

        return {last_update_id, snapshot};
    }

    void apply_delta(const Depth& entry){

        for(auto& [price_tick, q]: entry.bid_delta){
            if(q == 0.0) bids.erase(price_tick);
            else bids[price_tick] = q;
        }

        for(auto& [price_tick, q]: entry.ask_delta){
            if(q == 0.0) asks.erase(price_tick);
            else asks[price_tick] = q;
        }
    }
};

class RegimeModel {
public:
    using Vec = vector<double>;
    using Mat = vector<vector<double>>;

    std_string model_name;
    std_string target;
    int64_t horizon_ms;

    Mat means;
    vector<Mat> cov_inv;

    Vec log_det_cov;
    Vec log_weights;

    Vec scaler_mean;
    Vec scaler_scale;

    int n_regimes;
    vector<std_string> feature_cols;
    vector<std_string> regime_labels;

    int K;
    int D;

    // reusable buffers (IMPORTANT OPTIMIZATION)
    Vec x_input;
    Vec x_scaled;
    Vec diff;
    Vec scores;

    static constexpr double LOG_2PI = 1.8378770664093453;

    RegimeModel(const json& artifact){
        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<int64_t>();

        means = artifact["means"].get<Mat>();
        cov_inv = artifact["cov_inv"].get<vector<Mat>>();

        log_det_cov = artifact["log_det_cov"].get<Vec>();
        log_weights = artifact["log_weights"].get<Vec>();
   
        scaler_mean = artifact["scaler_mean"].get<Vec>();
        scaler_scale = artifact["scaler_scale"].get<Vec>();

        n_regimes = artifact["n_regimes"].get<int>();
        feature_cols = artifact["feature_cols"].get<vector<std_string>>();
        regime_labels = artifact["regime_labels"].get<vector<std_string>>();

        K = means.size();
        D = means[0].size();

        // allocate once
        x_input.resize(D);
        x_scaled.resize(D);
        diff.resize(D);
        scores.resize(K);

        cout << "INITIALIZED: " << model_name << " target: " << target << "\n";
    }

    void pack_features(const Regime& regime){
        x_input[0] = regime.volatility;
        x_input[1] = regime.spread;
        x_input[2] = regime.order_imbalance;
        x_input[3] = regime.trade_imbalance;
        x_input[4] = regime.quote_churn;
        x_input[5] = regime.inventory;
        x_input[6] = regime.inventory_vol;
        x_input[7] = regime.microprice_error;
    }

    void scale(){
        for(int i = 0; i < D; i++){
            x_scaled[i] = (x_input[i] - scaler_mean[i]) / scaler_scale[i];
        }
    }

    tuple<std_string, int, double> predict(const Regime& regime){

        pack_features(regime);
        scale();

        int best_k = -1; //gaussian component regime
        double best_score = -numeric_limits<double>::infinity();

        for(int k = 0; k < K; k++){
            const auto& mu = means[k];
            const auto& inv = cov_inv[k];

            // compute diff = x - mu
            for(int i = 0; i < D; i++){
                diff[i] = x_scaled[i] - mu[i];
            }

            // symmetric quadratic form (OPTIMIZED)
            double quad = 0.0;

            for(int i = 0; i < D; i++){
                double di = diff[i];
                const double* row = inv[i].data();

                quad += di * row[i] * di;

                for(int j = i + 1; j < D; j++){
                    double dj = diff[j];
                    quad += 2.0 * di * row[j] * dj;
                }
            }

            double logp = log_weights[k] - 0.5 * (D * LOG_2PI + log_det_cov[k] + quad);

            scores[k] = logp;

            if(logp > best_score){
                best_score = logp;
                best_k = k;
            }
        }

        // log-sum-exp normalization (stable softmax)
        double max_log = best_score;
        double sum = 0.0;

        for(double s: scores) sum += exp(s - max_log);

        double log_norm = max_log + log(sum);
        double prob = exp(scores[best_k] - log_norm);

        return {regime_labels[best_k], best_k, prob};
    }
};

class MicroSignalModel {
public:
    std_string model_name;
    std_string target;
    int64_t horizon_ms;

    double intercept;
    double beta;
    double ic;
    double rank_ic;

    MicroSignalModel(const json& artifact){
        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<int64_t>();
        intercept = artifact["intercept"].get<double>();
        beta = artifact["beta"].get<double>();
        ic = artifact["ic"].get<double>();
        rank_ic = artifact["rank_ic"].get<double>();

        cout << "INITIALIZED: " << model_name << " target: " << target << "\n";
    }

    double predict(const Features& features){
        double micro_signal = features.microprice_dev / features.mid; // (microprice - mid) / mid;
        double micro_signal_delta = features.mid * (intercept + beta * micro_signal);
        
        return micro_signal_delta;
    }
};

class ResidualModel {
public:
    std_string model_name;
    std_string target;
    int64_t horizon_ms;
    vector<std_string> feature_cols;
    int D;

    BoosterHandle booster;
    vector<float> x_input;
    DMatrixHandle dmat = nullptr;

    ~ResidualModel(){
        if(dmat) XGDMatrixFree(dmat);
    }

    ResidualModel(const json& artifact){

        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<int64_t>();
        feature_cols = artifact["feature_cols"].get<vector<std_string>>();
        D = artifact["feature_dim"].get<int>();

        if(D < 7) throw runtime_error("feature_dim too small");
        x_input.resize(D);

        std_string model_file = artifact["model_file"].get<std_string>();
        XGBoosterCreate(nullptr, 0, &booster);
        XGBoosterLoadModel(booster, model_file.c_str());

        cout << "INITIALIZED: " << model_name << " target: " << target << "\n";
    }

    void pack_features(const Features& f){
        x_input[0] = f.spread;
        x_input[1] = f.order_imbalance;
        x_input[2] = f.trade_imbalance;
        x_input[3] = f.inventory;
        x_input[4] = f.volatility;
        x_input[5] = f.queue_ahead_bid;
        x_input[6] = f.queue_ahead_ask;
    }

    double predict(const Features& f){
        pack_features(f);

        if(dmat) XGDMatrixFree(dmat);
        XGDMatrixCreateFromMat(x_input.data(), 1, D, NAN, &dmat);

        bst_ulong out_len;
        const float* out_result;

        XGBoosterPredict(
            booster,
            dmat,
            0,          // option_mask
            0,          // ntree_limit
            0,          // training = false (IMPORTANT)
            &out_len,
            &out_result
        );

        return out_result[0];
    }
};

class ToxicityModel {
public:
    std_string model_name;
    std_string target;
    int64_t horizon_ms;
    vector<std_string> feature_cols;
    int D;

    BoosterHandle booster;
    vector<float> x_input;
    DMatrixHandle dmat = nullptr;

    ~ToxicityModel(){
        if(dmat) XGDMatrixFree(dmat);
    }

    ToxicityModel(const json& artifact){

        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<int64_t>();
        feature_cols = artifact["feature_cols"].get<vector<std_string>>();
        D = artifact["feature_dim"].get<int>();
        
        if(D < 7) throw runtime_error("feature_dim too small");
        x_input.resize(D);

        std_string model_file = artifact["model_file"].get<std_string>();
        XGBoosterCreate(nullptr, 0, &booster);
        XGBoosterLoadModel(booster, model_file.c_str());

        cout << "INITIALIZED: " << model_name << ", target: " << target << "\n";
    }

    void pack_features(const Features& f){
        x_input[0] = f.microprice_dev;
        x_input[1] = f.order_imbalance;
        x_input[2] = f.trade_imbalance;
        x_input[3] = f.volatility;
        x_input[4] = f.spread;
        x_input[5] = f.queue_ahead_bid;
        x_input[6] = f.queue_ahead_ask;
    }

    double predict(const Features& f){
        pack_features(f);

        if(dmat) XGDMatrixFree(dmat);
        XGDMatrixCreateFromMat(x_input.data(), 1, D, NAN, &dmat);

        bst_ulong out_len;
        const float* out_result;

        XGBoosterPredict(
            booster,
            dmat,
            0,          // option_mask
            0,          // ntree_limit
            0,          // training = false (IMPORTANT)
            &out_len,
            &out_result
        );

        return out_result[0];
    }
};

class MarketMakingStrategy {
public:
    MarketConfig& config;

    // Models
    std_string struct_model;
    unique_ptr<RegimeModel> regime_model;
    unique_ptr<MicroSignalModel> micro_signal_model;
    unique_ptr<ResidualModel> residual_model;
    unique_ptr<ToxicityModel> toxicity_model;

    MarketMakingStrategy(MarketConfig& config) : config(config)
    {
        struct_model       = config.struct_model;
        regime_model       = load_model<RegimeModel>(config.regime_model);
        micro_signal_model = load_model<MicroSignalModel>(config.micro_signal_model);
        residual_model     = load_model<ResidualModel>(config.residual_model);
        toxicity_model     = load_model<ToxicityModel>(config.toxicity_model);
    }

    template<typename T> unique_ptr<T> load_model(const std_string& model){
        if(model.empty()) return nullptr;

        std_string file = "data/" + model + ".json";
        ifstream f(file);
        json artifact;
        f >> artifact;

        return make_unique<T>(artifact);
    }

    Policy detect_regime(const Regime& regime){
        
        Policy policy;
        
        if(regime_model == nullptr) return policy;

        auto [pred_regime, regime_id, prob] = regime_model->predict(regime);

        policy.regime = pred_regime;
        policy.regime_id = regime_id;
        policy.regime_prob = prob;

        if(pred_regime == "trending"){
            policy.alpha_order_imb = 0.6;
            policy.alpha_trade_imb = 0.2;
            policy.alpha_struct = 0.8;
            policy.spread_multiplier = 2.0;
            policy.k0 = 1.2;
            policy.inventory_target = (regime.trade_imbalance > 0) ? 1.0 : -1.0;
        }
        else if(pred_regime == "toxic"){
            policy.alpha_order_imb = 0.05;
            policy.alpha_trade_imb = 0.01;
            policy.alpha_struct = 0.4;
            policy.spread_multiplier = 1.5;
            policy.k0 = 0.5;
            policy.inventory_target = 0.0;
        }
        else{ // low_vol / normal / competitive
            policy.alpha_order_imb = 0.15;
            policy.alpha_trade_imb = 0.05;
            policy.alpha_struct = 0.2;
            policy.spread_multiplier = 0.7;
            policy.k0 = 1.0;
            policy.inventory_target = 0.0;
        }

        return policy;
    }

    // -------------------------
    // CORE ALPHA SIGNALS
    // -------------------------
    pair<double, double> compute_fair_and_micro(State& state, const Policy& policy){
        
        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9);

        double fair = microprice
            + policy.alpha_order_imb * state.order_imbalance // order imbalance, resting flow
            + policy.alpha_trade_imb * state.trade_imbalance; // trade imbalance, aggressive trade flow

        return {fair, microprice};
    }

    double compute_spread(const Features& features, const Policy& policy, const Toxicity& toxicity){
    
        double sigma = features.volatility;

        double base = 0.03; // keep base spread 0.03, since testnet futures have unusually wide spread
        double vol_component = 3.0 * sigma;

        double raw_spread = max(base, vol_component);
        double spread = raw_spread * policy.spread_multiplier * (1.0 + toxicity.k1 * toxicity.tox);

        return spread;
    }

    double compute_skew(State& state, const Policy& policy){
        
        double sigma = state.get_vol();
        double mid = state.market_book.mid();

        double effective_inventory = state.inventory - policy.inventory_target;
        
        return -effective_inventory * config.gamma * sigma * mid;
    }

    double compute_signal_quality(const auto& log){

        const size_t n = log.size();

        if(n < 20) return 1.0;
        // if(n < 10) return 0.0; trust or no trust?

        vector<double> pred_arr, realized_arr;
        pred_arr.reserve(n);
        realized_arr.reserve(n);

        for(const auto& x: log){
            pred_arr.push_back(x.pred);
            realized_arr.push_back(x.realized);
        }

        auto mean = [](const vector<double>& v){
            double sum = 0.0;
            for(double x: v) sum += x;
            return sum / v.size();
        };

        double mean_pred = mean(pred_arr);
        double mean_realized = mean(realized_arr);

        // -----------------------------
        // Pearson IC
        // -----------------------------
        double cov = 0.0, var_pred = 0.0, var_realized = 0.0;

        for(size_t i = 0; i < n; i++){
            double dev_pred = pred_arr[i] - mean_pred;
            double dev_realized = realized_arr[i] - mean_realized;

            cov += dev_pred * dev_realized;
            var_pred += dev_pred * dev_pred;
            var_realized += dev_realized * dev_realized;
        }

        double ic = 0.0;
        if(var_pred > 1e-12 && var_realized > 1e-12)
            ic = cov / sqrt(var_pred * var_realized);

        if(isnan(ic)) ic = 0.0;

        // -----------------------------
        // Rank IC (Spearman)
        // -----------------------------
        auto rank = [&](const vector<double>& v){
            vector<size_t> id_arr(n);
            vector<double> rank_arr(n);

            iota(id_arr.begin(), id_arr.end(), 0);
            sort(id_arr.begin(), id_arr.end(), [&](const size_t& a, const size_t& b){
                if (v[a] < v[b]) return true;
                if (v[a] > v[b]) return false;
                return a < b;
            });

            size_t i = 0;
            while(i < n){
                size_t j = i;

                // find tie group
                while (j + 1 < n && abs(v[id_arr[j + 1]] - v[id_arr[i]]) < 1e-12) j++;
        
                // average rank for ties
                double avg_rank = (i + j) / 2.0;

                for(size_t k = i; k <= j; k++) rank_arr[id_arr[k]] = avg_rank;

                i = j + 1;
            }
            return rank_arr;
        };

        vector<double> rank_pred = rank(pred_arr);
        vector<double> rank_realized = rank(realized_arr);

        double mean_pred_rank = mean(rank_pred);
        double mean_realized_rank = mean(rank_realized);

        double cov_rank = 0.0, var_pred_rank = 0.0, var_realized_rank = 0.0;

        for(size_t i = 0; i < n; i++){
            double dev_pred_rank = rank_pred[i] - mean_pred_rank;
            double dev_realized_rank = rank_realized[i] - mean_realized_rank;

            cov_rank += dev_pred_rank * dev_realized_rank;
            var_pred_rank  += dev_pred_rank * dev_pred_rank;
            var_realized_rank  += dev_realized_rank * dev_realized_rank;
        }

        double rank_ic = 0.0;
        if(var_pred_rank > 1e-12 && var_realized_rank > 1e-12)
            rank_ic = cov_rank / sqrt(var_pred_rank * var_realized_rank);

        if(isnan(rank_ic)) rank_ic = 0.0;

        // -----------------------------
        // Directional accuracy
        // -----------------------------
        double hits = 0.0;
        for(size_t i = 0; i < n; i++){
            if((pred_arr[i] > 0) == (realized_arr[i] > 0)) hits += 1.0;
        }

        double hit_rate = hits / n;
        double directional_score = (hit_rate - 0.5) * 2.0;  // [-1, 1]

        // -----------------------------
        // Final score
        // -----------------------------
        double vol_pred = sqrt(var_pred / n);
        double stability_penalty = exp(-vol_pred * 50.0);

        double raw = 0.5 * ic + 0.3 * rank_ic + 0.2 * directional_score;
        double signal_quality = stability_penalty * tanh(3.0 * raw);

        return 0.3 + 1.4 * ((signal_quality + 1.0) / 2.0);

        // This converts:
        // signal_quality ∈ [-1, +1]

        // into: 
        // output ∈ [0.3, 1.7]

        // 0.3  = very bad signal
        // 1.0  = neutral / no adjustment
        // 1.7  = very strong signal
    }

    double compute_struct_delta(const Features& features, const Policy& policy){
        
        if(struct_model != "blended_AS"){
            return 0.0;
        }

        // Move my current mid price toward the structural fair so value by some fraction.
        double reservation = features.fair + features.skew;
        double struct_delta = policy.alpha_struct * (reservation - features.mid);

        return struct_delta;
    }

    double compute_micro_signal_delta(const Features& features){

        if(micro_signal_model == nullptr) return 0.0;

        return micro_signal_model->predict(features);
    }

    pair<double, double> compute_residual_delta(State& state, const double& struct_delta, const double& micro_signal_delta, 
                                                const Features& features, const Policy& policy){
        
        if(residual_model == nullptr) return {0.0, 0.0};

        double reservation = features.mid + struct_delta + micro_signal_delta;
        double expected_log_return = residual_model->predict(features); // y^ in this case

        ResidualPred residual_pred; //store residual prediction
        residual_pred.ts = state.last_depth_ts;
        residual_pred.horizon_ms = residual_model->horizon_ms;
        residual_pred.pred = expected_log_return;
        residual_pred.reservation = reservation;
        state.mfs.residual_predictions.push_back(residual_pred);

        double residual_signal_quality = compute_signal_quality(state.mfs.residual_signal_log);
        double effective_k = policy.k0 * residual_signal_quality;

        double residual_center = reservation * exp(expected_log_return * effective_k); //scale log return by effective_k
        return {residual_center - reservation, residual_signal_quality};
    }

    Toxicity compute_toxicity(const Features& features){
        
        Toxicity toxicity;

        if(toxicity_model == nullptr) return toxicity;

        double prediction = toxicity_model->predict(features);
        // double toxicity_signal_quality = compute_toxicity_signal_quality(state.mfs.toxicity_signal_log);

        toxicity.tox = -prediction; //trained on future markout, negative markout = toxic
        // toxicity.k1 *= toxicity_signal_quality;
        // toxicity.k2 *= toxicity_signal_quality;
        // toxicity.pred = prediction;

        return toxicity;
    }

    // -------------------------
    // FINAL QUOTE GENERATION
    // -------------------------
    Signal generate_quotes(State& state){ // put execution here in the mean time

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;

        Regime regime = state.get_regime();
        Policy policy = detect_regime(regime);

        auto [fair, microprice] = compute_fair_and_micro(state, policy);
        double skew = compute_skew(state, policy);

        Features features;
        features.mid = mid;
        features.fair = fair;
        features.skew = skew;
        features.microprice = microprice;
        features.microprice_dev = microprice - mid;
        features.spread = best_ask - best_bid;
        features.order_imbalance = state.order_imbalance;
        features.trade_imbalance = state.trade_imbalance;
        features.inventory = state.inventory;
        features.volatility = state.get_vol();
        features.queue_ahead_bid = state.bid_queue_ahead.second;
        features.queue_ahead_ask = state.ask_queue_ahead.second;

        // -------------------------
        // ALPHA STACK
        // -------------------------
        double struct_delta = compute_struct_delta(features, policy);

        double micro_signal_delta = compute_micro_signal_delta(features);

        auto [residual_delta, residual_signal_quality] = compute_residual_delta(state, struct_delta, micro_signal_delta, features, policy);

        double center = mid + struct_delta + micro_signal_delta + residual_delta;
        Toxicity toxicity = compute_toxicity(features);

        double spread = compute_spread(features, policy, toxicity);
        double half = spread / 2.0;

        double bid = center - half;
        double ask = center + half;
        double tick = config.tick_size;

        bid = min(bid, best_bid);
        ask = max(ask, best_ask);

        if(bid >= ask){
            bid = best_bid - tick;
            ask = best_ask + tick;
        }

        bid = config.normalize_bid(bid);
        ask = config.normalize_ask(ask);

        double bid_delta = abs(best_bid - state.mfs.prev_best_bid);
        double ask_delta = abs(best_ask - state.mfs.prev_best_ask);

        Signal signal;
        signal.ts = state.last_depth_ts;
        signal.trade_latency = state.trade_latency;
        signal.depth_latency = state.depth_latency;
        signal.exchange_latency = state.exchange_latency;
        signal.mid = features.mid;
        signal.microprice = features.microprice;
        signal.microprice_dev = features.microprice_dev;
        signal.microprice_error = features.mid - features.microprice;
        signal.spread = features.spread;
        signal.best_bid = best_bid;
        signal.best_ask = best_ask;

        signal.order_imbalance = features.order_imbalance;
        signal.trade_imbalance = features.trade_imbalance;
        signal.volatility = features.volatility;
        signal.queue_ahead_bid = features.queue_ahead_bid;
        signal.queue_ahead_ask = features.queue_ahead_ask;

        signal.inventory = features.inventory;
        signal.realized_pnl = state.realized_pnl;
        signal.unrealized_pnl = state.get_unrealized_pnl(mid);
        signal.total_pnl = state.get_pnl(mid);
        signal.equity = state.cash + state.inventory * mid;

        signal.fair = features.fair;
        signal.skew = features.skew;
        signal.struct_delta = struct_delta;
        signal.micro_signal_delta = micro_signal_delta;
        signal.residual_delta = residual_delta;
        signal.reservation = center;

        signal.regime = policy.regime;
        signal.regime_id = policy.regime_id;
        signal.regime_prob = policy.regime_prob;
        signal.alpha_order_imb = policy.alpha_order_imb;
        signal.alpha_trade_imb = policy.alpha_trade_imb;
        signal.alpha_struct = policy.alpha_struct;
        signal.k0 = policy.k0;
        signal.spread_multiplier = policy.spread_multiplier;
        signal.inventory_target = policy.inventory_target;
        signal.residual_signal_quality = residual_signal_quality;
        signal.toxicity = toxicity;

        signal.bid_delta = bid_delta;
        signal.ask_delta = ask_delta;
        signal.quote_churn = bid_delta + ask_delta;

        signal.my_bid = bid;
        signal.my_ask = ask;

        return signal;
    }
};

class State {
public:
    MarketConfig& config;
    OrderBook market_book;
    MarketFeatureState mfs;
    optional<Signal> last_signal;

    double last_mid = 0.0;
    double order_imbalance = 0.0;
    double trade_imbalance = 0.0;
    double ewma_var = 0.0;

    double cash;
    double inventory = 0.0;
    double realized_pnl = 0.0;
    double avg_entry_price = 0.0;
    double mark_price = 0.0;

    deque<double> equity_history;
    deque<double> return_history;
    double last_equity = 0.0;
    double fees_paid = 0.0;

    Hawkes hawkes;
    pair<int64_t, double> bid_queue_ahead = {-1, 0.0}; // 1 price_tick per side only, init with -1 price_tick
    pair<int64_t, double> ask_queue_ahead = {-1, 0.0}; // 1 price_tick per side only

    optional<Trade> last_trade;
    optional<Order> last_fill_candidate;
    optional<Order> last_order_update;

    int64_t time = 0;
    int64_t last_trade_ts = 0;
    int64_t last_depth_ts = 0;
    int64_t trade_latency = 0;
    int64_t depth_latency = 0;
    int64_t exchange_latency = 0;

    bool initialized = false;

    State(MarketConfig& config) : config(config), market_book(config)
    {
        cash = config.initial_cash;
    }

    double get_vol(){
        return sqrt(ewma_var);
    }

    void update_queue_from_depth(const Depth& entry){

        // Since trade handler already captures executions, depth handler is mostly for cancellations.
        double beta = 0.9; // double beta = 0.2 - 0.4; lower beta to prevent double counting, this simulates poisson process now

        for(auto& [price_tick, new_qty]: entry.bid_delta){ //find price tick from bid_queue_ahead
            
            if(price_tick == bid_queue_ahead.first){
                auto it = market_book.bids.find(price_tick);
                double old_qty = (it != market_book.bids.end()) ? it->second : 0.0;

                if(old_qty > 0.0){
                    double depletion = max(0.0, old_qty - new_qty);
                    
                    if(depletion > 0.0){
                        hawkes.update(depletion, entry.ts); // hawkes process

                        bid_queue_ahead.second = max(0.0, bid_queue_ahead.second - hawkes.beta * depletion);
                        cout << "bid order queue_ahead @ " << price_tick << ": " << bid_queue_ahead.second << ", hawkes.beta * depletion: " << hawkes.beta * depletion << "\n";
                    }
                }
                break; // no need to continue scanning
            }
        }

        for(auto& [price_tick, new_qty]: entry.ask_delta){

            if(price_tick == ask_queue_ahead.first){
                auto it = market_book.asks.find(price_tick);
                double old_qty = (it != market_book.asks.end()) ? it->second : 0.0;

                if(old_qty > 0.0){
                    double depletion = max(0.0, old_qty - new_qty);

                    if(depletion > 0.0){
                        hawkes.update(depletion, entry.ts); // hawkes process

                        ask_queue_ahead.second = max(0.0, ask_queue_ahead.second - hawkes.beta * depletion);
                        cout << "ask order queue_ahead @ " << price_tick << ": " << ask_queue_ahead.second << ", hawkes.beta * depletion: " << hawkes.beta * depletion << "\n";
                    }
                }
                break; // no need to continue scanning
            }
        }
    }

    void update_vol(){
        double alpha = 0.90;
        double mid = market_book.mid();

        if(last_mid != 0.0){
            double r = log(mid / last_mid);
            ewma_var = alpha * ewma_var + (1 - alpha) * r * r;
        }
        last_mid = mid;
    }

    void compute_order_imbalance(){
        auto [bid_tick, bid_size] = market_book.best_bid();
        auto [ask_tick, ask_size] = market_book.best_ask();

        order_imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1e-9);
    }

    template <typename T> void push_limited(deque<T>& dq, const T& value, size_t maxlen = 10){
        dq.push_back(value);
        if(dq.size() > maxlen) dq.pop_front();
    }

    void update_market_feature_state(){
        auto& book = market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double spread = best_ask - best_bid;
        double microprice = (best_ask * bid_size + best_bid * ask_size) /(bid_size + ask_size + 1e-9);

        double mid_return = (last_mid != 0.0) ? (mid - last_mid) / last_mid : 0.0;
        last_mid = mid;

        double quote_churn = 0.0;
        if(mfs.prev_best_bid != 0.0 && mfs.prev_best_ask != 0.0){
            double bid_delta = abs(best_bid - mfs.prev_best_bid);
            double ask_delta = abs(best_ask - mfs.prev_best_ask);
            quote_churn = bid_delta + ask_delta;
        }

        mfs.prev_best_bid = best_bid;
        mfs.prev_best_ask = best_ask;

        push_limited(mfs.mid_returns, mid_return);
        push_limited(mfs.spread, spread);
        push_limited(mfs.order_imbalance, order_imbalance);
        push_limited(mfs.trade_imbalance, trade_imbalance);
        push_limited(mfs.quote_churn, quote_churn);
        push_limited(mfs.inventory, inventory);
        push_limited(mfs.microprice_error, mid - microprice);
    }

    void update_residual_realization(){

        while(!mfs.residual_predictions.empty() && 
        last_depth_ts - mfs.residual_predictions.front().ts >= mfs.residual_predictions.front().horizon_ms){
            
            auto& entry = mfs.residual_predictions.front();
            mfs.residual_predictions.pop_front();
            
            entry.realized = log(last_mid / entry.reservation);
            push_limited(mfs.residual_signal_log, entry, 2000); //2000 in queue, 200s window
        }
    }

    // void update_toxicity_realization(){
    //     int64_t now = max(last_depth_ts, last_trade_ts);

    //     while(!mfs.toxicity_predictions.empty() && 
    //     now - mfs.toxicity_predictions.front().ts >= mfs.toxicity_predictions.front().horizon_ms){
            
    //         auto& entry = mfs.toxicity_predictions.front();
    //         mfs.toxicity_predictions.pop_front();           
            
    //         entry.realized = p.fill_sign * (last_mid - entry.fill_price);
    //         push_limited(mfs.toxicity_signal_log, entry, 2000); //2000 in queue, 200s window
    //     }
    // }

    void update_performance(){
        auto [bid_tick, bid_size] = market_book.best_bid();
        auto [ask_tick, ask_size] = market_book.best_ask();

        if(bid_size <= 0 || ask_size <= 0) return;

        double bid = config.from_tick(bid_tick);
        double ask = config.from_tick(ask_tick);

        double mid = (bid + ask) / 2.0;
        double equity = cash + inventory * mid;

        if(last_equity > 0.0 && equity > 0.0){
            double r = log(equity / last_equity);

            if(isfinite(r)) return_history.push_back(r);
        }

        last_equity = equity;
        equity_history.push_back(equity);
    }

    double set_queue_position(const std_string& side, const int64_t& price_tick){

        double book_size = 0.0;

        if(side == "BUY"){
            auto it = market_book.bids.find(price_tick);
            if(it != market_book.bids.end()) book_size = it->second;
            
            bid_queue_ahead = {price_tick, book_size};
        }
        else{
            auto it = market_book.asks.find(price_tick);
            if(it != market_book.asks.end()) book_size = it->second;

            ask_queue_ahead = {price_tick, book_size};
        }

        return book_size;
    }

    void reset_queue_position(const std_string& side){

        if(side == "BUY") bid_queue_ahead = {-1, 0.0};
        else ask_queue_ahead = {-1, 0.0};
    }

    Regime get_regime(){

        auto mean = [](const deque<double>& q){
            if(q.empty()) return 0.0;
            double s = 0;
            for(auto x: q) s += x;
            return s / q.size();
        };

        auto stddev = [](const deque<double>& q){
            if(q.size() < 2) return 0.0;
            double m = 0;
            for(auto x: q) m += x;
            m /= q.size();

            double var = 0;
            for(auto x: q) var += (x - m) * (x - m);
            return sqrt(var / q.size());
        };

        Regime regime;
        regime.volatility = stddev(mfs.mid_returns);
        regime.spread = mean(mfs.spread);
        regime.order_imbalance = mean(mfs.order_imbalance);
        regime.trade_imbalance = mean(mfs.trade_imbalance);
        regime.quote_churn = mean(mfs.quote_churn);
        regime.inventory = mean(mfs.inventory);
        regime.inventory_vol = stddev(mfs.inventory);
        regime.microprice_error = mean(mfs.microprice_error);

        return regime;
    }

    void on_fill(const double& price, double fill_qty, const std_string& side, const bool& is_maker){

        // if (toxicity_model) {
            //     ToxicityPrediction p;

            //     p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
            //     p.horizon_ms = toxicity_model->horizon_ms;

            //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
            //     p.fill_price = fill_price;

            //     p.fill_sign = (side == "BUY") ? 1 : -1;

            //     state.market_feature_state.toxicity_predictions.push_back(std::move(p));
            // }

        double old_inv = inventory;
        double old_avg = avg_entry_price;

        double fill_value = price * fill_qty;
        double fee_rate = is_maker ? config.maker_fee_rate : config.taker_fee_rate;
        double fee = fill_value * fee_rate;
        fees_paid += fee;

        if(side == "BUY"){
            if(old_inv < 0){
                double close_qty = min(fill_qty, abs(old_inv));
                realized_pnl += close_qty * (old_avg - price);

                old_inv += close_qty;
                fill_qty -= close_qty;
            }

            inventory = old_inv;

            if(fill_qty > 0){
                double new_inv = old_inv + fill_qty;

                if(old_inv > 0){
                    avg_entry_price = (old_avg * old_inv + price * fill_qty) / new_inv;
                }
                else avg_entry_price = price;

                inventory = new_inv;
            }
            cash -= (fill_value + fee);
        }

        else{ // SELL
            if(old_inv > 0){
                double close_qty = min(fill_qty, old_inv);
                realized_pnl += close_qty * (price - old_avg);

                old_inv -= close_qty;
                fill_qty -= close_qty;
            }

            inventory = old_inv;

            if(fill_qty > 0){
                double new_inv = old_inv - fill_qty;

                if(old_inv < 0){
                    avg_entry_price = (old_avg * abs(old_inv) + price * fill_qty) / abs(new_inv);
                }
                else avg_entry_price = price;

                inventory = new_inv;
            }
            cash += (fill_value - fee);
        }

        if(inventory == 0.0) avg_entry_price = 0.0;
    }

    double get_unrealized_pnl(double mid){
        // return inventory * (mark_price - avg_entry_price); to align with binance stream unrealized_pnl

        return inventory * (mid - avg_entry_price);
    }

    double get_pnl(double mid){
        return realized_pnl + get_unrealized_pnl(mid) - fees_paid;
    }

    PerformanceMetrics compute_performance(){
        PerformanceMetrics performance;

        vector<double> returns;
        returns.reserve(return_history.size());

        for(double r: return_history){
            if(isfinite(r)) returns.push_back(r);
        }

        if(returns.size() < 30){
            cout << "PERFORMANCE - insufficient returns: " << returns.size() << "\n";
            return performance;
        }

        // -------------------------
        // Mean return
        // -------------------------
        double mean = 0.0;

        for(double r: returns) mean += r;

        mean /= returns.size();

        // -------------------------
        // Variance + downside variance
        // -------------------------
        double var = 0.0;
        double downside_var = 0.0;

        for(double r: returns){
            double diff = r - mean;
            var += diff * diff;

            double downside = min(r, 0.0); // MAR = 0
            downside_var += downside * downside;
        }

        var /= returns.size();
        downside_var /= returns.size();

        double std = sqrt(var);
        double downside_std = sqrt(downside_var);

        // -------------------------
        // Sharpe
        // -------------------------
        double annualization_factor = sqrt(10 * 60 * 60 * 24 * 365);

        if(std > 0.0 && isfinite(std)){
            double sharpe = mean / (std + 1e-9);
            performance.sharpe = sharpe;
            performance.annualized_sharpe = sharpe * annualization_factor;
        }

        // -------------------------
        // Sortino
        // -------------------------
        if(downside_std > 0.0 && isfinite(downside_std)){
            double sortino = mean / (downside_std + 1e-9);
            performance.sortino = sortino;
            performance.annualized_sortino = sortino * annualization_factor;
        }

        cout << "SHARPE: " << performance.sharpe
            << " Annualized SHARPE: " << performance.annualized_sharpe
            << " SORTINO: " << performance.sortino
            << " Annualized SORTINO: " << performance.annualized_sortino << "\n";

        return performance;
    }
};

class DatasetRecorder {
public:
    MarketConfig& config;
    State& state;
    const json& params;

    vector<EventRow> events;
    vector<SnapshotRow> snapshots;
    vector<TradeRow> trades;
    vector<QuoteRow> quotes;
    vector<FillRow> fills;

    std_string run_id;
    std_string folder_path;
    std_string orderbook_snapshot_path;
    std_string events_path;
    std_string snapshots_path;
    std_string trades_path;
    std_string quotes_path;
    std_string fills_path;

    static constexpr size_t EVENTS_CHUNK = 10000;
    static constexpr size_t SNAPSHOT_CHUNK = 10000;
    static constexpr size_t TRADE_CHUNK = 10000;
    static constexpr size_t QUOTE_CHUNK = 10000;
    static constexpr size_t FILL_CHUNK  = 5000; // usually smaller

    int events_id = 0;
    int snapshots_id = 0;
    int trades_id = 0;
    int quotes_id = 0;
    int fills_id = 0;

    DatasetRecorder(MarketConfig& config, State& state, const json& params)
        : config(config), state(state), params(params) {initialize();}

    std_string build_file_path(const std_string& file, const int& file_id){
        ostringstream ss;
        ss << folder_path << '/' << file << '_' << setw(6) << setfill('0') << file_id << ".parquet";
        
        cout << file << "_file_path: " << ss.str() << "\n";
        return ss.str();
    }

    void initialize(){
        auto now = system_clock::now();
        time_t now_t = system_clock::to_time_t(now);

        tm tm{};
        gmtime_s(&tm, &now_t);

        ostringstream ss;
        ss << put_time(&tm, "%Y%m%d_%H%M%S");

        run_id = ss.str();
        folder_path = "data/runs/run_" + run_id;
        filesystem::create_directories(folder_path);

        orderbook_snapshot_path = folder_path + "/orderbook_snapshot.json";
        events_path = build_file_path("events", events_id++);
        snapshots_path = build_file_path("snapshots", snapshots_id++);
        trades_path = build_file_path("trades", trades_id++);
        quotes_path = build_file_path("quotes", quotes_id++);
        fills_path = build_file_path("fills", fills_id++);
    }

    void export_orderbook_snapshot(const json& snapshot){
        ofstream(orderbook_snapshot_path) << snapshot.dump(4);
    }

    void log_event(const std_string& type, const int64_t& ts, const int64_t& local_ts, const int64_t& latency, const std_string& msg){

        EventRow row;
        row.type = type;
        row.ts = ts;
        row.local_ts = local_ts;
        row.latency = latency;
        row.msg = msg;

        events.push_back(move(row));

        if(events.size() >= EVENTS_CHUNK){
            events_path = build_file_path("events", events_id++);
            export_event_parquet(events);
            events.clear();
        }
    }

    void export_event_parquet(const vector<EventRow>& events){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        StringBuilder type_b(pool);
        Int64Builder ts_b(pool);
        Int64Builder local_ts_b(pool);
        Int64Builder latency_b(pool);
        StringBuilder msg_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& e: events){
            type_b.Append(e.type);
            ts_b.Append(e.ts);
            local_ts_b.Append(e.local_ts);
            latency_b.Append(e.latency);
            msg_b.Append(e.msg);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> type_arr, ts_arr, local_ts_arr, latency_arr, msg_arr;

        type_b.Finish(&type_arr);
        ts_b.Finish(&ts_arr);
        local_ts_b.Finish(&local_ts_arr);
        latency_b.Finish(&latency_arr);
        msg_b.Finish(&msg_arr);
        
        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("type", utf8()),
            field("ts", int64()),
            field("local_ts", int64()),
            field("latency", int64()),
            field("msg", utf8())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {type_arr, ts_arr, local_ts_arr, latency_arr, msg_arr});

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(events_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void stop(){
        state.update_performance();

        // flush events
        if(!events.empty()){
            events_path = build_file_path("events", events_id++);
            export_event_parquet(events);
        }

        // flush snapshots
        if(!snapshots.empty()){
            snapshots_path = build_file_path("snapshots", snapshots_id++);
            export_snapshot_parquet(snapshots);
        }

        // flush trades
        if(!trades.empty()){
            trades_path = build_file_path("trades", trades_id++);
            export_trade_parquet(trades);
        }

        // flush quotes
        if(!quotes.empty()){
            quotes_path = build_file_path("quotes", quotes_id++);
            export_quote_parquet(quotes);
        }

        // flush fills
        if(!fills.empty()){
            fills_path = build_file_path("fills", fills_id++);
            export_fill_parquet(fills);
        }

        // manifest
        json manifest = params;
        std_string manifest_path = folder_path + "/manifest.json";
        PerformanceMetrics performance = state.compute_performance();

        manifest["run_id"] = run_id;
        manifest["folder_path"] = folder_path;
        manifest["performance"] = {
            {"pnl", round(state.get_pnl(state.market_book.mid()) * 10000.0) / 10000.0},
            {"sharpe", round(performance.sharpe * 10000.0) / 10000.0},
            {"annualized_sharpe", round(performance.annualized_sharpe * 10000.0) / 10000.0},
            {"sortino", round(performance.sortino * 10000.0) / 10000.0},
            {"sortino", round(performance.annualized_sortino * 10000.0) / 10000.0},
            {"fees_paid", round(state.fees_paid * 10000.0) / 10000.0},
            {"fees_per_fill", state.fees_paid / (fills.size() + 1e-9)},
            {"pnl_per_fill", state.get_pnl(state.market_book.mid()) / (fills.size() + 1e-9)}
        };

        ofstream(manifest_path) << manifest.dump(2);
        cout << "DATASETS SAVED SUCCESSFULLY\n";
        cout << "Saved run → " << folder_path << "\n";
    }

    void log_snapshot(const Signal& signal){
        SnapshotRow row;
  
        row.ts = signal.ts;
        row.trade_latency = signal.trade_latency;
        row.depth_latency = signal.depth_latency;
        row.exchange_latency = signal.exchange_latency;
        row.symbol = config.instrument_upper;

        row.mid = signal.mid;
        row.mid_tick = config.to_tick(signal.mid);
        row.microprice = signal.microprice;
        row.microprice_dev = signal.microprice_dev;
        row.microprice_error = signal.microprice_error;
        row.spread = signal.spread;

        row.best_bid = signal.best_bid;
        row.best_ask = signal.best_ask;
        row.best_bid_tick = config.to_tick(signal.best_bid);
        row.best_ask_tick = config.to_tick(signal.best_ask);

        row.order_imbalance = signal.order_imbalance;
        row.trade_imbalance = signal.trade_imbalance;
        row.volatility = signal.volatility;
        row.queue_ahead_bid = signal.queue_ahead_bid;
        row.queue_ahead_ask = signal.queue_ahead_ask;

        row.inventory = signal.inventory;
        row.realized_pnl = signal.realized_pnl;
        row.unrealized_pnl = signal.unrealized_pnl;
        row.total_pnl = signal.total_pnl;
        row.equity = signal.equity;

        row.fair = signal.fair;
        row.skew = signal.skew;
        row.struct_delta = signal.struct_delta;
        row.micro_signal_delta = signal.micro_signal_delta;
        row.residual_delta = signal.residual_delta;
        row.reservation = signal.reservation;

        row.regime = signal.regime;
        row.regime_id = signal.regime_id;
        row.regime_prob = signal.regime_prob;
        row.alpha_order_imb = signal.alpha_order_imb;
        row.alpha_trade_imb = signal.alpha_trade_imb;
        row.alpha_struct = signal.alpha_struct;
        row.k0 = signal.k0;
        row.spread_multiplier = signal.spread_multiplier;
        row.inventory_target = signal.inventory_target;
        row.residual_signal_quality = signal.residual_signal_quality;
        row.tox = signal.toxicity.tox;
        row.k1 = signal.toxicity.k1;
        row.k2 = signal.toxicity.k2;

        row.my_bid = signal.my_bid;
        row.my_ask = signal.my_ask;
        row.my_bid_tick = config.to_tick(signal.my_bid);
        row.my_ask_tick = config.to_tick(signal.my_ask);

        row.bid_distance_touch = signal.my_bid - signal.best_bid;
        row.ask_distance_touch = signal.my_ask - signal.best_ask;
        row.bid_distance_spread = signal.my_bid - signal.best_ask;
        row.ask_distance_spread = signal.my_ask - signal.best_bid;

        row.bid_delta = signal.bid_delta;
        row.ask_delta = signal.ask_delta;
        row.quote_churn = signal.quote_churn;
    
        snapshots.push_back(move(row));

        if(snapshots.size() >= SNAPSHOT_CHUNK){
            snapshots_path = build_file_path("snapshots", snapshots_id++);
            export_snapshot_parquet(snapshots);
            snapshots.clear();
        }
    }

    void export_snapshot_parquet(const vector<SnapshotRow>& snapshots){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder trade_latency_b(pool);
        Int64Builder depth_latency_b(pool);
        Int64Builder exchange_latency_b(pool);
        StringBuilder symbol_b(pool);

        DoubleBuilder mid_b(pool);
        Int64Builder mid_tick_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);

        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);

        DoubleBuilder inventory_b(pool);
        DoubleBuilder realized_pnl_b(pool);
        DoubleBuilder unrealized_pnl_b(pool);
        DoubleBuilder total_pnl_b(pool);
        DoubleBuilder equity_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);
        
        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

        DoubleBuilder my_bid_b(pool);
        DoubleBuilder my_ask_b(pool);
        Int64Builder my_bid_tick_b(pool);
        Int64Builder my_ask_tick_b(pool);

        DoubleBuilder bid_distance_touch_b(pool);
        DoubleBuilder ask_distance_touch_b(pool);
        DoubleBuilder bid_distance_spread_b(pool);
        DoubleBuilder ask_distance_spread_b(pool);

        DoubleBuilder bid_delta_b(pool);
        DoubleBuilder ask_delta_b(pool);
        DoubleBuilder quote_churn_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: snapshots){
            ts_b.Append(r.ts);
            trade_latency_b.Append(r.trade_latency);
            depth_latency_b.Append(r.depth_latency);
            exchange_latency_b.Append(r.exchange_latency);
            symbol_b.Append(r.symbol);

            mid_b.Append(r.mid);
            mid_tick_b.Append(r.mid_tick);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);

            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);

            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);

            inventory_b.Append(r.inventory);
            realized_pnl_b.Append(r.realized_pnl);
            unrealized_pnl_b.Append(r.unrealized_pnl);
            total_pnl_b.Append(r.total_pnl);
            equity_b.Append(r.equity);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);

            my_bid_b.Append(r.my_bid);
            my_ask_b.Append(r.my_ask);
            my_bid_tick_b.Append(r.my_bid_tick);
            my_ask_tick_b.Append(r.my_ask_tick);

            bid_distance_touch_b.Append(r.bid_distance_touch);
            ask_distance_touch_b.Append(r.ask_distance_touch);
            bid_distance_spread_b.Append(r.bid_distance_spread);
            ask_distance_spread_b.Append(r.ask_distance_spread);

            bid_delta_b.Append(r.bid_delta);
            ask_delta_b.Append(r.ask_delta);
            quote_churn_b.Append(r.quote_churn);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, trade_latency_arr, depth_latency_arr, exchange_latency_arr, symbol_arr, mid_arr, mid_tick_arr;
        shared_ptr<Array> microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;
        shared_ptr<Array> inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, equity_arr;
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k1_arr, k2_arr;
        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_distance_touch_arr, ask_distance_touch_arr;
        shared_ptr<Array> bid_distance_spread_arr, ask_distance_spread_arr;
        shared_ptr<Array> bid_delta_arr, ask_delta_arr, quote_churn_arr;

        ts_b.Finish(&ts_arr);
        trade_latency_b.Finish(&trade_latency_arr);
        depth_latency_b.Finish(&depth_latency_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        symbol_b.Finish(&symbol_arr);

        mid_b.Finish(&mid_arr);
        mid_tick_b.Finish(&mid_tick_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);

        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);

        inventory_b.Finish(&inventory_arr);
        realized_pnl_b.Finish(&realized_pnl_arr);
        unrealized_pnl_b.Finish(&unrealized_pnl_arr);
        total_pnl_b.Finish(&total_pnl_arr);
        equity_b.Finish(&equity_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

        my_bid_b.Finish(&my_bid_arr);
        my_ask_b.Finish(&my_ask_arr);
        my_bid_tick_b.Finish(&my_bid_tick_arr);
        my_ask_tick_b.Finish(&my_ask_tick_arr);

        bid_distance_touch_b.Finish(&bid_distance_touch_arr);
        ask_distance_touch_b.Finish(&ask_distance_touch_arr);
        bid_distance_spread_b.Finish(&bid_distance_spread_arr);
        ask_distance_spread_b.Finish(&ask_distance_spread_arr);

        bid_delta_b.Finish(&bid_delta_arr);
        ask_delta_b.Finish(&ask_delta_arr);
        quote_churn_b.Finish(&quote_churn_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("trade_latency", int64()),
            field("depth_latency", int64()),
            field("exchange_latency", int64()),
            field("symbol", utf8()),

            field("mid", float64()),
            field("mid_tick", int64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),

            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),
            
            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),

            field("inventory", float64()),
            field("realized_pnl", float64()),
            field("unrealized_pnl", float64()),
            field("total_pnl", float64()),
            field("equity", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64()),

            field("my_bid", float64()),
            field("my_ask", float64()),
            field("my_bid_tick", int64()),
            field("my_ask_tick", int64()),

            field("bid_distance_touch", float64()),
            field("ask_distance_touch", float64()),
            field("bid_distance_spread", float64()),
            field("ask_distance_spread", float64()),

            field("bid_delta", float64()),
            field("ask_delta", float64()),
            field("quote_churn", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, trade_latency_arr, depth_latency_arr, exchange_latency_arr, symbol_arr, mid_arr, mid_tick_arr,
            microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, equity_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k1_arr, k2_arr,
            my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr,
            bid_distance_touch_arr, ask_distance_touch_arr,
            bid_distance_spread_arr, ask_distance_spread_arr,
            bid_delta_arr, ask_delta_arr, quote_churn_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(snapshots_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_trade(const Trade& trade){
        TradeRow row;

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9);

        double notional = trade.price * trade.qty;

        row.ts = trade.ts;
        row.symbol = config.instrument_upper;

        row.price = trade.price;
        row.price_tick = config.to_tick(trade.price);
        row.qty = trade.qty;
        row.side = trade.side;
        row.is_buyer_maker = (trade.side == "SELL") ? true : false;

        row.mid = mid;
        row.microprice = microprice;
        row.microprice_dev = microprice - mid;
        row.microprice_error = mid - microprice;
        row.spread = best_ask - best_bid;
        row.best_bid = best_bid;
        row.best_ask = best_ask;
        row.best_bid_tick = bid_tick;
        row.best_ask_tick = ask_tick;
        
        row.bid_size = bid_size;
        row.ask_size = ask_size;

        row.trade_to_mid = trade.price - mid;
        row.trade_to_microprice = trade.price - microprice;
        row.price_to_best_bid = trade.price - best_bid;
        row.price_to_best_ask = trade.price - best_ask;

        row.trade_side = trade.side == "SELL" ? "SELL_AGGRESSOR" : "BUY_AGGRESSOR";
        row.trade_sign = trade.side == "SELL" ? -1 : 1;

        row.notional = notional;
        row.log_notional = log1p(notional);
        row.intensity = trade.qty / (bid_size + ask_size + 1e-9);

        trades.push_back(move(row));

        if(trades.size() >= TRADE_CHUNK){
            trades_path = build_file_path("trades", trades_id++);
            export_trade_parquet(trades);
            trades.clear();
        }
    }

    void export_trade_parquet(const vector<TradeRow>& trades){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        StringBuilder symbol_b(pool);

        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);
        StringBuilder side_b(pool);
        BooleanBuilder is_buyer_maker_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder bid_size_b(pool);
        DoubleBuilder ask_size_b(pool);

        DoubleBuilder trade_to_mid_b(pool);
        DoubleBuilder trade_to_microprice_b(pool);
        DoubleBuilder price_to_best_bid_b(pool);
        DoubleBuilder price_to_best_ask_b(pool);

        StringBuilder trade_side_b(pool);
        Int32Builder trade_sign_b(pool);

        DoubleBuilder notional_b(pool);
        DoubleBuilder log_notional_b(pool);
        DoubleBuilder intensity_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: trades){
            ts_b.Append(r.ts);
            symbol_b.Append(r.symbol);

            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);
            side_b.Append(r.side);
            is_buyer_maker_b.Append(r.is_buyer_maker);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);

            bid_size_b.Append(r.bid_size);
            ask_size_b.Append(r.ask_size);

            trade_to_mid_b.Append(r.trade_to_mid);
            trade_to_microprice_b.Append(r.trade_to_microprice);
            price_to_best_bid_b.Append(r.price_to_best_bid);
            price_to_best_ask_b.Append(r.price_to_best_ask);

            trade_side_b.Append(r.trade_side);
            trade_sign_b.Append(r.trade_sign);

            notional_b.Append(r.notional);
            log_notional_b.Append(r.log_notional);
            intensity_b.Append(r.intensity);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, symbol_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;
        shared_ptr<Array> side_arr, is_buyer_maker_arr;

        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        shared_ptr<Array> bid_size_arr, ask_size_arr;

        shared_ptr<Array> trade_to_mid_arr, trade_to_microprice_arr;
        shared_ptr<Array> price_to_best_bid_arr, price_to_best_ask_arr;

        shared_ptr<Array> trade_side_arr, trade_sign_arr;
        shared_ptr<Array> notional_arr, log_notional_arr, intensity_arr;

        ts_b.Finish(&ts_arr);
        symbol_b.Finish(&symbol_arr);

        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);
        side_b.Finish(&side_arr);
        is_buyer_maker_b.Finish(&is_buyer_maker_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        bid_size_b.Finish(&bid_size_arr);
        ask_size_b.Finish(&ask_size_arr);

        trade_to_mid_b.Finish(&trade_to_mid_arr);
        trade_to_microprice_b.Finish(&trade_to_microprice_arr);
        price_to_best_bid_b.Finish(&price_to_best_bid_arr);
        price_to_best_ask_b.Finish(&price_to_best_ask_arr);

        trade_side_b.Finish(&trade_side_arr);
        trade_sign_b.Finish(&trade_sign_arr);

        notional_b.Finish(&notional_arr);
        log_notional_b.Finish(&log_notional_arr);
        intensity_b.Finish(&intensity_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("symbol", utf8()),

            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
            field("side", utf8()),
            field("is_buyer_maker", boolean()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("bid_size", float64()),
            field("ask_size", float64()),

            field("trade_to_mid", float64()),
            field("trade_to_microprice", float64()),
            field("price_to_best_bid", float64()),
            field("price_to_best_ask", float64()),

            field("trade_side", utf8()),
            field("trade_sign", int32()),

            field("notional", float64()),
            field("log_notional", float64()),
            field("intensity", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, symbol_arr,
            price_arr, price_tick_arr, qty_arr,
            side_arr, is_buyer_maker_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            bid_size_arr, ask_size_arr,
            trade_to_mid_arr, trade_to_microprice_arr,
            price_to_best_bid_arr, price_to_best_ask_arr,
            trade_side_arr, trade_sign_arr,
            notional_arr, log_notional_arr, intensity_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(trades_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_quote(const Order& order, const std_string& side, const std_string& event_type){
        QuoteRow row;

        double price = config.from_tick(order.price_tick);

        row.ts = order.ts;
        row.exchange_latency = order.exchange_latency;
        row.symbol = config.instrument_upper;
        row.client_oid = order.client_oid;
        row.side = side;
        row.event_type = event_type;

        row.price = price;
        row.price_tick = order.price_tick;
        row.qty = order.qty;

        row.mid = order.signal.mid;
        row.microprice = order.signal.microprice;
        row.microprice_dev = order.signal.microprice_dev;
        row.microprice_error = order.signal.microprice_error;
        row.spread = order.signal.spread;
        row.best_bid = order.signal.best_bid;
        row.best_ask = order.signal.best_ask;
        row.best_bid_tick = config.to_tick(order.signal.best_bid);
        row.best_ask_tick = config.to_tick(order.signal.best_ask);
        
        row.order_imbalance = order.signal.order_imbalance;
        row.trade_imbalance = order.signal.trade_imbalance;
        row.volatility = order.signal.volatility;
        row.queue_ahead_bid = order.signal.queue_ahead_bid;
        row.queue_ahead_ask = order.signal.queue_ahead_ask;

        row.distance_to_mid = price - order.signal.mid;
        row.distance_to_touch = (order.side == "BUY") ? price - order.signal.best_bid : price - order.signal.best_ask;
        row.inventory = order.signal.inventory;

        row.fair = order.signal.fair;
        row.skew = order.signal.skew;
        row.struct_delta = order.signal.struct_delta;
        row.micro_signal_delta = order.signal.micro_signal_delta;
        row.residual_delta = order.signal.residual_delta;
        row.reservation = order.signal.reservation;
        
        row.regime = order.signal.regime;
        row.regime_id = order.signal.regime_id;
        row.regime_prob = order.signal.regime_prob;
        row.alpha_order_imb = order.signal.alpha_order_imb;
        row.alpha_trade_imb = order.signal.alpha_trade_imb;
        row.alpha_struct = order.signal.alpha_struct;
        row.k0 = order.signal.k0;
        row.spread_multiplier = order.signal.spread_multiplier;
        row.inventory_target = order.signal.inventory_target;
        row.residual_signal_quality = order.signal.residual_signal_quality;
        row.tox = order.signal.toxicity.tox;
        row.k1 = order.signal.toxicity.k1;
        row.k2 = order.signal.toxicity.k2;
        
        quotes.push_back(move(row));

        if(quotes.size() >= QUOTE_CHUNK){
            quotes_path = build_file_path("quotes", quotes_id++);
            export_quote_parquet(quotes);
            quotes.clear();
        }
    }

    void export_quote_parquet(const vector<QuoteRow>& quotes){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder exchange_latency_b(pool);
        StringBuilder symbol_b(pool);
        StringBuilder client_oid_b(pool);
        StringBuilder side_b(pool);
        StringBuilder event_type_b(pool);

        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);

        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);
        
        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);

        DoubleBuilder distance_to_mid_b(pool);
        DoubleBuilder distance_to_touch_b(pool);
        DoubleBuilder inventory_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: quotes){
            ts_b.Append(r.ts);
            exchange_latency_b.Append(r.exchange_latency);
            symbol_b.Append(r.symbol);
            client_oid_b.Append(r.client_oid);
            side_b.Append(r.side);
            event_type_b.Append(r.event_type);

            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);
            
            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);

            distance_to_mid_b.Append(r.distance_to_mid);
            distance_to_touch_b.Append(r.distance_to_touch);
            inventory_b.Append(r.inventory);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, exchange_latency_arr, symbol_arr, client_oid_arr;
        shared_ptr<Array> side_arr, event_type_arr;

        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;

        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;

        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;

        shared_ptr<Array> distance_to_mid_arr, distance_to_touch_arr, inventory_arr;

        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr;
        shared_ptr<Array> micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k1_arr, k2_arr;

        ts_b.Finish(&ts_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        symbol_b.Finish(&symbol_arr);
        client_oid_b.Finish(&client_oid_arr);
        side_b.Finish(&side_arr);
        event_type_b.Finish(&event_type_arr);

        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);
        
        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);

        distance_to_mid_b.Finish(&distance_to_mid_arr);
        distance_to_touch_b.Finish(&distance_to_touch_arr);
        inventory_b.Finish(&inventory_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);

        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("exchange_latency", int64()),
            field("symbol", utf8()),
            field("client_oid", utf8()),
            field("side", utf8()),
            field("event_type", utf8()),

            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),

            field("distance_to_mid", float64()),
            field("distance_to_touch", float64()),
            field("inventory", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),

            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, exchange_latency_arr, symbol_arr, client_oid_arr,
            side_arr, event_type_arr,
            price_arr, price_tick_arr, qty_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            distance_to_mid_arr, distance_to_touch_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr,
            micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k1_arr, k2_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(quotes_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_fill(const Order& order, const double& fill_qty, const int64_t& fill_ts, bool is_maker){
        FillRow row;

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9);

        double volatility_at_fill = state.get_vol();

        row.ts = state.last_trade_ts;
        row.exchange_latency = order.exchange_latency;
        row.time_to_fill = fill_ts - order.live_ts;
        row.symbol = config.instrument_upper;
        row.side = order.side;
        row.price = config.from_tick(order.price_tick);
        row.price_tick = order.price_tick;
        row.qty = fill_qty;
        row.cum_qty = order.qty - order.remaining;

        row.is_maker = is_maker;
        row.fill_sign = (order.side == "BUY") ? 1 : -1;
        row.fill_type = (order.side == "BUY") ? "BID_HIT" : "ASK_LIFT";
        row.fill_status = (order.remaining > 0.0) ? "PARTIAL" : "FULL";
        row.queue_ahead_at_join = order.queue_ahead_at_join;

        row.mid_at_fill = mid;
        row.microprice_at_fill = microprice;
        row.microprice_dev_at_fill = microprice - mid;
        row.microprice_error_at_fill = mid - microprice;
        row.spread_at_fill = best_ask - best_bid;
        row.best_bid_at_fill = best_bid;
        row.best_ask_at_fill = best_ask;
        row.volatility_at_fill = volatility_at_fill;
        row.volatility_at_fill_bps = volatility_at_fill * 10000.0;

        row.mid = order.signal.mid;
        row.microprice = order.signal.microprice;
        row.microprice_dev = order.signal.microprice_dev;
        row.microprice_error = order.signal.microprice_error;
        row.spread = order.signal.spread;
        row.best_bid = order.signal.best_bid;
        row.best_ask = order.signal.best_ask;
        row.best_bid_tick = config.to_tick(order.signal.best_bid);
        row.best_ask_tick = config.to_tick(order.signal.best_ask);

        row.order_imbalance = order.signal.order_imbalance;
        row.trade_imbalance = order.signal.trade_imbalance;
        row.volatility = order.signal.volatility;
        row.volatility_bps = order.signal.volatility * 10000.0;
        row.queue_ahead_bid = order.signal.queue_ahead_bid;
        row.queue_ahead_ask = order.signal.queue_ahead_ask;
        row.inventory = state.inventory;

        row.fair = order.signal.fair;
        row.skew = order.signal.skew;
        row.struct_delta = order.signal.struct_delta;
        row.micro_signal_delta = order.signal.micro_signal_delta;
        row.residual_delta = order.signal.residual_delta;
        row.reservation = order.signal.reservation;

        row.regime = order.signal.regime;
        row.regime_id = order.signal.regime_id;
        row.regime_prob = order.signal.regime_prob;
        row.alpha_order_imb = order.signal.alpha_order_imb;
        row.alpha_trade_imb = order.signal.alpha_trade_imb;
        row.alpha_struct = order.signal.alpha_struct;
        row.k0 = order.signal.k0;
        row.spread_multiplier = order.signal.spread_multiplier;
        row.inventory_target = order.signal.inventory_target;
        row.residual_signal_quality = order.signal.residual_signal_quality;
        row.tox = order.signal.toxicity.tox;
        row.k1 = order.signal.toxicity.k1;
        row.k2 = order.signal.toxicity.k2;

        row.my_bid = order.signal.my_bid;
        row.my_ask = order.signal.my_ask;
        row.my_bid_tick = config.to_tick(order.signal.my_bid);
        row.my_ask_tick = config.to_tick(order.signal.my_ask);
        
        row.bid_distance_touch = order.signal.my_bid - best_bid;
        row.ask_distance_touch = order.signal.my_ask - best_ask;
        row.bid_distance_spread = order.signal.my_bid - best_ask;
        row.ask_distance_spread = order.signal.my_ask - best_bid;

        fills.push_back(move(row));

        if(fills.size() >= FILL_CHUNK){
            fills_path = build_file_path("fills", fills_id++);
            export_fill_parquet(fills);
            fills.clear();
        }
    }

    void export_fill_parquet(const vector<FillRow>& fills){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder exchange_latency_b(pool);
        Int64Builder time_to_fill_b(pool);
        StringBuilder symbol_b(pool);
        StringBuilder side_b(pool);
        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);
        DoubleBuilder cum_qty_b(pool);

        BooleanBuilder is_maker_b(pool);
        Int32Builder fill_sign_b(pool);
        StringBuilder fill_type_b(pool);
        StringBuilder fill_status_b(pool);
        DoubleBuilder queue_ahead_at_join_b(pool);

        DoubleBuilder mid_at_fill_b(pool);
        DoubleBuilder microprice_at_fill_b(pool);
        DoubleBuilder microprice_dev_at_fill_b(pool);
        DoubleBuilder microprice_error_at_fill_b(pool);
        DoubleBuilder spread_at_fill_b(pool);
        DoubleBuilder best_bid_at_fill_b(pool);
        DoubleBuilder best_ask_at_fill_b(pool);
        DoubleBuilder volatility_at_fill_b(pool);
        DoubleBuilder volatility_at_fill_bps_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder volatility_bps_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);
        DoubleBuilder inventory_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

        DoubleBuilder my_bid_b(pool);
        DoubleBuilder my_ask_b(pool);
        Int64Builder my_bid_tick_b(pool);
        Int64Builder my_ask_tick_b(pool);

        DoubleBuilder bid_dist_touch_b(pool);
        DoubleBuilder ask_dist_touch_b(pool);
        DoubleBuilder bid_dist_spread_b(pool);
        DoubleBuilder ask_dist_spread_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: fills){
            ts_b.Append(r.ts);
            exchange_latency_b.Append(r.exchange_latency);
            time_to_fill_b.Append(r.time_to_fill);
            symbol_b.Append(r.symbol);
            side_b.Append(r.side);
            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);
            cum_qty_b.Append(r.cum_qty);

            is_maker_b.Append(r.is_maker);
            fill_sign_b.Append(r.fill_sign);
            fill_type_b.Append(r.fill_type);
            fill_status_b.Append(r.fill_status);
            queue_ahead_at_join_b.Append(r.queue_ahead_at_join);

            mid_at_fill_b.Append(r.mid_at_fill);
            microprice_at_fill_b.Append(r.microprice_at_fill);
            microprice_dev_at_fill_b.Append(r.microprice_dev_at_fill);
            microprice_error_at_fill_b.Append(r.microprice_error_at_fill);
            spread_at_fill_b.Append(r.spread_at_fill);
            best_bid_at_fill_b.Append(r.best_bid_at_fill);
            best_ask_at_fill_b.Append(r.best_ask_at_fill);
            volatility_at_fill_b.Append(r.volatility_at_fill);
            volatility_at_fill_bps_b.Append(r.volatility_at_fill_bps);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);
            
            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            volatility_bps_b.Append(r.volatility_bps);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);
            inventory_b.Append(r.inventory);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);

            my_bid_b.Append(r.my_bid);
            my_ask_b.Append(r.my_ask);
            my_bid_tick_b.Append(r.my_bid_tick);
            my_ask_tick_b.Append(r.my_ask_tick);

            bid_dist_touch_b.Append(r.bid_distance_touch);
            ask_dist_touch_b.Append(r.ask_distance_touch);
            bid_dist_spread_b.Append(r.bid_distance_spread);
            ask_dist_spread_b.Append(r.ask_distance_spread);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, exchange_latency_arr, time_to_fill_arr, symbol_arr, side_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr, cum_qty_arr;
        shared_ptr<Array> is_maker_arr, fill_sign_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr;
        
        shared_ptr<Array> mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr;
        shared_ptr<Array> spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr;
        shared_ptr<Array> volatility_at_fill_arr, volatility_at_fill_bps_arr;

        
        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr;
        shared_ptr<Array> spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr;
        
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k1_arr, k2_arr;

        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_dist_touch_arr, ask_dist_touch_arr;
        shared_ptr<Array> bid_dist_spread_arr, ask_dist_spread_arr;

        ts_b.Finish(&ts_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        time_to_fill_b.Finish(&time_to_fill_arr);
        symbol_b.Finish(&symbol_arr);
        side_b.Finish(&side_arr);
        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);
        cum_qty_b.Finish(&cum_qty_arr);

        is_maker_b.Finish(&is_maker_arr);
        fill_sign_b.Finish(&fill_sign_arr);
        fill_type_b.Finish(&fill_type_arr);
        fill_status_b.Finish(&fill_status_arr);
        queue_ahead_at_join_b.Finish(&queue_ahead_at_join_arr);

        mid_at_fill_b.Finish(&mid_at_fill_arr);
        microprice_at_fill_b.Finish(&microprice_at_fill_arr);
        microprice_dev_at_fill_b.Finish(&microprice_dev_at_fill_arr);
        microprice_error_at_fill_b.Finish(&microprice_error_at_fill_arr);
        spread_at_fill_b.Finish(&spread_at_fill_arr);
        best_bid_at_fill_b.Finish(&best_bid_at_fill_arr);
        best_ask_at_fill_b.Finish(&best_ask_at_fill_arr);
        volatility_at_fill_b.Finish(&volatility_at_fill_arr);
        volatility_at_fill_bps_b.Finish(&volatility_at_fill_bps_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        volatility_bps_b.Finish(&volatility_bps_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);
        inventory_b.Finish(&inventory_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

        my_bid_b.Finish(&my_bid_arr);
        my_ask_b.Finish(&my_ask_arr);
        my_bid_tick_b.Finish(&my_bid_tick_arr);
        my_ask_tick_b.Finish(&my_ask_tick_arr);

        bid_dist_touch_b.Finish(&bid_dist_touch_arr);
        ask_dist_touch_b.Finish(&ask_dist_touch_arr);
        bid_dist_spread_b.Finish(&bid_dist_spread_arr);
        ask_dist_spread_b.Finish(&ask_dist_spread_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("exchange_latency", int64()),
            field("time_to_fill", int64()),
            field("symbol", utf8()),
            field("side", utf8()),
            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
            field("cum_qty", float64()),
            field("is_maker", boolean()),
            field("fill_sign", int32()),
            field("fill_type", utf8()),
            field("fill_status", utf8()),
            field("queue_ahead_at_join", float64()),

            field("mid_at_fill", float64()),
            field("microprice_at_fill", float64()),
            field("microprice_dev_at_fill", float64()),
            field("microprice_error_at_fill", float64()),
            field("spread_at_fill", float64()),
            field("best_bid_at_fill", float64()),
            field("best_ask_at_fill", float64()),
            field("volatility_at_fill", float64()),
            field("volatility_at_fill_bps", float64()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("volatility_bps", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),
            field("inventory", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64()),
            
            field("my_bid", float64()),
            field("my_ask", float64()),
            field("my_bid_tick", int64()),
            field("my_ask_tick", int64()),

            field("bid_distance_touch", float64()),
            field("ask_distance_touch", float64()),
            field("bid_distance_spread", float64()),
            field("ask_distance_spread", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, exchange_latency_arr, time_to_fill_arr, symbol_arr, side_arr,
            price_arr, price_tick_arr, qty_arr, cum_qty_arr,
            is_maker_arr, fill_sign_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr,
            mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr,
            spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr,
            volatility_at_fill_arr, volatility_at_fill_bps_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr,
            spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k1_arr, k2_arr,
            my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr,
            bid_dist_touch_arr, ask_dist_touch_arr,
            bid_dist_spread_arr, ask_dist_spread_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(fills_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }
};

class Feed {
public:
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual ~Feed() = default;
};

class BinanceSpotFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event;
    function<void(const json&)> export_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    vector<Depth> depth_buffer;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    BinanceSpotFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event,
                    function<void(const json&)> export_orderbook_snapshot):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    export_orderbook_snapshot(move(export_orderbook_snapshot)) {}

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake(config.hostname, "/ws/" + config.instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "TRADE WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = int64_t(doc["T"]);
            trade.local_ts = clock.now_ms();
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.local_ts, trade.ts);

            log_event("trade", trade.ts, trade.local_ts, trade.latency, msg);

            ExecutionEvent ev;
            ev.type = ExecutionEventType::TRADE_UPDATE;
            ev.trade = trade;
            execution_event.push(ev);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake(config.hostname, "/ws/" + config.instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "DEPTH WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = int64_t(doc["E"]);
            depth.local_ts = clock.now_ms();
            depth.U = int64_t(doc["U"]);
            depth.u = int64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.local_ts, depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", depth.ts, depth.local_ts, depth.latency, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(depth);
                    continue;
                }
            }
            
            ExecutionEvent ev;
            ev.type = ExecutionEventType::DEPTH_UPDATE_SPOT;
            ev.depth = depth;
            execution_event.push(ev);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceSpotFeed::trade_loop, this);
        depth_thread = thread(&BinanceSpotFeed::depth_loop, this);

        cout << "LIVE SPOT SOCKETS STARTED\n";

        // wait for first message
        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{ return first_depth_received.load(); });
        }

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance();

        export_orderbook_snapshot(snapshot);

        // -------------------------
        // WAIT FOR STREAM ALIGNMENT (YOUR GATE FIX)
        // -------------------------
        bool valid = false;

        for(int i = 0; i < 500; i++){
            {
                lock_guard<mutex> lock(buffer_mtx);

                if(!depth_buffer.empty() && depth_buffer.back().u > snapshot_id){
                    valid = true;
                    break;
                }
            }
            this_thread::sleep_for(milliseconds(10));
        }

        if(!valid) throw runtime_error("Stream not aligned (no post-snapshot events)");
        
        vector<Depth> buffered;
        {
            lock_guard lock(buffer_mtx);
            buffered = depth_buffer;   // copy, don't swap
        }
        
        sort(buffered.begin(), buffered.end(), [](const auto& a, const auto& b) {return a.U < b.U;});

        auto it = find_if(buffered.begin(), buffered.end(), [&](const Depth& d){
            return d.U <= snapshot_id + 1 && snapshot_id + 1 <= d.u;});

        if(it == buffered.end()) throw runtime_error("Couldn't synchronize order book");
        
        // -------------------------
        // APPLY REPLAY
        // -------------------------
        for(; it != buffered.end(); ++it){
            cout << "BUFFER U: " << it->U << " snapshot_id + 1: " << snapshot_id + 1 << " u: " << it->u << "\n";

            state.market_book.apply_delta(*it);
            state.market_book.last_update_id = it->u;
            state.update_vol();
        }

        cout << "BOOK SYNCHRONIZED\n";
        
        // -------------------------
        // LIVE MODE
        // -------------------------
        {
            lock_guard<mutex> lock(buffer_mtx);
            buffering = false;
        }
        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING BINANCE FEED\n";

        running = false;

        if(trade_thread.joinable()) trade_thread.join();
        if(depth_thread.joinable()) depth_thread.join();

        cout << "BINANCE FEED STOPPED\n";
    }
};

class BinanceFuturesFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event;
    function<void(const json&)> export_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    vector<Depth> depth_buffer;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    BinanceFuturesFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event,
                    function<void(const json&)> export_orderbook_snapshot):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    export_orderbook_snapshot(move(export_orderbook_snapshot)) {}

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake(config.hostname, "/ws/" + config.instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "TRADE WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = int64_t(doc["T"]);
            trade.local_ts = clock.now_ms();
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.local_ts, trade.ts);

            log_event("trade", trade.ts, clock.now_ms(), trade.latency, msg);

            ExecutionEvent ev;
            ev.type = ExecutionEventType::TRADE_UPDATE;
            ev.trade = trade;
            execution_event.push(ev);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake(config.hostname, "/ws/" + config.instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "DEPTH WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = int64_t(doc["E"]);
            depth.local_ts = clock.now_ms();
            depth.pu = int64_t(doc["pu"]);
            depth.U = int64_t(doc["U"]);
            depth.u = int64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.local_ts, depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", depth.ts, clock.now_ms(), depth.latency, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(depth);
                    continue;
                }
            }

            ExecutionEvent ev;
            ev.type = ExecutionEventType::DEPTH_UPDATE_FUTURES;
            ev.depth = depth;
            execution_event.push(ev);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceFuturesFeed::trade_loop, this);
        depth_thread = thread(&BinanceFuturesFeed::depth_loop, this);

        cout << "LIVE FUTURES SOCKETS STARTED\n";

        // wait for first message
        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{ return first_depth_received.load(); });
        }

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance();

        export_orderbook_snapshot(snapshot);

        // -------------------------
        // WAIT FOR STREAM ALIGNMENT (YOUR GATE FIX)
        // -------------------------
        bool valid = false;

        for(int i = 0; i < 500; i++){
            {
                lock_guard<mutex> lock(buffer_mtx);
                if(!depth_buffer.empty() && depth_buffer.back().u > snapshot_id){
                    valid = true;
                    break;
                }
            }
            this_thread::sleep_for(milliseconds(10));
        }

        if(!valid) throw runtime_error("Stream not aligned (no post-snapshot events)");

        vector<Depth> buffered;
        {
            lock_guard lock(buffer_mtx);
            buffered = depth_buffer;   // copy, don't swap
        }
        
        sort(buffered.begin(), buffered.end(), [](const auto& a, const auto& b) {return a.U < b.U;});

        auto it = find_if(buffered.begin(), buffered.end(), [&](const Depth& d){
            return d.U <= snapshot_id && snapshot_id <= d.u;});

        if(it == buffered.end()) throw runtime_error("Couldn't synchronize order book");

        // -------------------------
        // APPLY REPLAY
        // -------------------------
        for(; it != buffered.end(); ++it){
            cout << "BUFFER pu: " << it->pu << " U: " << it->U << " snapshot_id: "<< snapshot_id << " u: " << it->u << '\n';

            state.market_book.apply_delta(*it);
            state.market_book.last_update_id = it->u;
            state.update_vol();
        }

        cout << "BOOK SYNCHRONIZED\n";
        
        // -------------------------
        // LIVE MODE
        // -------------------------
        {
            lock_guard<mutex> lock(buffer_mtx);
            buffering = false;
        }
        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    // =========================
    // STOP
    // =========================
    void stop() override {
        cout << "STOPPING BINANCE FEED\n";

        running = false;

        if(trade_thread.joinable()) trade_thread.join();
        if(depth_thread.joinable()) depth_thread.join();

        cout << "BINANCE FEED STOPPED\n";
    }
};

class BinanceSpotReplayFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    vector<EventRow> events;
    json orderbook_snapshot;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event;
    function<void(const json&)> export_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;

    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0;
    optional<int64_t> last_ts;
    double speed_multiplier = 1.0;

    simdjson::ondemand::parser parser;
    
    BinanceSpotReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event,
                    function<void(const json&)> export_orderbook_snapshot):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    export_orderbook_snapshot(move(export_orderbook_snapshot)) {initialize();}

    void initialize(){
        std_string orderbook_snapshot_path = config.folder_path + "/orderbook_snapshot.json";
        std_string events_path = config.folder_path + "/events.parquet";

        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        cout << "events_path: " << events_path << "\n";

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto local_ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("local_ts")->chunk(0)
        );

        auto latency_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("latency")->chunk(0)
        );

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventRow e;
            e.type = type_col->GetString(i);
            e.ts = ts_col->Value(i);
            e.local_ts = local_ts_col->Value(i);
            e.latency = latency_col->Value(i);
            e.msg = msg_col->GetString(i);
            events.push_back(e);
        }

        // stable_sort(events.begin(), events.end(), [](const auto& a, const auto& b) {return a.ts < b.ts;});

        cout << "Events: " << n << "\n";
    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void on_trade_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = int64_t(doc["T"]);
        trade.local_ts = e.local_ts;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = e.latency;
        
        log_event("trade", trade.ts, trade.local_ts, trade.latency, e.msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = int64_t(doc["E"]);
        depth.local_ts = e.local_ts;
        depth.U  = int64_t(doc["U"]);
        depth.u  = int64_t(doc["u"]);
        depth.latency = e.latency;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", depth.ts, depth.local_ts, depth.latency, e.msg);
        
        if(!snapshot_aligned.load()){
            if(depth.U <= state.market_book.last_update_id + 1 && state.market_book.last_update_id + 1 <= depth.u){
                cout << "BUFFER U: " << depth.U << " snapshot_id + 1: " << state.market_book.last_update_id + 1 << " u: " << depth.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(depth);
                state.market_book.last_update_id = depth.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        ExecutionEvent ev;
        ev.type = ExecutionEventType::DEPTH_UPDATE_SPOT;
        ev.depth = depth;
        execution_event.push(ev);
    }

    void run(){
        while(running && i < events.size()){
            EventRow& e = events[i];

            if(last_ts.has_value()){
                double dt = (e.local_ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = e.local_ts;

            if(e.type == "trade") on_trade_message(e);
            else if(e.type == "depth") on_depth_message(e);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY SPOT SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        export_orderbook_snapshot(snapshot);

        replay_thread = thread(&BinanceSpotReplayFeed::run, this);

        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{return first_depth_received.load(); });
        }

        while(!snapshot_aligned.load()) this_thread::sleep_for(milliseconds(1));

        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING REPLAY\n";
        running = false;

        if(replay_thread.joinable()) replay_thread.join();

        cout << "REPLAY STOPPED\n";
    }
};

class BinanceFuturesReplayFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    vector<EventRow> events;
    json orderbook_snapshot;

    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event;
    function<void(const json&)> export_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;
    
    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0; //provides wrap around loop
    optional<int64_t> last_ts;
    double speed_multiplier = 1.0;

    simdjson::ondemand::parser parser;
    
    BinanceFuturesReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const int64_t&, const int64_t&, const int64_t&, const std_string&)> log_event,
                    function<void(const json&)> export_orderbook_snapshot):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    export_orderbook_snapshot(move(export_orderbook_snapshot)) {initialize();}

    void initialize(){
        std_string orderbook_snapshot_path = config.folder_path + "/orderbook_snapshot.json";
        std_string events_path = config.folder_path + "/events.parquet";

        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        cout << "events_path: " << events_path << "\n";

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto local_ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("local_ts")->chunk(0)
        );

        auto latency_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("latency")->chunk(0)
        );

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventRow e;
            e.type = type_col->GetString(i);
            e.ts = ts_col->Value(i);
            e.local_ts = local_ts_col->Value(i);
            e.latency = latency_col->Value(i);
            e.msg = msg_col->GetString(i);
            events.push_back(e);
        }

        // stable_sort(events.begin(), events.end(), [](const auto& a, const auto& b) {return a.ts < b.ts;}); //assuming not sorted yet

        cout << "Events: " << n << "\n";
    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void on_trade_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = int64_t(doc["T"]);
        trade.local_ts    = e.local_ts;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = e.latency;

        log_event("trade", trade.ts, trade.local_ts, trade.latency, e.msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = int64_t(doc["E"]);
        depth.local_ts = e.local_ts;
        depth.pu = int64_t(doc["pu"]);
        depth.U  = int64_t(doc["U"]);
        depth.u  = int64_t(doc["u"]);
        depth.latency = e.latency;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", depth.ts, depth.local_ts, depth.latency, e.msg);
        
        if(!snapshot_aligned.load()){
            if(depth.U <= state.market_book.last_update_id && state.market_book.last_update_id <= depth.u){
                cout << "BUFFER pu: " << depth.pu << " U: " << depth.U << " snapshot_id: " << state.market_book.last_update_id << " u: " << depth.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(depth);
                state.market_book.last_update_id = depth.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        ExecutionEvent ev;
        ev.type = ExecutionEventType::DEPTH_UPDATE_FUTURES;
        ev.depth = depth;
        execution_event.push(ev);
    }

    void run(){
        while(running && i < events.size()){
            EventRow& e = events[i];

            if(last_ts.has_value()){
                double dt = (e.local_ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = e.local_ts;

            if(e.type == "depth") on_depth_message(e);
            else if(e.type == "trade") on_trade_message(e);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY FUTURES SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        export_orderbook_snapshot(snapshot);

        replay_thread = thread(&BinanceFuturesReplayFeed::run, this);

        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{return first_depth_received.load(); });
        }

        while(!snapshot_aligned.load()) this_thread::sleep_for(milliseconds(1));

        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING REPLAY\n";
        running = false;

        if(replay_thread.joinable()) replay_thread.join();

        cout << "REPLAY STOPPED\n";
    }
};

class Execution {
public:
    virtual double get_last_bid() = 0;
    virtual double get_last_ask() = 0;
    virtual double get_current_bid_size() = 0;
    virtual double get_current_ask_size() = 0;
    virtual void place_quotes_latency(const Signal&) = 0; //paper
    virtual bool process_latency_queue() = 0; //paper
    virtual void process_trade(const Trade&) = 0;
    virtual Order* get_open_order(const std_string&) = 0;
    virtual void cancel_all_orders() = 0;
    virtual void place_quotes(const Signal&) = 0;
    virtual void place_market() = 0;
    virtual void apply_stream_update(const Stream&) = 0;
    virtual ~Execution() = default;
};

class PaperExecution : public Execution {
public:
    MarketConfig& config;
    State& state;
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    mt19937 rng;
    uniform_real_distribution<double> dist;

    priority_queue<LatencyEvent, vector<LatencyEvent>, Compare> latency_queue;

    PaperExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, BinanceClock& clock)
        : config(config), state(state), recorder(recorder), clock(clock)
    {
        state.exchange_latency = config.exchange_latency;
        
        rng.seed(random_device{}());
        dist = uniform_real_distribution<double>(0.0, 1.0);
    }

    double get_last_bid() override {
        return last_bid;
    }

    double get_last_ask() override {
        return last_ask;
    }

    double get_current_bid_size() override {
        return current_bid_size;
    }

    double get_current_ask_size() override {
        return current_ask_size;
    }

    // -------------------------
    // LATENCY SIMULATION
    // -------------------------
    void place_quotes_latency(const Signal& signal) override {

        LatencyEvent event;
        event.execute_ts = clock.now_ms() + config.exchange_latency;
        event.type = "PLACE_QUOTES";
        event.signal = signal;

        latency_queue.push(event);
    }

    bool process_latency_queue() override {

        bool changed = false;

        while(!latency_queue.empty()){
            auto event = latency_queue.top();
            if(clock.now_ms() < event.execute_ts) break;

            latency_queue.pop();
            place_quotes(event.signal);

            changed = true;
        }
        return changed;
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick &&
            (order.status == "LIVE" || order.status == "PARTIALLY_FILLED")) return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {

        update_trade_flow(trade);
        match_side(trade);
    }

    void update_trade_flow(const Trade& trade){

        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;
        
        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){
        double inv = state.inventory;
        double vol = state.get_vol();

        double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

        double inv_scale = 5.0;
        double inv_signal = tanh(inv / inv_scale);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        double risk_penalty = exp(-0.2 * inv * inv);

        double toxicity_penalty = exp(-signal.toxicity.k2 * signal.toxicity.tox); //negative markout translates into positive exp

        double base = config.base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        double bid_size = size * bid_multiplier;
        double ask_size = size * ask_multiplier;

        // inventory limits
        double max_buy = max(0.0, config.max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell = max(0.0, config.max_inv + inv);

        bid_size = min(bid_size, max_buy);
        ask_size = min(ask_size, max_sell);

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        return {bid_size, ask_size};
    }

    Order* get_open_order(const std_string& side) override {     
        for(auto& [client_oid, order]: open_orders) if(order.side == side) return &order;
        return nullptr;
    }

    std_string uuid16(){
        auto u = boost::uuids::random_generator()();
        std_string s = boost::uuids::to_string(u);

        s.erase(remove(s.begin(), s.end(), '-'), s.end());
        return s.substr(0, 16);
    }

    void place_limit(const std_string& side, const double& price, const double& size, const Signal& signal){

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = config.to_tick(price);
        int64_t ts = clock.now_ms();

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = size;
        order.remaining = size;
        order.status = "LIVE";
        order.ts = ts;
        order.live_ts = ts;
        order.exchange_latency = state.exchange_latency;
        order.owner = "self";
        order.signal = signal;
        order.queue_ahead_at_join = state.set_queue_position(side, price_tick);
        // order.queue_ahead = order.queue_ahead_at_join;

        open_orders[client_oid] = order;   // <-- insert into map

        recorder.log_quote(order, (side == "BUY") ? "BID" : "ASK", "NEW");
    }

    void place_market() override {

        double pos = state.inventory;
        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();
        
        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = (pos > 0) ? bid_tick : ask_tick;
        int64_t ts = clock.now_ms();
        std_string side = (pos > 0) ? "SELL" : "BUY";

        Order& order = open_orders[client_oid]; // <-- insert into map
        
        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = abs(pos);
        order.remaining = abs(pos);
        order.status = "LIVE";
        order.ts = ts;
        order.live_ts = ts;
        order.exchange_latency = state.exchange_latency;
        order.owner = "self";
        order.signal = *state.last_signal;
        order.queue_ahead_at_join = 0.0;
        // order.queue_ahead = 0.0;

        open_orders[client_oid] = order;   // <-- insert into map

        execute_market(&order);
    }

    void cancel_order(Order* order){

        order->ts = clock.now_ms();
        order->status = "CANCELED";
        order->exchange_latency = state.exchange_latency;

        state.reset_queue_position(order->side);

        state.last_order_update = *order;

        recorder.log_quote(*order, (order->side == "BUY") ? "BID" : "ASK", "CANCELED");

        open_orders.erase(order->client_oid);
    }

    void cancel_all_orders() override {
        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        if(bid_order && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")) cancel_order(bid_order);

        if(ask_order && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")) cancel_order(ask_order);
    }

    void place_quotes(const Signal& signal) override {

        double desired_bid = signal.my_bid;
        double desired_ask = signal.my_ask;

        auto [bid_size, ask_size] = compute_order_size(signal);
        double tick = config.tick_size;

        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        current_bid_size = bid_size;
        current_ask_size = ask_size;

        // -------------------------
        // BID ORDERS
        // -------------------------
        if(!bid_order && bid_size > 0.0){ // if no bid order
            if(bid_size > 0.0){
                place_limit("BUY", desired_bid, bid_size, signal);
                last_bid = desired_bid;
            }
        }
        
        // if bid change
        else if(abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")){
            cancel_order(bid_order);

            if(bid_size > 0.0){
                place_limit("BUY", desired_bid, bid_size, signal);
                last_bid = desired_bid;
            }
        }

        // -------------------------
        // ASK ORDERS
        // -------------------------
        if(!ask_order && ask_size > 0.0){ // if no ask order
            if(ask_size > 0.0){
                place_limit("SELL", desired_ask, ask_size, signal);
                last_ask = desired_ask;
            }
        }

        // else if ask change
        else if(abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")){
            cancel_order(ask_order);

            if(ask_size > 0.0){
                place_limit("SELL", desired_ask, ask_size, signal);
                last_ask = desired_ask;
            }
        }
    }

    void match_side(const Trade& trade){

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);

        // state.hawkes.update(depletion, trade.ts); // hawkes process

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;

        double fill_qty = 0.0;

        auto& queue_ahead = (side == "BUY") ? state.bid_queue_ahead : state.ask_queue_ahead;

        cout << "match side order queue ahead before: " << queue_ahead.second << "\n";

        if(queue_ahead.second > 0.0){
            double removed_qty = min(queue_ahead.second, trade.qty);

            //remove matched trade qty from queue_ahead
            queue_ahead.second = max(0.0, queue_ahead.second - removed_qty);

            //get the remaining trade qty after using it for updating queue_ahead
            double remaining_trade_qty = trade.qty - removed_qty;

            //--------------------------------------------------
            // Trade reaches us
            //--------------------------------------------------
            if(remaining_trade_qty > 0.0){ //if there is still excess trade_qty after using it for updating queue_ahead
                fill_qty = min(order->remaining, remaining_trade_qty); // update fills
            }
        }
        
        //--------------------------------------------------
        // Already at front
        //--------------------------------------------------
        else{
            fill_qty = min(order->remaining, trade.qty); // update fills
        }

        cout << "fill_qty: " << fill_qty << "\n";

        if(fill_qty > 0.0){
            order->remaining = max(0.0, order->remaining - fill_qty);

            state.on_fill(trade.price, fill_qty, order->side, true);
            cout << "order->side: " << " side: " << side << "\n";

            if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
            else order->status = "FILLED";

            recorder.log_fill(*order, fill_qty, trade.ts + clock.offset_ms.load(), true);
        }

        cout << "match side order queue ahead after: " << queue_ahead.second << "\n";

        state.last_order_update = *order;

        if(order->status == "FILLED"){
            state.reset_queue_position(side); // reset queue position
            open_orders.erase(order->client_oid);
        }
    }

    void orderMarketToString(Order* order, const double& fill_qty){
        cout << format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), fill_qty, order->status) << "\n";
    }

    void execute_market(Order* order){

        auto& book = state.market_book;

        if(order->side == "BUY"){
            for(auto it = book.asks.begin(); it != book.asks.end() && order->remaining > 0;){
                
                order->price_tick = it->first; //set to current market order price

                double fill_qty = min(order->remaining, it->second);
                order->remaining = max(0.0, order->remaining - fill_qty);
                it->second = max(0.0, it->second - fill_qty);

                if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
                else order->status = "FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "BUY", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;
                orderMarketToString(order, fill_qty);

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0.0) it = book.asks.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
        else if(order->side == "SELL"){
            for(auto it = book.bids.begin(); it != book.bids.end() && order->remaining > 0;){

                order->price_tick = it->first;
                
                double fill_qty = min(order->remaining, it->second);
                order->remaining = max(0.0, order->remaining - fill_qty);
                it->second = max(0.0, it->second - fill_qty);
                
                if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
                else order->status = "FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "SELL", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;
                orderMarketToString(order, fill_qty);

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0.0) it = book.bids.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
    }

    void apply_stream_update(const Stream& stream) override {}
};

class HttpClient {
public:
    MarketConfig& config;

    asio::io_context ioc;
    ssl::context ctx;
    ssl_stream ssl_sock;

    mutex mtx;

    HttpClient(MarketConfig& config) : config(config), ctx(ssl::context::tlsv12_client), ssl_sock(ioc, ctx) {}

    void initialize(){
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.base_url, "443");

        asio::connect(ssl_sock.next_layer(), results);

        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        cout << "[HTTP] broker connected: " << config.base_url << "\n";
    }

    string request(http::verb method, const string& target,
                const vector<string>& headers = {}, const string& body = ""){

        lock_guard<mutex> lock(mtx);

        http::request<http::string_body> req{method, target, 11};

        req.set(http::field::host, config.base_url);
        req.set(http::field::user_agent, "mm-engine");

        for(const auto& h: headers){
            auto pos = h.find(":");

            if(pos != string::npos){
                std_string key = h.substr(0, pos);
                std_string value = h.substr(pos + 1);

                while(!value.empty() && value[0] == ' ')
                    value.erase(value.begin());

                req.set(key,value);
            }
        }

        if(!body.empty()){
            req.body() = body;
            req.prepare_payload();
        }

        beast::flat_buffer buffer;

        http::write(ssl_sock, req);
        http::response<http::string_body> res;
        http::read(ssl_sock, buffer, res);

        if(res.result_int() >= 400){
            cout << "ERROR: status >= 400: HTTP "
                + to_string(res.result_int()) + ": " + res.body();
        }

        cout << "\n===== HTTP RESPONSE =====\n";
        cout << res << endl;

        return res.body();
    }
};

class BinanceBroker {
public:
    MarketConfig& config;
    BinanceClock& clock;
    HttpClient http;

    std_string listen_key;
    atomic<bool> keepalive_running{false};
    thread keepalive_thread;

    BinanceBroker(MarketConfig& config, BinanceClock& clock)
        : config(config), clock(clock), http(config) {http.initialize();}

    // -------------------------
    // USER STREAM
    // -------------------------
    std_string open_user_stream(){
        std_string url = config.endpoint + "/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        auto j = json::parse(res);

        listen_key = j["listenKey"];

        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){
        std_string url = config.endpoint + "/listenKey?listenKey=" + listen_key;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        http.request(http::verb::put, url, headers);
    }

    void start_keepalive_loop(){
        keepalive_running = true;

        keepalive_thread = thread([this](){
            while(keepalive_running){
                this_thread::sleep_for(minutes(20));
                try{
                    keepalive_listen_key();
                    cout << "[keepalive sent]\n";
                }
                catch(...){
                    cout << "[keepalive error]\n";
                }
            }
        });
    }

    void stop_keepalive(){
        keepalive_running = false;

        if(keepalive_thread.joinable()) keepalive_thread.join();
    }

    // -------------------------
    // SIGNING
    // -------------------------
    std_string sign(const std_string& query){
        unsigned char* digest;
        digest = HMAC(EVP_sha256(), config.api_secret.c_str(), config.api_secret.size(),
                      (unsigned char*)query.c_str(), query.size(), NULL, NULL);

        char mdString[65];
        for(int i = 0; i < 32; i++)
            sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);

        return std_string(mdString);
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    double get_position(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/fapi/v2/positionRisk?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        for(auto& p: arr){
            if(p["symbol"] == config.instrument_upper) return stod(p["positionAmt"].get<std_string>());
        }
        return 0.0;
    }

    json place_limit(const Order& order, const double& price, const double& size){
        
        ostringstream q;        
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << size
          << "&price=" << fixed << setprecision(config.price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << order.qty
          << "&reduceOnly=true"
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};
        
        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json cancel_order(const Order& order){
        
        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::delete_, url, headers);
        return json::parse(res);
    }
};

class LiveExecution : public Execution {
public:
    MarketConfig& config;
    State& state;
    DatasetRecorder& recorder;
    BinanceBroker& broker;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    LiveExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, 
                BinanceBroker& broker, BinanceClock& clock)
        : config(config), state(state), recorder(recorder), broker(broker), clock(clock) {}

    double get_last_bid() override {
        return last_bid;
    }

    double get_last_ask() override {
        return last_ask;
    }

    double get_current_bid_size() override {
        return current_bid_size;
    }

    double get_current_ask_size() override {
        return current_ask_size;
    }

    void place_quotes_latency(const Signal& signal) override {}

    bool process_latency_queue() override {
        return true; // Live execution processes immediately through exchange events
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick &&
            (order.status == "LIVE" || order.status == "PARTIALLY_FILLED")) return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {
        
        update_trade_flow(trade);

        // state.hawkes.update(depletion, entry.ts); // hawkes process

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;
    }

    void update_trade_flow(const Trade& trade){
        
        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;

        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){
        double inv = state.inventory;
        double vol = state.get_vol();

        double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

        double inv_scale = 5.0;
        double inv_signal = tanh(inv / inv_scale);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        double risk_penalty = exp(-0.2 * inv * inv);

        double toxicity_penalty = exp(-signal.toxicity.k2 * signal.toxicity.tox); //negative markout translates into positive exp

        double base = config.base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        double bid_size = size * bid_multiplier;
        double ask_size = size * ask_multiplier;

        // inventory limits
        double max_buy = max(0.0, config.max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell = max(0.0, config.max_inv + inv);

        bid_size = min(bid_size, max_buy);
        ask_size = min(ask_size, max_sell);

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        return {bid_size, ask_size};
    }

    Order* get_open_order(const std_string& side) override {     
        for(auto& [client_oid, order]: open_orders) if(order.side == side) return &order;
        return nullptr;
    }

    std_string uuid16(){
        auto u = boost::uuids::random_generator()();
        std_string s = boost::uuids::to_string(u);

        s.erase(remove(s.begin(), s.end(), '-'), s.end());
        return s.substr(0, 16);
    }

    void place_limit(const std_string& side, const double& price, const double& size, const Signal& signal){

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = config.to_tick(price);

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = size;
        order.remaining = size;
        order.status = "PENDING_NEW";
        order.ts = clock.now_ms();
        order.owner = "self";
        order.signal = signal;

        cout << "- PLACE LIMIT ORDER - client_oid: " << order.client_oid << 
        ", status: " << order.status << ", timestamp: " << order.ts << "\n";

        json resp = broker.place_limit(order, price, size);
        order.resp = resp;

        if(resp.contains("code")){ // TO BE DELETED
            int code = resp["code"];

            if(code == -4003){
                cout << "Limit order rejected: Quantity less than or equal to zero.\n";
            }

            else if(code == -4164){
                cout << "Order's notional must be no smaller than 50 (unless you choose reduce only).\n";
            }
            // other exchange errors
            else cout << "Limit order failed: " << resp.dump() << "\n";

            order.ts = clock.now_ms();
            order.status = "REJECTED";
            order.exchange_latency = state.exchange_latency;

            recorder.log_quote(order, (order.side == "BUY") ? "BID" : "ASK", "REJECTED");
            open_orders.erase(order.client_oid);

            return;
        }

        recorder.log_quote(order, (side == "BUY") ? "BID" : "ASK", "NEW_SUBMITTED");
    }

    void place_market() override {

        double pos = broker.get_position();
        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = (pos > 0) ? bid_tick : ask_tick;
        std_string side = (pos > 0) ? "SELL" : "BUY";

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = abs(pos);
        order.remaining = abs(pos);
        order.status = "PENDING_NEW";
        order.ts = clock.now_ms();
        order.owner = "self";
        order.signal = *state.last_signal;
        order.queue_ahead_at_join = 0.0;

        json resp = broker.place_market(order);
        order.resp = resp;
    }

    void cancel_order(Order* order){
        
        order->ts = clock.now_ms();
        order->status = "PENDING_CANCEL";
        order->pending_cancel = true;
        
        cout << "- CANCEL LIMIT ORDER - client_oid: " << order->client_oid << 
        ", status: " << order->status << ", timestamp: " << order->ts << "\n";

        json resp = broker.cancel_order(*order);
        order->resp = resp;

        if(resp.contains("code")){ // TO BE DELETED
            int code = resp["code"];

            if(code == -2011){ // Order already filled/canceled
                cout << "Cancel rejected: order no longer open\n";
            }
            // other exchange errors
            else cout << "Cancel failed: " << resp.dump() << "\n";
            return;
        }

        recorder.log_quote(*order, (order->side == "BUY") ? "BID" : "ASK", "CANCEL_SUBMITTED");
    }

    void cancel_all_orders() override {
        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        if(bid_order && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")) cancel_order(bid_order);

        if(ask_order && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")) cancel_order(ask_order);
    }

    void place_quotes(const Signal& signal) override {

        double desired_bid = signal.my_bid;
        double desired_ask = signal.my_ask;

        auto [bid_size, ask_size] = compute_order_size(signal);
        double tick = config.tick_size;

        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        current_bid_size = bid_size;
        current_ask_size = ask_size;

        // -------------------------
        // BID ORDERS
        // -------------------------
        if(!bid_order){ // if no bid order
            if(bid_size > 0.0) place_limit("BUY", desired_bid, bid_size, signal);
        }

        else if(abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")){
            cancel_order(bid_order);
        }

        // -------------------------
        // ASK ORDERS
        // -------------------------
        if(!ask_order){ // if no ask order
            if(ask_size > 0.0) place_limit("SELL", desired_ask, ask_size, signal);
        }

        else if(abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")){
            cancel_order(ask_order);
        }
    }

    Order* get_order(const std_string& msg_client_oid){
        for(auto& [client_oid, order]: open_orders) if(client_oid == msg_client_oid) return &order;
        return nullptr;
    }

    void orderMarketToString(Order* order, const Stream& stream){
        cout << format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, stream.fill_price, stream.fill_qty, order->status) << "\n";
    }

    // -------------------------
    // BINANCE USER STREAM SIGNAL
    // -------------------------
    void apply_stream_update(const Stream& stream) override {

        Order* order = get_order(stream.client_oid);

        if(!order) return;

        // -------------------------
        // EXCHANGE LATENCY
        // -------------------------
        order->exchange_latency = clock.compute_exchange_latency(stream.exchange_ts, order->ts);
        state.exchange_latency = order->exchange_latency;
 
        // -------------------------
        // NEW
        // -------------------------
        if(stream.exec_type == "NEW"){
            
            order->live_ts = stream.exchange_ts;
            order->status = "LIVE";
            order->queue_ahead_at_join = state.set_queue_position(stream.side, order->price_tick);

            (stream.side == "BUY") ? last_bid = stream.price : last_ask = stream.price;
            state.last_order_update = *order;

            cout << "- PLACE LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "NEW");
        }

        // -------------------------
        // CANCELED
        // -------------------------
        else if(stream.exec_type == "CANCELED"){
            
            order->status = "CANCELED";

            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            cout << "- CANCEL LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";

            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "CANCELED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // REJECTED - PENDING NEW ORDER REJECTED
        // -------------------------
        else if(stream.exec_type == "REJECTED"){
            
            order->status = "REJECTED";
            state.last_order_update = *order;

            cout << "- REJECTED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "REJECTED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // EXPIRED - FOR EXPIRED_IN_MATCH, IF EXCHANGE LAGS BEHIND NEW QUOTES
        // -------------------------
        else if(stream.exec_type == "EXPIRED"){
            
            order->status = "EXPIRED";

            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            cout << "- EXPIRED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";

            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "EXPIRED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // TRADE
        // -------------------------
        else if(stream.exec_type == "TRADE"){
            // cout << "stream fill_price: " << stream.fill_price << ", stream fill_qty: " << stream.fill_qty << "\n";
            // cout << "stream fees paid: " << stream.fees_paid << "\n"; // maker_fees = 0.0002, same as params

            // if(toxicity_model){
            //     ToxicityPrediction p;
                    // p.ts = stream.exchange_ts;
                // //p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
            //     p.horizon_ms = toxicity_model->horizon_ms;

            //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
            //     p.fill_price = fill_price;

            //     p.side = (side == "BUY") ? 1 : -1;

            //     state.market_feature_state.toxicity_predictions.push_back(move(p));
            // }

            state.on_fill(stream.fill_price, stream.fill_qty, stream.side, true);
            
            if(stream.status == "PARTIALLY_FILLED"){
                order->status = "PARTIALLY_FILLED";
                order->remaining = max(0.0, order->remaining - stream.fill_qty);

                (stream.side == "BUY") ? state.bid_queue_ahead.second = 0.0 : state.ask_queue_ahead.second = 0.0;

                cout << "- PARTIALLY FILLED LIMIT ORDER - client_oid: " << order->client_oid << 
                ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            }

            else if(stream.status == "FILLED"){
                order->status = "FILLED";
                order->remaining = 0.0;
                
                state.reset_queue_position(stream.side);

                cout << "- FILLED LIMIT ORDER - client_oid: " << order->client_oid << 
                ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            }

            if(stream.order_type == "MARKET"){
                orderMarketToString(order, stream);
                recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, false);
            }
            else recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, true);

            state.last_order_update = *order;
       
            if(stream.status == "FILLED") open_orders.erase(order->client_oid);
        }
        else{
            cout << "UNKNOWN ORDER UPDATE " << stream.exec_type << "\n";
            throw runtime_error("unknown order update");            
        }
    }
};

class BinanceUserStream {
public:
    MarketConfig& config;
    BinanceBroker& broker;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;
    
    atomic<bool> running{false};
    atomic<bool> connected{false};
    
    thread stream_thread;
    simdjson::ondemand::parser parser;

    mutex connection_mtx;
    condition_variable connection_cv;

    BinanceUserStream(MarketConfig& config, BinanceBroker& broker, ExecutionEventQueue& execution_event, BinanceClock& clock)
        : config(config), broker(broker), execution_event(execution_event), clock(clock) {}

    void start(){
        running = true;
        stream_thread = thread([this](){run();});
        wait_until_connected();
    }

    void wait_until_connected(){
        unique_lock<mutex> lock(connection_mtx);
        connection_cv.wait(lock, [this]{return connected.load();});
    }

    void run(){
        broker.open_user_stream();

        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.hostname, "443");
        
        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake(config.hostname, "/ws/" + broker.listen_key);

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        beast::flat_buffer buffer;

        while(running){
            ws.read(buffer);

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            on_message(msg);
        }
    }

    void stop(){
        running = false;

        if(stream_thread.joinable()) stream_thread.join();
    }

    void on_message(const std_string& msg){
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        if(std_string(doc["e"]) != "ORDER_TRADE_UPDATE") return;
        cout << msg << endl;

        simdjson::ondemand::object o = doc["o"].get_object();

        Stream stream;
        stream.client_oid = std_string(o["c"]);
        stream.side = std_string(o["S"]);
        stream.status = std_string(o["X"]);
        stream.exec_type = std_string(o["x"]);
        stream.order_type = std_string(o["o"]);
        stream.price = double(o["p"].get_double_in_string());
        stream.qty = double(o["q"].get_double_in_string());
        stream.fill_price = double(o["L"].get_double_in_string());
        stream.fill_qty = double(o["l"].get_double_in_string());
        stream.fees_paid = double(o["n"].get_double_in_string());
        stream.exchange_ts = int64_t(o["T"]);
        stream.local_ts = clock.now_ms();

        ExecutionEvent ev;
        ev.type = ExecutionEventType::STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};

class Engine {
public:
    MarketConfig& config;
    State& state;
    MarketMakingStrategy& strategy;
    Execution& execution;
    BinanceClock& clock;
    ExecutionEventQueue& execution_event;
    EventNotifier& dashboard_event;
    SnapshotStore& snapshot_store;
    DatasetRecorder& recorder;

    std_string header;
    
    Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& execution, BinanceClock& clock,
        ExecutionEventQueue& execution_event, EventNotifier& dashboard_event, SnapshotStore& snapshot_store, DatasetRecorder& recorder)
        : config(config), state(state), strategy(strategy), execution(execution), clock(clock), execution_event(execution_event),
        dashboard_event(dashboard_event), snapshot_store(snapshot_store), recorder(recorder) {build_header();}

    void process_event(const ExecutionEvent& ev){
        switch(ev.type){
            case ExecutionEventType::TRADE_UPDATE:
                on_trade_event(ev.trade);
                break;

            case ExecutionEventType::DEPTH_UPDATE_SPOT:
                on_depth_event_spot(ev.depth);
                break;
            
            case ExecutionEventType::DEPTH_UPDATE_FUTURES:
                on_depth_event_futures(ev.depth);
                break;

            case ExecutionEventType::STREAM_UPDATE:
                on_stream_event(ev.stream);
                break;

            case ExecutionEventType::MARK_PRICE_UPDATE:
                on_mark_price_event(ev.stream);
                break;
        }

        Snapshot snap = build_snapshot();
        snapshot_store.set(move(snap));

        {
            lock_guard<mutex> lock(dashboard_event.signal_mtx);
            dashboard_event.signal_pending = true;
        }
        dashboard_event.signal_cv.notify_one();
    }

    void process_event_latency(const ExecutionEvent& ev){
        switch(ev.type){
            case ExecutionEventType::TRADE_UPDATE:
                on_trade_event(ev.trade);
                break;

            case ExecutionEventType::DEPTH_UPDATE_SPOT:
                on_depth_event_spot_latency(ev.depth);
                break;
            
            case ExecutionEventType::DEPTH_UPDATE_FUTURES:
                on_depth_event_futures_latency(ev.depth);
                break;
        }
    }

    void on_trade_event(const Trade& trade){
        state.time = trade.local_ts;
        state.last_trade = trade;
        state.last_trade_ts = trade.ts;
        state.trade_latency = trade.latency;

        recorder.log_trade(trade);
        execution.process_trade(trade);
    }

    void on_depth_event_spot(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.time = depth.local_ts;
        state.last_depth_ts = depth.ts;
        state.depth_latency = depth.latency;
        
        auto& book = state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << depth.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA - REMOVE LOCK FOR SINGLE THREADED QUEUE
        // -----------------------------
        state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_residual_realization();
        state.update_performance();
        // state.update_toxicity_realization();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);
        state.last_signal = signal;
        execution.place_quotes(signal);
    }

    void on_depth_event_futures(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.time = depth.local_ts;
        state.last_depth_ts = depth.ts;
        state.depth_latency = depth.latency;
        
        auto& book = state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.pu != book.last_update_id){
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << depth.pu << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_residual_realization();
        state.update_performance();
        // state.update_toxicity_realization();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);
        state.last_signal = signal;
        execution.place_quotes(signal);
    }

    void on_depth_event_spot_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.time = depth.local_ts;
        state.last_depth_ts = depth.ts;
        state.depth_latency = depth.latency;
        
        auto& book = state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << depth.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_residual_realization();
        state.update_performance();
        // state.update_toxicity_realization();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);
        state.last_signal = signal;
        execution.place_quotes_latency(signal);
    }

    void on_depth_event_futures_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.time = depth.local_ts;
        state.last_depth_ts = depth.ts;
        state.depth_latency = depth.latency;
        
        auto& book = state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.pu != book.last_update_id){
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << depth.pu << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_residual_realization();
        state.update_performance();
        // state.update_toxicity_realization();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);
        state.last_signal = signal;
        execution.place_quotes_latency(signal);
    }

    void on_stream_event(const Stream& stream){
        state.time = stream.local_ts;
        execution.apply_stream_update(stream);
        execution.place_quotes(*state.last_signal);
    }

    void on_mark_price_event(const Stream& stream){
        state.time = stream.local_ts;
        state.mark_price = stream.price;
    }

    std_string tradeToString(const optional<Trade>& trade){
        return trade ? format("{:<5} | {:>10.4f} | {:>8.6f}", trade->side, trade->price, trade->qty) : "—";
    }

    std_string orderPointerToString(Order* order){
        return order ? format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->remaining, order->status) : "—";
    }

    std_string orderOptionalToString(const optional<Order>& order){
        return order ? format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->remaining, order->status) : "—";
    }

    Snapshot build_snapshot(){
        Snapshot snap;

        auto& book = state.market_book;
 
        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double spread = best_ask - best_bid;
        double microprice = (best_ask * bid_size + best_bid * ask_size) /(bid_size + ask_size + 1e-9);

        double bid_queue = (state.bid_queue_ahead.first == bid_tick) ? state.bid_queue_ahead.second : 0.0;
        double ask_queue = (state.ask_queue_ahead.first == ask_tick) ? state.ask_queue_ahead.second : 0.0;

        snap.title.header = header;
        snap.title.regime = state.last_signal ? state.last_signal->regime : "";
        snap.title.pnl_pct = state.get_pnl(mid) / config.initial_cash * 100;

        snap.market.mid = mid;
        snap.market.microprice = microprice;
        snap.market.spread = spread;
        snap.market.best_bid = best_bid;
        snap.market.best_ask = best_ask;
        snap.market.bid_size = bid_size;
        snap.market.ask_size = ask_size;
        snap.market.ewma_vol = state.get_vol();
        snap.market.order_imbalance = state.order_imbalance;
        snap.market.trade_imbalance = state.trade_imbalance;
        snap.market.trade = tradeToString(state.last_trade);

        snap.regime.regime = state.last_signal ? state.last_signal->regime : "";
        snap.regime.confidence = state.last_signal ? state.last_signal->regime_prob : 0.0;

        snap.signals.fair = state.last_signal ? state.last_signal->fair : 0.0;
        snap.signals.skew = state.last_signal ? state.last_signal->skew : 0.0;
        snap.signals.reservation = state.last_signal ? state.last_signal->reservation : 0.0;
        snap.signals.alpha_order_imb = state.last_signal ? state.last_signal->alpha_order_imb : 0.0;
        snap.signals.alpha_trade_imb = state.last_signal ? state.last_signal->alpha_trade_imb : 0.0;
        snap.signals.alpha_struct = state.last_signal ? state.last_signal->alpha_struct : 0.0;
        snap.signals.k0 = state.last_signal ? state.last_signal->k0 : 0.0;
        snap.signals.spread_multiplier = state.last_signal ? state.last_signal->spread_multiplier : 0.0;
        snap.signals.inventory_target = state.last_signal ? state.last_signal->inventory_target : 0.0;
        snap.signals.residual_signal_quality = state.last_signal ? state.last_signal->residual_signal_quality : 0.0;
        snap.signals.tox = state.last_signal ? state.last_signal->toxicity.tox : 0.0;
        snap.signals.k1 = state.last_signal ? state.last_signal->toxicity.k1 : 0.0;
        snap.signals.k2 = state.last_signal ? state.last_signal->toxicity.k2 : 0.0;

        snap.quotes.my_bid = execution.get_last_bid();
        snap.quotes.my_ask = execution.get_last_ask();
        snap.quotes.current_bid_size = execution.get_current_bid_size();
        snap.quotes.current_ask_size = execution.get_current_ask_size();

        snap.execution.bid_queue = bid_queue;
        snap.execution.ask_queue = ask_queue;
        snap.execution.bid_pressure = bid_queue / (bid_size + 1e-9);
        snap.execution.ask_pressure = ask_queue / (ask_size + 1e-9);

        snap.execution.buy_order = orderPointerToString(execution.get_open_order("BUY"));
        snap.execution.sell_order = orderPointerToString(execution.get_open_order("SELL"));
        snap.execution.last_fill_candidate = orderOptionalToString(state.last_fill_candidate);
        snap.execution.last_order_update = orderOptionalToString(state.last_order_update);
        
        snap.risk.inventory = state.inventory;
        snap.risk.realized_pnl = state.realized_pnl;
        snap.risk.unrealized_pnl = state.get_unrealized_pnl(mid);
        snap.risk.fees_paid = state.fees_paid;
        snap.risk.total_pnl = state.get_pnl(mid);

        snap.system.time = config.format_ms_precise(state.time);
        snap.system.last_trade_ts = config.format_ms_precise(state.last_trade_ts);
        snap.system.last_depth_ts = config.format_ms_precise(state.last_depth_ts);
        snap.system.trade_latency = state.trade_latency;
        snap.system.depth_latency = state.depth_latency;
        snap.system.exchange_latency = state.exchange_latency;

        return snap;
    }

    void build_header(){
        std_string parts;

        auto add = [&](const std_string& s){
            if(s.empty()) return;
            if(!parts.empty()) parts += " | ";
            parts += s;
        };

        add(config.struct_model);
        add(config.regime_model);
        add(config.micro_signal_model);
        add(config.residual_model);
        add(config.toxicity_model);
        add(config.mode);
        add(config.exchange + "_" + config.market);
        add(config.instrument);

        header = parts;
    }
};

class BinanceClock {
public:
    MarketConfig& config;

    atomic<int64_t> offset_ms{0};
    atomic<int64_t> rtt_ms{0};

    atomic<bool> running{false};
    atomic<bool> connected{false};

    mutex connection_mtx;
    mutex clock_mtx;
    condition_variable connection_cv;
    condition_variable clock_cv;

    thread clock_thread;

    BinanceClock(MarketConfig& config): config(config) {}

    int64_t now_ms() const {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    int64_t compute_feed_latency(const int64_t& local_ts, const int64_t& feed_ts) const {
        return (local_ts - offset_ms.load()) - feed_ts;
    }

    int64_t compute_exchange_latency(const int64_t& stream_ts, const int64_t& local_ts) const {
        return stream_ts - (local_ts - offset_ms.load());
    }

    void start(){
        running = true;
        clock_thread = thread(&BinanceClock::clock_sync_loop, this);
        wait_until_connected();
    }

    void wait_until_connected(){
        unique_lock<mutex> lock(connection_mtx);
        connection_cv.wait(lock, [this]{return connected.load();});
    }

    void clock_sync_loop(){
        while(running){
            try{
                sync_once();
            }
            catch(const exception& e){
                cout << "CLOCK ERROR: " << e.what() << "\n";
            }

            unique_lock<mutex> lock(clock_mtx);
            clock_cv.wait_for(lock, seconds(5), [this]{return !running.load();});
        }
    }

    void sync_once(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.base_url, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        simdjson::ondemand::parser parser;

        int64_t best_offset = 0;
        int64_t best_rtt = INT64_MAX;

        for(int i = 0; i < 5; i++){
            http::request<http::empty_body> req{http::verb::get, config.endpoint + "/time", 11};

            req.keep_alive(true);
            req.set(http::field::host, config.base_url);
            req.set(http::field::user_agent, "mm-engine");

            // Wall clock (for Binance comparison)
            int64_t t0_wall = now_ms();

            // Monotonic clock (for RTT)
            auto t0 = steady_clock::now();

            http::write(ssl_sock, req);
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(ssl_sock, buffer, res);

            auto t1 = steady_clock::now();
            int64_t t1_wall = now_ms();

            simdjson::padded_string json(res.body());
            auto doc = parser.iterate(json);

            int64_t server_ms = int64_t(doc["serverTime"]);
            int64_t rtt = duration_cast<milliseconds>(t1 - t0).count();

            // Midpoint of the wall clock
            int64_t midpoint_wall = (t0_wall + t1_wall) / 2;
            int64_t offset = midpoint_wall - server_ms;

            if(rtt < best_rtt){
                best_rtt = rtt;
                best_offset = offset;
            }

            this_thread::sleep_for(milliseconds(100));
        }

        offset_ms.store(best_offset);
        rtt_ms.store(best_rtt);

        cout << "base_url: " << config.base_url << " endpoint: " << config.endpoint  + "/time" << " [CLOCK]"
        << " best_offset: " << best_offset << " ms" << " best_rtt: " << best_rtt << " ms\n";

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        boost::system::error_code ec;
        ssl_sock.shutdown(ec);
    }

    void stop(){
        running = false;
        clock_cv.notify_one();

        if(clock_thread.joinable()) clock_thread.join();
    }
};

class SnapshotStore {
public:
    mutex mtx;
    Snapshot latest;

    void set(Snapshot snap){
        lock_guard<mutex> lock(mtx);
        latest = move(snap);
    }

    Snapshot get(){
        lock_guard<mutex> lock(mtx);
        return latest;
    }
};

class DashboardTerminal {
public:
    SnapshotStore& snapshot_store;
    ScreenInteractive screen = ScreenInteractive::Fullscreen();
    thread terminal_thread;

    DashboardTerminal(SnapshotStore& snapshot_store) : snapshot_store(snapshot_store) {}

    void start(){
        terminal_thread = thread([this](){
            Component ui = Dashboard();
            screen.Loop(ui);
        });
    }

    void refresh(){
        screen.PostEvent(Event::Custom);
    }

    void stop(){
        screen.ExitLoopClosure()();

        if(terminal_thread.joinable()) terminal_thread.join();
    }

    Element color_pnl(const double& value){
        if(value > 0) return text("▲ " + format("{:.4f}", value)) | color(Color::Green);
        else if(value < 0) return text("▼ " + format("{:.4f}", abs(value))) | color(Color::Red);
        else return text(format("{:.4f}", value));
    }

    Element color_risk(const double& inventory, const double& limit = 10.0) {
        double intensity = min(abs(inventory) / limit, 1.0);
        auto value = text(format("{:.4f}", inventory));

        if(intensity < 0.3) return value | color(Color::Green);
        else if(intensity < 0.7) return hbox({text("▲ "),  value}) | color(Color::Yellow);
        else return hbox({text("▲ "), value}) | color(Color::Red);
    }

    Element centered_inventory_bar(const double& inv, const double& max_inv = 10.0, const int& width = 21){
        int half = width / 2;
        int scaled = static_cast<int>((inv / max_inv) * half);
        scaled = clamp(scaled, -half, half);

        std_string left, right;

        for(int i = 0; i < half; i++){
            left += ((half - i - 1) < -scaled ? "█" : " ");
            right += (i < scaled ? "█" : " ");
        }

        return hbox({text(left) | color(Color::Red), text("|"), text(right) | color(Color::Green)});
    }

    Component Dashboard(){

        auto row_title = [](const std_string& title, const std_string& value){
            return hbox({
                text(" "), text(move(title)) | bold | color(Color::Yellow) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text(move(value)) | color(Color::GrayLight)
            });
        };
        
        auto row_text = [](const std_string& title, const std_string& value){
            return hbox({
                text(" "), text(move(title)) | color(Color::CyanLight) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text(move(value)) | color(Color::GrayLight)
            });
        };

        auto row_elem = [](const std_string& title, const ftxui::Element& value){
            return hbox({
                text(" "), text(move(title)) | color(Color::CyanLight) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), move(value) | color(Color::GrayLight)
            });
        };

        auto make_title = [](const Snapshot& snap){
            vector<std_string> parts;

            auto add = [&](const std_string& s){
                if(!s.empty()) parts.push_back(s);
            };

            stringstream ss(snap.title.header);
            std_string item;

            while(getline(ss, item, '|')){
                // trim spaces
                item.erase(0, item.find_first_not_of(" "));
                item.erase(item.find_last_not_of(" ") + 1);
                add(item);
            }

            ftxui::Elements rendered;

            for(size_t i = 0; i < parts.size(); i++){
                if(i > 0) rendered.push_back(text(" | ") | dim);
                rendered.push_back(text(parts[i]) | italic);
            }

            rendered.push_back(text(" | ") | dim);
            rendered.push_back(text(snap.title.regime) | italic);
            rendered.push_back(text(" | ") | dim);
            rendered.push_back(text("pnl=") | italic);
            rendered.push_back(text((snap.title.pnl_pct > 0 ? "+" : "") + format("{:.4f}", snap.title.pnl_pct) + "%") | italic);

            return hbox(move(rendered)) | color(Color::GrayLight) | center;
        };

        return ftxui::Renderer([&]{
            Snapshot snap = snapshot_store.get();

            auto title = make_title(snap);

            auto header = hbox({
                text(" "), text("Metric") | bold | color(Color::White) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text("Value") | bold | color(Color::White)
            });

            auto market = vbox({
                row_title("MARKET", ""),
                row_text("Mid", format("{:<15.4f}", snap.market.mid)),
                row_text("Microprice", format("{:<15.4f}", snap.market.microprice)),
                row_text("Spread", format("{:<15.4f}", snap.market.spread)),
                row_text("Best Bid / Size", format("{:<10.4f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.market.bid_size) + ")"),
                row_text("Best Ask / Size", format("{:<10.4f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.market.bid_size) + ")"),
                row_text("EWMA Vol", format("{:.2e}", snap.market.ewma_vol)),
                row_text("Order Imbalance", format("{:<15.4f}", snap.market.order_imbalance)),
                row_text("Trade Imbalance", format("{:<15.4f}", snap.market.trade_imbalance)),
                row_text("Last Trade", snap.market.trade),
                row_text("", "")
            });

            auto regime = vbox({
                row_title("REGIME", ""),
                row_text("Regime", snap.regime.regime),
                row_text("Confidence", format("{:<15.2f}", snap.regime.confidence)),
                row_text("", "")
            });

            auto signals = vbox({
                row_title("SIGNALS", ""),
                row_text("Spread Multiplier", format("{:<15.4f}", snap.signals.spread_multiplier)),
                row_text("Inventory Target", format("{:<15.2f}", snap.signals.inventory_target)),
                row_text("Alpha Order Imb", format("{:<15.2f}", snap.signals.alpha_order_imb)),
                row_text("Alpha Trade Imb", format("{:<15.2f}", snap.signals.alpha_trade_imb)),
                row_text("Alpha Struct", format("{:<15.2f}", snap.signals.alpha_struct)),
                row_text("Fair Value", format("{:<15.4f}", snap.signals.fair)),
                row_text("Inventory Skew", format("{:<15.4f}", snap.signals.skew)),
                row_text("Residual Signal Quality", format("{:<15.2f}", snap.signals.residual_signal_quality)),
                row_text("Toxicity", format("{:<15.2f}", snap.signals.tox)),
                row_text("Reservation", format("{:<15.4f}", snap.signals.reservation)),
                
                row_text("", "")
            });
            
            auto quotes = vbox({
                row_title("QUOTES", ""),
                row_text("My Bid / Size", format("{:<10.4f}", snap.quotes.my_bid) + " (" + format("{:<6.4f}", snap.quotes.current_bid_size) + ")"),
                row_text("My Ask / Size", format("{:<10.4f}", snap.quotes.my_ask) + " (" + format("{:<6.4f}", snap.quotes.current_ask_size) + ")"),
                row_text("", "")
            });

            auto execution = vbox({
                row_title("EXECUTION", ""),
                row_text("Queue Ahead / Bid", format("{:<10.4f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.execution.bid_queue) + ")"),
                row_text("Queue Ahead / Ask", format("{:<10.4f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.execution.ask_queue) + ")"),
                row_text("Queue Pressure / Bid", format("{:<10.4f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.execution.bid_pressure) + ")"),
                row_text("Queue Pressure / Ask", format("{:<10.4f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.execution.ask_pressure) + ")"),
                row_text("Open Orders", snap.execution.buy_order),
                row_text("", snap.execution.sell_order),
                row_text("Last Fill Candidate", snap.execution.last_fill_candidate),
                row_text("Last Order Update", snap.execution.last_order_update),
                row_text("", "")
            });

            auto risk = vbox({
                row_title("RISK", ""),
                row_elem("Inventory", color_risk(snap.risk.inventory)),
                row_elem("Realized PnL", color_pnl(snap.risk.realized_pnl)),
                row_elem("Unrealized PnL", color_pnl(snap.risk.unrealized_pnl)),
                row_elem("Fees Paid", color_pnl(snap.risk.fees_paid)),
                row_elem("Total PnL", color_pnl(snap.risk.total_pnl)),
                row_elem("Risk", centered_inventory_bar(snap.risk.inventory)),
                row_text("", "")
            });

            auto system = vbox({
                row_title("SYSTEM", ""),
                row_text("Time", snap.system.time),
                row_text("Last Trade ts", snap.system.last_trade_ts),
                row_text("Last Depth ts", snap.system.last_depth_ts),
                row_text("Trade Latency", format("{:<10}", snap.system.trade_latency)),
                row_text("Depth Latency", format("{:<10}", snap.system.depth_latency)),
                row_text("Exchange Latency", format("{:<10}", snap.system.exchange_latency)),
                row_text("", "")
            });

            auto content = vbox({title, separator(), header, separator(), market, regime, signals, 
                    quotes, execution, risk, system, separator()});
        
            return content | border | color(Color::GrayLight);
        });
    };
};

class WsSession : public enable_shared_from_this<WsSession> {
public:
    websocket::stream<tcp::socket> ws; 
    boost::beast::flat_buffer buffer; 
    mutex write_mtx;
    
    explicit WsSession(tcp::socket socket) : ws(move(socket)) {}
        
    void run(){
        auto self = shared_from_this();
        ws.set_option(websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

        ws.async_accept([self](boost::system::error_code ec){
            if(ec) return;

            self->ws.text(true);
            cout << "DASHBOARD CLIENT CONNECTED\n";
        });
    }
    
    bool send(const std_string& msg){
        lock_guard<mutex> lock(write_mtx);
        boost::system::error_code ec;
        ws.write(boost::asio::buffer(msg), ec);

        if(ec) return false;  // IMPORTANT: signal failure instead of throwing
        return true;
    }
    
    void close(){ 
        boost::system::error_code ec; 
        ws.close(websocket::close_code::normal, ec); 
    }
};

class DashboardServer {
public:
    MarketConfig& config;
    SnapshotStore& snapshot_store; 

    boost::asio::io_context ioc; 
    tcp::acceptor acceptor; 

    set<shared_ptr<WsSession>> clients;
    mutex dash_mtx;
    thread server_thread;

    atomic<bool> running{false}; // keeps io_context alive 
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard; 
    
    DashboardServer(MarketConfig& config, SnapshotStore& snapshot_store)
        : config(config), snapshot_store(snapshot_store), acceptor(ioc), guard(boost::asio::make_work_guard(ioc)) {}

    void start(){
        running = true;
        tcp::endpoint endpoint(boost::asio::ip::make_address(config.host), config.port); 
        
        acceptor.open(endpoint.protocol());
        acceptor.set_option(tcp::acceptor::reuse_address(true));
        acceptor.bind(endpoint);
        acceptor.listen();
        
        server_thread = thread([this](){
            cout << "WS RUNNING ON http://localhost:" << config.port << "\n";
            do_accept();
            ioc.run();
        });
    }

    void do_accept(){ 
        acceptor.async_accept([this](boost::system::error_code ec, tcp::socket socket){
            if(!ec){
                auto session = make_shared<WsSession>(move(socket));
                {
                    lock_guard<mutex> lock(dash_mtx);
                    clients.insert(session);
                } // IMPORTANT: must run in same strand context
                session->run();
            }
            else if(running){
                // only print errors when server is actually running
                cout << "ACCEPT ERROR: " << ec.message() << "\n";
            }
            if(running) do_accept();
        });
    }

    void publish(){
        json event = {
            {"type","snapshot"},
            {"data", snapshot_to_json(snapshot_store.get())}
        };
        std_string msg = event.dump();

        vector<shared_ptr<WsSession>> snapshot_clients;
        {
            lock_guard<mutex> lock(dash_mtx);
            snapshot_clients.assign(clients.begin(), clients.end());
        }

        for(auto& client: snapshot_clients){
            bool ok = client->send(msg);

            if(!ok){
                client->close();
                lock_guard<mutex> lock(dash_mtx);
                clients.erase(client);
            }
        }
    }

    void stop(){ 
        running = false; 
        boost::system::error_code ec; 
        acceptor.close(ec);
        ioc.stop();
        if(server_thread.joinable()) server_thread.join();
    }

    json snapshot_to_json(const Snapshot& snap){
        return {
            {"title", {
                {"header", snap.title.header},
                {"regime", snap.title.regime},
                {"pnl_pct", snap.title.pnl_pct}
            }},
            {"market", {
                {"mid", snap.market.mid},
                {"microprice", snap.market.microprice},
                {"spread", snap.market.spread},
                {"best_bid", snap.market.best_bid},
                {"best_ask", snap.market.best_ask},
                {"bid_size", snap.market.bid_size},
                {"ask_size", snap.market.ask_size},
                {"ewma_vol", snap.market.ewma_vol},
                {"order_imbalance", snap.market.order_imbalance},
                {"trade_imbalance", snap.market.trade_imbalance},
                {"trade", snap.market.trade},
            }},
            {"regime", {
                {"regime", snap.regime.regime},
                {"confidence", snap.regime.confidence},
            }},
            {"signals", {
                {"fair", snap.signals.fair},
                {"skew", snap.signals.skew},
                {"reservation", snap.signals.reservation},
                {"alpha_order_imb", snap.signals.alpha_order_imb},
                {"alpha_trade_imb", snap.signals.alpha_trade_imb},
                {"alpha_struct", snap.signals.alpha_struct},
                {"k0", snap.signals.k0},
                {"spread_multiplier", snap.signals.spread_multiplier},
                {"inventory_target", snap.signals.inventory_target},
                {"residual_signal_quality", snap.signals.residual_signal_quality},
                {"tox", snap.signals.tox},
                {"k1", snap.signals.k1},
                {"k2", snap.signals.k2},
            }},
            {"quotes", {
                {"my_bid", snap.quotes.my_bid},
                {"my_ask", snap.quotes.my_ask},
                {"current_bid_size", snap.quotes.current_bid_size},
                {"current_ask_size", snap.quotes.current_ask_size},
            }},
            {"execution", {
                {"bid_queue", snap.execution.bid_queue},
                {"ask_queue", snap.execution.ask_queue},
                {"bid_pressure", snap.execution.bid_pressure},
                {"ask_pressure", snap.execution.ask_pressure},
                {"buy_order", snap.execution.buy_order},
                {"sell_order", snap.execution.sell_order},
                {"last_fill_candidate", snap.execution.last_fill_candidate},
                {"last_order_update", snap.execution.last_order_update},
            }},
            {"risk", {
                {"inventory", snap.risk.inventory},
                {"realized_pnl", snap.risk.realized_pnl},
                {"unrealized_pnl", snap.risk.unrealized_pnl},
                {"fees_paid", snap.risk.fees_paid},
                {"total_pnl", snap.risk.total_pnl},
            }},
            {"system", {
                {"time", snap.system.time},
                {"last_trade_ts", snap.system.last_trade_ts},
                {"last_depth_ts", snap.system.last_depth_ts},
                {"trade_latency", snap.system.trade_latency},
                {"depth_latency", snap.system.depth_latency},
                {"exchange_latency", snap.system.exchange_latency},
            }}
        };
    }
};

class TradingSystem {
public:
    const json& params;
    MarketConfig config;
    State state;
    MarketMakingStrategy strategy;
    DatasetRecorder recorder;
    BinanceClock clock;

    ExecutionEventQueue execution_event;
    EventNotifier dashboard_event;
    SnapshotStore snapshot_store;
    DashboardTerminal dashboard_terminal;
    DashboardServer dashboard_server;

    unique_ptr<Execution> execution;
    unique_ptr<BinanceBroker> broker;
    unique_ptr<BinanceUserStream> user_stream;

    unique_ptr<Engine> engine;
    unique_ptr<Feed> feed;

    asio::io_context ioc;
    asio::signal_set signals;
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

    atomic<bool> engine_running{false};
    atomic<bool> dashboard_running{false};
    atomic<bool> shutdown_requested{false};

    vector<thread> threads;

    TradingSystem(const json& params) :
        params(params), config(params), state(config), strategy(config), recorder(config, state, params),
        clock(config), dashboard_terminal(snapshot_store), dashboard_server(config, snapshot_store),
        signals(ioc, SIGINT, SIGTERM) {initialize();}

    void initialize(){
        if(config.mode == "live"){
            broker = make_unique<BinanceBroker>(config, clock);
            execution = make_unique<LiveExecution>(config, state, recorder, *broker, clock);
            user_stream = make_unique<BinanceUserStream>(config, *broker, execution_event, clock);
        }
        
        else if(config.mode != "live"){
            execution = make_unique<PaperExecution>(config, state, recorder, clock);
        }

        engine = make_unique<Engine>(config, state, strategy, *execution, clock, execution_event, dashboard_event, snapshot_store, recorder);

        auto log_event = [this](const std_string& type, const int64_t& ts, const int64_t& local_ts, 
            const int64_t& latency, const std_string& msg) {recorder.log_event(type, ts, local_ts, latency, msg);};
        auto export_orderbook_snapshot = [this](const json& snapshot) {recorder.export_orderbook_snapshot(snapshot);};

        if(config.exchange == "binance" && config.market == "spot" && config.mode != "replay"){
            feed = make_unique<BinanceSpotFeed>(config, state, execution_event, clock, log_event, export_orderbook_snapshot);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode != "replay"){
            feed = make_unique<BinanceFuturesFeed>(config, state, execution_event, clock, log_event, export_orderbook_snapshot);
        }

        else if(config.exchange == "binance" && config.market == "spot" && config.mode == "replay"){
            feed = make_unique<BinanceSpotReplayFeed>(config, state, execution_event, clock, log_event, export_orderbook_snapshot);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode == "replay"){
            feed = make_unique<BinanceFuturesReplayFeed>(config, state, execution_event, clock, log_event, export_orderbook_snapshot);
        }
    }

    void start_signal_handler(){
        signals.async_wait([this](const boost::system::error_code& ec, int signal){
                if(ec) return;

                cout << "Signal received: " << signal << "\n";
                {
                    lock_guard<mutex> lock(shutdown_mtx);
                    shutdown_requested = true;
                }
                shutdown_cv.notify_one();
            }
        );
    }

    void start(){
        engine_running = true;
        dashboard_running = true;

        start_signal_handler();
        threads.emplace_back([this](){ioc.run();});

        clock.start();

        // dashboard_terminal.start();
        dashboard_server.start();

        feed->start();

        if(user_stream) user_stream->start();

        start_dashboard_loop();

        if(config.exchange_latency == 0){
            cout << "Starting execution loop\n";
            start_execution_loop();
        }
        else{
            cout << "Starting execution latency loop\n";
            start_execution_latency_loop();
        }
    }

    void start_dashboard_loop(){
        threads.emplace_back([this](){
            while(dashboard_running){
                {
                    unique_lock<mutex> lock(dashboard_event.signal_mtx);
                    dashboard_event.signal_cv.wait(lock, [this]{
                        return dashboard_event.signal_pending || !dashboard_running;});

                    if(!dashboard_running) break;
                    dashboard_event.signal_pending = false;
                }

                // dashboard_terminal.refresh();
                dashboard_server.publish();
            }
        });
    }

    void start_execution_loop(){
        threads.emplace_back([this](){
            while(engine_running){
                ExecutionEvent ev;

                if(!execution_event.pop(ev, engine_running)) break;

                engine->process_event(ev);
            }
        });
    }

    void start_execution_latency_loop(){ //polling driven
        threads.emplace_back([this](){
            while(engine_running){
                ExecutionEvent ev;
                bool state_changed = false;

                if(execution_event.pop_timeout(ev, engine_running, 1ms)){
                    engine->process_event_latency(ev);
                    state_changed = true;
                }

                if(execution->process_latency_queue()){
                    state_changed = true;
                }

                if(state_changed){
                    Snapshot snap = engine->build_snapshot();
                    snapshot_store.set(move(snap));

                    {
                        lock_guard<mutex> lock(dashboard_event.signal_mtx);
                        dashboard_event.signal_pending = true;
                    }
                    dashboard_event.signal_cv.notify_one();
                }
            }
        });
    }

    void wait_for_shutdown(){
        unique_lock<mutex> lock(shutdown_mtx);

        shutdown_cv.wait(lock, [this]{
            return shutdown_requested.load();
        });

        shutdown();
    }

    void shutdown(){
        cout << "INTERRUPT RECEIVED - SHUTTING DOWN\n";

        feed->stop();

        //--------------------------------------------------
        // cancel orders
        //--------------------------------------------------
        cout << "CLOSING OPEN POSITIONS\n";
        execution->cancel_all_orders();

        while(execution->get_open_order("BUY") || execution->get_open_order("SELL")){
            this_thread::sleep_for(milliseconds(50));
        }

        //--------------------------------------------------
        // flatten inventory
        //--------------------------------------------------
        execution->place_market();

        while(abs(state.inventory) > 1e-9){
            this_thread::sleep_for(milliseconds(50));
        }
        cout << "\n";

        //--------------------------------------------------
        // dashboards
        //--------------------------------------------------
        dashboard_running = false;
        dashboard_event.signal_cv.notify_all();

        // dashboard_terminal.stop();
        dashboard_server.stop();

        //--------------------------------------------------
        // now stop engine
        //--------------------------------------------------
        engine_running = false;

        // wake execution thread if it is blocked in cv.wait()
        execution_event.wake();

        clock.stop();

        if(broker) broker->stop_keepalive();
        if(user_stream) user_stream->stop();

        ioc.stop();

        for(auto& t: threads){
            if(t.joinable()) t.join();
        }

        recorder.stop();
    }
};

int main(){
    // std_string path;
    
    // cout << "Enter manifest path: ";
    // getline(cin, path);
    // path = path.substr(1, path.size() - 2);

    std_string path = "D:\\OneDrive\\Trading\\manifest.json";
    
    ifstream f(path);

    if(!f.is_open()){
        cerr << "Cannot open manifest\n";
        return 1;
    }

    cout << path << "\n";
    json params;
    f >> params;

    TradingSystem system(params);

    system.start();
    system.wait_for_shutdown();

    return 0;
}