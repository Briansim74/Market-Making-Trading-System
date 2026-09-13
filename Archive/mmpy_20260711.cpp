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
#include <boost/beast/core.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <curl/curl.h>
#include <cpr/cpr.h>
#include "simdjson.h"
#include <nlohmann/json.hpp>
#include <xgboost/c_api.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <ftxui/dom/table.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>

#include "mmpy_structs.hpp" //structs
#include "mmpy_config.hpp" //market config & orderbook
#include "mmpy_dashboard.hpp" //dashboard classes
#include "mmpy_feed.hpp" // feeds
#include "mmpy_state.hpp" //state & market_feature_state
#include "mmpy_recorder.hpp" //dataset recorder
#include "mmpy_strategy.hpp" // models & strategy

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

using std::cout;
using json = nlohmann::json;
using std_string = std::string;

using namespace std;
using namespace arrow;
using namespace ftxui;
using namespace std::chrono;

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;
using ssl_stream = boost::asio::ssl::stream<tcp::socket>;
using ws_stream  = websocket::stream<ssl_stream>;

struct Trade {
    std_string side;
    double price;
    double qty;
    uint64_t ts;
};

struct Depth {
    uint64_t ts;
    uint64_t U;
    uint64_t u;
    uint64_t pu;
    vector<pair<int64_t, double>> bid_delta;
    vector<pair<int64_t, double>> ask_delta;
};

struct EventNotifier {
    mutex signal_mtx;
    condition_variable signal_cv;
    bool signal_pending = false;
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
    uint64_t ts;
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
    uint64_t ts;
    std_string owner;
    json resp;
    Signal signal;
    double queue_ahead_at_join;
};

struct ResidualPred {
    uint64_t ts;
    uint64_t horizon_ms;
    double pred;
    double reservation;
    double realized;
};

struct ToxicityPred {
    uint64_t ts;
    uint64_t horizon_ms;
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

struct LatencyEvent {
    uint64_t execute_ts;
    std_string type;
    Signal signal;
};

struct Compare {
    bool operator()(const LatencyEvent& a, const LatencyEvent& b){
        return a.execute_ts > b.execute_ts; // "Does a have lower priority than b?" -> min heap
    }
};

struct EventsRow {
    std_string type;
    uint64_t ts;
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
        std_string latency_ms;
    } system;
};

struct SnapshotRow {
    uint64_t ts;
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
    std_string symbol;
    std_string side;
    double price;
    int64_t price_tick;
    double qty;

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
    double tick_size = 0.0;
    double step_size = 0.0;
    size_t price_precision = 0;
    size_t qty_precision = 0;

    MarketConfig(const json& params){
        load_filters(params);
    }

    int get_precision(double step){
        int precision = 0;
        while(step < 1.0){
            step *= 10.0;
            precision++;
            if(precision > 18) break; // safety guard
        }
        return precision;
    }

    void load_filters(const json& params){
        if(params["exchange"].get<std_string>() == "binance_spot"){
            tick_size = params["tick_size"].get<double>();
            step_size = params["step_size"].get<double>();
            price_precision = get_precision(tick_size);
            qty_precision   = get_precision(step_size);
            cout << "Exchange: " << params["exchange"].get<std_string>() << "\n";
            cout << "tick_size: " << tick_size << ", price_precision: " << price_precision << "\n";
            cout << "step_size: " << step_size << ", qty_precision: " << qty_precision << "\n";
            return;
        }

        std_string instrument = params["instrument"].get<std_string>();
        transform(instrument.begin(), instrument.end(), instrument.begin(), [](unsigned char c){return toupper(c);});

        std_string url = params["api"]["base_url"].get<std_string>() + "/fapi/v1/exchangeInfo?symbol=" + instrument;
        auto r = cpr::Get(cpr::Url{url});
        auto data = json::parse(r.text);

        cout << "Exchange: " << params["exchange"].get<std_string>() << "\n";
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

    int64_t to_tick(double price) const{
        return static_cast<int64_t>(llround(price / tick_size));
    }

    double from_tick(int64_t tick) const{
        return tick * tick_size;
    }

    double round_price(double price) const{
        return llround(price / tick_size) * tick_size;
    }

    uint64_t now_ms() const{
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    uint64_t to_ms(double ts) const{
        return static_cast<uint64_t>(ts * 1000.0);
    }

    std_string format_ms(uint64_t ts_ms) const{
        time_t t = ts_ms / 1000;
        tm tm = *localtime(&t);

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S");

        return oss.str();
    }

    std_string format_ms_precise(uint64_t ts_ms) const{
        time_t t = ts_ms / 1000;
        tm tm = *localtime(&t);

        int ms = ts_ms % 1000;

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << setw(3) << setfill('0') << ms;

        return oss.str();
    }
};

class OrderBook {
public:
    MarketConfig& config; // pointer to avoid copies
    const json& params;
    std::map<int64_t, double, greater<>> bids;
    std::map<int64_t, double> asks;
    uint64_t last_update_id = 0;
    recursive_mutex mtx; //recursive mtx
    
    OrderBook(MarketConfig& config, const json& params): config(config), params(params) {}

    pair<int64_t, double> best_bid(){
        if(bids.empty()) return {0, 0.0};
        return *bids.begin();
    }

    pair<int64_t, double> best_ask(){
        if(asks.empty()) return {0, 0.0};
        return *asks.begin();
    }

    double mid(){
        lock_guard<recursive_mutex> lock(mtx);

        if(bids.empty() || asks.empty())
            return 0.0;

        auto [bid_tick, bid_size] = best_bid();
        auto [ask_tick, ask_size] = best_ask();

        return config.from_tick((bid_tick + ask_tick) / 2);
    }

    pair<uint64_t, json> initialize_from_binance(const std_string& symbol, int limit){
        
        std_string url;
        if(params["exchange"] == "binance_spot"){
            url = "https://api.binance.com/api/v3/depth?symbol=" + symbol + "&limit=" + to_string(limit);
        }
        else if(params["exchange"] == "binance_futures"){
            url = "https://testnet.binancefuture.com/fapi/v1/depth?symbol=" + symbol + "&limit=" + to_string(limit);
        }

        auto r = cpr::Get(cpr::Url{url});
        auto snapshot = json::parse(r.text);

        last_update_id = snapshot["lastUpdateId"].get<uint64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());
            
            //cout << "p: " << p << " q: " << q << "\n";

            bids[config.to_tick(p)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            //cout << "p: " << p << " q: " << q << "\n";

            asks[config.to_tick(p)] = q;
        }

        cout << "SNAPSHOT FETCHED: " << last_update_id << "\n";
        cout << "ORDER BOOK INITIALIZED\n";

        return {last_update_id, snapshot};
    }

    pair<uint64_t, json> initialize_from_orderbook_snapshot(const json& snapshot){

        last_update_id = snapshot["lastUpdateId"].get<uint64_t>();

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
        lock_guard<recursive_mutex> lock(mtx);

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
    uint64_t horizon_ms;

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
        horizon_ms = artifact["horizon_ms"].get<uint64_t>();

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
    uint64_t horizon_ms;

    double intercept;
    double beta;
    double ic;
    double rank_ic;

    MicroSignalModel(const json& artifact){
        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<uint64_t>();
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
    uint64_t horizon_ms;

    BoosterHandle booster;
    vector<std_string> feature_cols;
    int D;
    
    vector<float> x_input;
    DMatrixHandle dmat = nullptr;

    ~ResidualModel(){
        if(dmat) XGDMatrixFree(dmat);
    }

    ResidualModel(const json& artifact){

        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<uint64_t>();
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
    uint64_t horizon_ms;

    BoosterHandle booster;
    vector<std_string> feature_cols;
    int D;

    vector<float> x_input;
    DMatrixHandle dmat = nullptr;

    ~ToxicityModel(){
        if(dmat) XGDMatrixFree(dmat);
    }

    ToxicityModel(const json& artifact){

        model_name = artifact["model_name"].get<std_string>();
        target = artifact["target"].get<std_string>();
        horizon_ms = artifact["horizon_ms"].get<uint64_t>();
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
    const json& params;
    double gamma;
    std_string folder_path;

    // Models
    std_string struct_model;
    unique_ptr<RegimeModel> regime_model;
    unique_ptr<MicroSignalModel> micro_signal_model;
    unique_ptr<ResidualModel> residual_model;
    unique_ptr<ToxicityModel> toxicity_model;

    MarketMakingStrategy(MarketConfig& config, const json& params)
        : config(config), params(params){
        gamma = params["gamma"].get<double>();
        folder_path = params["folder_path"].get<std_string>();

        struct_model = params["models"]["struct_model"].get<std_string>();
        regime_model       = load_model<RegimeModel>("regime_model");
        micro_signal_model = load_model<MicroSignalModel>("micro_signal_model");
        residual_model     = load_model<ResidualModel>("residual_model");
        toxicity_model     = load_model<ToxicityModel>("toxicity_model");
    }

    template<typename T> unique_ptr<T> load_model(const std_string& model){
        if(params["models"][model].get<std_string>().empty()) return nullptr;

        std_string file = folder_path + "/" + params["models"][model].get<std_string>() + ".json";
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
        
        return -effective_inventory * gamma * sigma * mid;
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
    }

    double compute_struct_delta(const Features& features, const Policy& policy){
        
        if(struct_model != "blended_AS") return 0.0;

        // Move my current mid price toward the structural fair so value by some fraction.
        double reservation = features.fair + features.skew;
        double struct_delta = policy.alpha_struct * (reservation - features.mid);

        return struct_delta;
    }

    double compute_micro_signal_delta(const Features& features){

        if(micro_signal_model == nullptr) return 0.0;

        return micro_signal_model->predict(features);
    }

    pair<double, double> compute_residual_delta(State& state, double struct_delta, double micro_signal_delta, 
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
    Signal generate_quotes(State& state){

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
        features.queue_ahead_bid = state.compute_queue_ahead("bids", config.to_tick(best_bid));
        features.queue_ahead_ask = state.compute_queue_ahead("asks", config.to_tick(best_ask));

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

        bid = config.round_price(bid);
        ask = config.round_price(ask);

        double bid_delta = abs(best_bid - state.mfs.prev_best_bid);
        double ask_delta = abs(best_ask - state.mfs.prev_best_ask);

        Signal signal;
        signal.ts = state.last_depth_ts;
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
    const json& params;
    OrderBook market_book;
    MarketFeatureState mfs;
    optional<Signal> last_signal;

    double last_mid = 0.0;
    double order_imbalance = 0.0;
    double trade_imbalance = 0.0;
    double ewma_var = 0.0;

    double inventory = 0.0;
    double cash;
    double initial_cash;
    double realized_pnl = 0.0;
    double avg_entry_price = 0.0;

    deque<double> equity_history;
    deque<double> return_history;
    double last_equity = 0.0;

    double maker_fee_rate;
    double taker_fee_rate;
    double fees_paid = 0.0;

    unordered_map<std_string, unordered_map<int64_t, double>> queue_flow;
    unordered_map<std_string, unordered_map<int64_t, double>> my_queue_position;
    unordered_map<std_string, unordered_map<int64_t, deque<Trade>>> trade_buckets;
    uint64_t trade_flow_window_ms = 1000; // for trade flow buckets

    double dt = 0.0;
    uint64_t last_fill_match_ts = 0;

    optional<Trade> last_trade;
    uint64_t last_trade_ts = 0;
    uint64_t last_depth_ts = 0;
    optional<Order> last_fill_candidate;
    optional<Order> last_order_update;

    bool initialized = false;

    State(MarketConfig& config, const json& params)
        : config(config), params(params), market_book(config, params){
            cash = params["cash"].get<double>();
            initial_cash = params["initial_cash"].get<double>();
            maker_fee_rate = params["fees"]["maker_fee_rate"].get<double>();
            taker_fee_rate = params["fees"]["taker_fee_rate"].get<double>();
        }

    double get_vol(){
        return sqrt(ewma_var);
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

    void on_fill(const double& price, double fill_qty, const std_string& side, const std_string& liquidity){

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
        double fee_rate = (liquidity == "maker") ? maker_fee_rate : taker_fee_rate;
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
        return inventory * (mid - avg_entry_price);
    }

    double get_pnl(double mid){
        return realized_pnl + get_unrealized_pnl(mid) - fees_paid;
    }

    double get_value(auto& m, const std_string& side, const int64_t& price_tick){
        auto it1 = m.find(side);
        if(it1 == m.end()) return 0.0;

        auto it2 = it1->second.find(price_tick);
        if(it2 == it1->second.end()) return 0.0;

        return it2->second;
    }

    double compute_queue_ahead(const std_string& side, const int64_t& price_tick){
        double my_queue_pos = get_value(my_queue_position, side, price_tick);
        double flow   = get_value(queue_flow, side, price_tick);

        double ahead = my_queue_pos - flow;
        return max(0.0, ahead);
    }

    void reset_queue_ahead(const std_string& side, const int64_t& price_tick){
        my_queue_position[side].erase(price_tick);
        queue_flow[side].erase(price_tick);
    }

    void update_queue_from_depth(const Depth& entry){

        for(auto& [price_tick, q]: entry.bid_delta){
            
            auto it = market_book.bids.find(price_tick);
            double old_qty = (it != market_book.bids.end()) ? it->second : 0.0;
            double new_qty = q;

            if(old_qty > 0.0){
                double depletion = max(0.0, old_qty - new_qty);
                queue_flow["bids"][price_tick] += depletion;
            }
        }

        for(auto& [price_tick, q]: entry.ask_delta){
            
            auto it = market_book.asks.find(price_tick);
            double old_qty = (it != market_book.asks.end()) ? it->second : 0.0;
            double new_qty = static_cast<double>(q);

            if(old_qty > 0.0){
                double depletion = max(0.0, old_qty - new_qty);
                queue_flow["asks"][price_tick] += depletion;
            }
        }
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
    //        uint64_t now = std::max(last_depth_ts, last_trade_ts);
    //     while(!mfs.toxicity_predictions.empty() && 
    //     now - mfs.toxicity_predictions.front().ts >= mfs.toxicity_predictions.front().horizon_ms){
    //         auto& entry = mfs.toxicity_predictions.front();
    //         mfs.toxicity_predictions.pop_front();

            
            
    //         entry.realized = p.fill_sign * (last_mid - entry.fill_price);
    //         push_limited(mfs.toxicity_signal_log, entry, 2000);
    //     }
    // }

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
    std_string instrument_upper;

    vector<SnapshotRow> snapshots;
    vector<TradeRow> trades;
    vector<QuoteRow> quotes;
    vector<FillRow> fills;
    
    vector<EventsRow> events;
    json orderbook_snapshot;

    std_string run_id;
    std_string folder_path;
    std_string events_path;
    std_string orderbook_snapshot_path;

    std_string snapshots_path;
    std_string trades_path;
    std_string quotes_path;
    std_string fills_path;

    static constexpr size_t SNAPSHOT_CHUNK = 10000;
    static constexpr size_t TRADE_CHUNK = 10000;
    static constexpr size_t QUOTE_CHUNK = 10000;
    static constexpr size_t FILL_CHUNK  = 5000;   // usually smaller

    int snapshots_id = 0;
    int trades_id = 0;
    int quotes_id = 0;
    int fills_id = 0;

    DatasetRecorder(MarketConfig& config, State& state, const json& params)
        : config(config), state(state), params(params) {initialize();}

    std_string build_file_path(const std_string& file, int file_id){
        ostringstream ss;
        ss << folder_path << '/' << file << '_' << setw(6) << setfill('0') << file_id << ".parquet";
        
        cout << file << "_file_path: " << ss.str() << "\n";
        return ss.str();
    }

    void initialize(){
        instrument_upper = params["instrument"].get<std_string>();
        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
        cout << "instrument_upper: " << instrument_upper << "\n";

        std_string current_run_path = params["folder_path"].get<std_string>();

        auto now = system_clock::now();
        time_t now_t = system_clock::to_time_t(now);

        tm tm{};
        gmtime_s(&tm, &now_t);

        ostringstream ss;
        ss << put_time(&tm, "%Y%m%d_%H%M%S");

        run_id = ss.str();
        folder_path = current_run_path + "/runs/run_" + run_id;
        filesystem::create_directories(folder_path);

        events_path = folder_path + "/" + params["files"]["replay_events"]["events"].get<std_string>();
        orderbook_snapshot_path = folder_path + "/" + params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();
        snapshots_path = build_file_path("snapshots", snapshots_id++);
        trades_path = build_file_path("trades", trades_id++);
        quotes_path = build_file_path("quotes", quotes_id++);
        fills_path = build_file_path("fills", fills_id++);
    }

    void log_event(const std_string& type, const uint64_t& ts, const std_string& msg){
        events.push_back(EventsRow{type, ts, msg});
    }

    void log_orderbook_snapshot(const json& snapshot){
        orderbook_snapshot = snapshot;
    }

    void export_event_parquet(){
        std_string filename = folder_path + "/" + params["files"]["replay_events"]["events"].get<std_string>();
        cout << "Exported events: " << filename << "\n";
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        StringBuilder type_b(pool);
        Int64Builder ts_b(pool);
        StringBuilder msg_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& e: events){
            type_b.Append(e.type);
            ts_b.Append(e.ts);
            msg_b.Append(e.msg);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> type_arr, ts_arr, msg_arr;

        type_b.Finish(&type_arr);
        ts_b.Finish(&ts_arr);
        msg_b.Finish(&msg_arr);
        
        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("type", utf8()),
            field("ts", int64()),
            field("msg", utf8())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {type_arr, ts_arr, msg_arr});

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(filename).Value(&out);
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1024);
    }

    void export_orderbook_snapshot(){
        ofstream(orderbook_snapshot_path) << orderbook_snapshot.dump(4);
    }

    void shutdown(){
        state.update_performance();

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

        // export events and orderbook snapshot
        export_event_parquet();
        export_orderbook_snapshot();

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
        row.symbol = instrument_upper;

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
        shared_ptr<Array> ts_arr, symbol_arr, mid_arr, mid_tick_arr;
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
            ts_arr, symbol_arr, mid_arr, mid_tick_arr,
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
        row.symbol = instrument_upper;

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
        row.symbol = instrument_upper;
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
        shared_ptr<Array> ts_arr, symbol_arr, client_oid_arr;
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
            ts_arr, symbol_arr, client_oid_arr,
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

    void log_fill(const double& qty, const Order& order, const bool& is_maker){
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
        row.symbol = instrument_upper;
        row.side = order.side;
        row.price = config.from_tick(order.price_tick);
        row.price_tick = order.price_tick;
        row.qty = qty;

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
        StringBuilder symbol_b(pool);
        StringBuilder side_b(pool);
        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);

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
            symbol_b.Append(r.symbol);
            side_b.Append(r.side);
            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);

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
        shared_ptr<Array> ts_arr, symbol_arr, side_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;
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
        symbol_b.Finish(&symbol_arr);
        side_b.Finish(&side_arr);
        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);

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
            field("symbol", utf8()),
            field("side", utf8()),
            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
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
            ts_arr, symbol_arr, side_arr,
            price_arr, price_tick_arr, qty_arr,
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
    std_string instrument;
    std_string instrument_upper;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    vector<Depth> depth_buffer;

    BinanceSpotFeed(MarketConfig& config, State& state,
                    function<void(const Trade&)> on_trade_event, function<void()> on_depth_event,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_trade_event(move(on_trade_event)),
                    on_depth_event(move(on_depth_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot)), 
                    instrument(params["instrument"].get<std_string>()),
                    instrument_upper(instrument) {
                        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
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

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binance.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binance.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binance.com", "/ws/" + instrument + "@trade");

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
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.ts = doc["T"];

            log_event("trade", trade.ts, msg);

            state.last_trade = trade;
            state.last_trade_ts = trade.ts;

            on_trade_event(trade);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binance.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binance.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binance.com", "/ws/" + instrument + "@depth@100ms");

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

            Depth entry;
            entry.ts = uint64_t(doc["E"]);
            entry.U = uint64_t(doc["U"]);
            entry.u = uint64_t(doc["u"]);
            parse_book(doc.get_object(), entry);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", entry.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(entry);
                    continue;
                }
            }
            
            // -------------------------
            // LIVE PROCESSING
            // -------------------------
            state.last_depth_ts = entry.ts;

            on_depth(entry);
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

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance(instrument_upper, 1000);

        log_orderbook_snapshot(snapshot);

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

    void on_depth(const Depth& entry){

        auto& book = state.market_book;
        // cout << "entry.u: " << entry.u << " entry.ts: " << entry.ts << " entry.U: " << entry.U << '\n';
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(entry.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << entry.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry);
            book.apply_delta(entry);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        if(state.initialized) on_depth_event();
    }
};

class BinanceFuturesFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    std_string instrument;
    std_string instrument_upper;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    vector<Depth> depth_buffer;

    BinanceFuturesFeed(MarketConfig& config, State& state,
                    function<void(const Trade&)> on_trade_event, function<void()> on_depth_event,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_trade_event(move(on_trade_event)),
                    on_depth_event(move(on_depth_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot)), 
                    instrument(params["instrument"].get<std_string>()),
                    instrument_upper(instrument) {
                        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
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

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binancefuture.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binancefuture.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binancefuture.com", "/ws/" + instrument + "@trade");

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
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.ts = doc["T"];

            log_event("trade", trade.ts, msg);

            state.last_trade = trade;
            state.last_trade_ts = trade.ts;

            on_trade_event(trade);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binancefuture.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binancefuture.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binancefuture.com", "/ws/" + instrument + "@depth@100ms");

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

            Depth entry;
            entry.ts = uint64_t(doc["E"]);
            entry.pu = uint64_t(doc["pu"]);
            entry.U = uint64_t(doc["U"]);
            entry.u = uint64_t(doc["u"]);
            parse_book(doc.get_object(), entry);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", entry.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(entry);
                    continue;
                }
            }
            
            // -------------------------
            // LIVE PROCESSING
            // -------------------------
            state.last_depth_ts = entry.ts;

            on_depth(entry);
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

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance(instrument_upper, 1000);

        log_orderbook_snapshot(snapshot);

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

    void on_depth(const Depth& entry){
        
        auto& book = state.market_book;
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        if(entry.pu != book.last_update_id) {
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << entry.pu << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry);
            book.apply_delta(entry);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        if(state.initialized) on_depth_event();
    }
};

class BinanceSpotReplayFeed : public Feed {
public:
    State& state;
    MarketConfig& config;
    std_string events_path;
    std_string orderbook_snapshot_path;
    vector<EventsRow> events;
    json orderbook_snapshot;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;
    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0;
    optional<uint64_t> last_ts;
    double speed_multiplier = 5.0;

    simdjson::ondemand::parser parser;
    
    BinanceSpotReplayFeed(MarketConfig& config, State& state,
                    function<void(const Trade&)> on_trade_event, function<void()> on_depth_event,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_trade_event(move(on_trade_event)),
                    on_depth_event(move(on_depth_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))

    {
        events_path   = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["events"].get<std_string>();

        orderbook_snapshot_path = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();

        initialize();
    }

    void initialize(){
        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

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

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("message")->chunk(0)
            // table->GetColumnByName("msg")->chunk(0) // TO BE CHANGED ONCE LOG EVENT IS SETTLED
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventsRow e;
            e.type = type_col->GetString(i);
            e.ts = static_cast<uint64_t>(ts_col->Value(i));
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

    void on_trade_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.ts    = doc["T"];

        log_event("trade", trade.ts, msg);

        state.last_trade = trade;
        state.last_trade_ts = trade.ts;

        on_trade_event(trade);
    }

    void on_depth_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Depth entry;
        entry.ts = uint64_t(doc["E"]);
        entry.U  = uint64_t(doc["U"]);
        entry.u  = uint64_t(doc["u"]);
        parse_book(doc.get_object(), entry);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", entry.ts, msg);
        
        if(!snapshot_aligned.load()){
            if(entry.U <= state.market_book.last_update_id + 1 && state.market_book.last_update_id + 1 <= entry.u){
                cout << "BUFFER U: " << entry.U << " snapshot_id + 1: " << state.market_book.last_update_id + 1 << " u: " << entry.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(entry);
                state.market_book.last_update_id = entry.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.last_depth_ts = entry.ts;

        on_depth(entry);
    }

    void run(){
        while(running && i < events.size()){
            auto& event = events[i];

            if(last_ts.has_value()){
                double dt = (event.ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = event.ts;

            if(event.type == "depth") on_depth_message(event.msg);
            else if(event.type == "trade") on_trade_message(event.msg);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY SPOT SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        log_orderbook_snapshot(snapshot);

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

    void on_depth(const Depth& entry){

        auto& book = state.market_book;
        // cout << " entry.U: " << entry.U << " state.market_book.last_update_id: " << state.market_book.last_update_id << " entry.u: " << entry.u << '\n';
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(entry.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << entry.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry);
            book.apply_delta(entry);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        if(state.initialized) on_depth_event();
    }
};

class BinanceFuturesReplayFeed : public Feed {
public:
    State& state;
    MarketConfig& config;
    std_string events_path;
    std_string orderbook_snapshot_path;
    vector<EventsRow> events;
    json orderbook_snapshot;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;
    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0;
    optional<uint64_t> last_ts;
    double speed_multiplier = 5.0;

    simdjson::ondemand::parser parser;
    
    BinanceFuturesReplayFeed(MarketConfig& config, State& state,
                    function<void(const Trade&)> on_trade_event, function<void()> on_depth_event,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_depth_event(move(on_depth_event)),
                    on_trade_event(move(on_trade_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))

    {
        events_path   = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["events"].get<std_string>();

        orderbook_snapshot_path = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();

        initialize();
    }

    void initialize(){
        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

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

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("message")->chunk(0)
            // table->GetColumnByName("msg")->chunk(0) // TO BE CHANGED ONCE LOG EVENT IS SETTLED
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventsRow e;
            e.type = type_col->GetString(i);
            e.ts = static_cast<uint64_t>(ts_col->Value(i));
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

    void on_trade_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.ts    = doc["T"];

        log_event("trade", trade.ts, msg);

        state.last_trade = trade;
        state.last_trade_ts = trade.ts;

        on_trade_event(trade);
    }

    void on_depth_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Depth entry;
        entry.ts = uint64_t(doc["E"]);
        entry.U  = uint64_t(doc["U"]);
        entry.u  = uint64_t(doc["u"]);
        parse_book(doc.get_object(), entry);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", entry.ts, msg);
        
        if(!snapshot_aligned.load()){
            if(entry.U <= state.market_book.last_update_id && state.market_book.last_update_id <= entry.u){
                cout << "BUFFER U: " << entry.U << " snapshot_id: " << state.market_book.last_update_id << " u: " << entry.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(entry);
                state.market_book.last_update_id = entry.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.last_depth_ts = entry.ts;

        on_depth(entry);
    }

    void run(){
        while(running && i < events.size()){
            auto& event = events[i];

            if(last_ts.has_value()){
                double dt = (event.ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = event.ts;

            if(event.type == "depth") on_depth_message(event.msg);
            else if(event.type == "trade") on_trade_message(event.msg);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY FUTURES SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        log_orderbook_snapshot(snapshot);

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

    void on_depth(const Depth& entry){

        auto& book = state.market_book;
        // cout << " entry.U: " << entry.U << " state.market_book.last_update_id: " << state.market_book.last_update_id << " entry.u: " << entry.u << '\n';
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(entry.pu != book.last_update_id) {
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << entry.pu << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry);
            book.apply_delta(entry);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        if(state.initialized) on_depth_event();
    }
};

// class Execution {
// public:
//     virtual Order* get_open_order(const std_string&) = 0;
//     virtual Order* get_order(const std_string&) = 0;
//     virtual pair<double, double> compute_order_size(const Signal&) = 0;
//     virtual bool can_quote(const std_string&) = 0;
//     virtual void place_quotes(const Signal&) = 0;
//     virtual void cancel_all_orders() = 0;
//     virtual void process_trade(const Trade&) = 0;
//     virtual void update_trade_flow(const Trade&) = 0;
//     virtual void update_trade_flow_buckets(const Trade&) = 0;

//     virtual std_string new_order_id() = 0;
//     virtual void process_place_quotes(const Signal&) = 0;
//     virtual void process_latency_queue() = 0;
//     virtual Order* place_limit(const std_string&, const double&, const double&, const uint64_t&, const Signal&) = 0;
//     virtual void cancel_side(const std_string&) = 0;
//     virtual void place_market() = 0;
//     virtual double get_trade_rate(const Trade&) = 0;
//     virtual void match_side(const Trade&) = 0;
//     virtual void execute_market(Order&) = 0;

//     virtual ~Execution() = default;
// };

class Execution {
public:
    virtual double get_last_bid() = 0;
    virtual double get_last_ask() = 0;
    virtual double get_current_bid_size() = 0;
    virtual double get_current_ask_size() = 0;
    virtual void place_quotes_latency(const Signal&) = 0;
    virtual void process_latency_queue() = 0;
    virtual void process_trade(const Trade&) = 0;
    virtual Order* get_open_order(const std_string&) = 0;
    virtual void cancel_all_orders() = 0;
    virtual void place_quotes(const Signal&) = 0;
    virtual void place_market() = 0;
    virtual ~Execution() = default;
};

// 1. Binance Broker (REAL API layer) 100–2000ms variable
class BinanceBroker {
public:
    MarketConfig& config;

    std_string api_key;
    std_string api_secret;
    std_string base_url;
    std_string instrument;

    std_string listen_key;
    atomic<bool> keepalive_running{false};

    BinanceBroker(MarketConfig& config, const json& params)
        : config(config)
    {
        api_key = params["api"]["api_key"];
        api_secret = params["api"]["api_secret"];
        base_url = params["api"]["base_url"];
        instrument = params["instrument"];
    }

    // -------------------------
    // HTTP helpers (libcurl assumed)
    // -------------------------
    static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp){
        size_t total = size * nmemb;
        ((std_string*)userp)->append((char*)contents,total);
        return total;
    }

    std_string http_request(const std_string& url, const std_string& method, 
                        const vector<std_string>& headers = {}, const std_string& body = ""){
        CURL* curl = curl_easy_init();

        if(!curl) throw runtime_error("curl init failed");

        std_string response;

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        // timeouts
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);

        // headers
        curl_slist* header_list = nullptr;

        for(const auto& h: headers){
            header_list = curl_slist_append(header_list, h.c_str());
        }

        if(header_list) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);

        // method
        if(method == "POST"){
            curl_easy_setopt(curl, CURLOPT_POST, 1L);

            if(!body.empty()) curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        }
        else if(method == "PUT"){
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");

            if(!body.empty()) curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        }
        else if(method == "DELETE") curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        else throw runtime_error("unsupported method: " + method);

        CURLcode res = curl_easy_perform(curl);

        if(res != CURLE_OK){
            std_string err = curl_easy_strerror(res);

            curl_slist_free_all(header_list);
            curl_easy_cleanup(curl);

            throw runtime_error(err);
        }

        long status;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

        curl_slist_free_all(header_list);
        curl_easy_cleanup(curl);

        if(status >= 400) throw runtime_error("HTTP " + to_string(status) + ": " + response);

        return response;
    }

    // -------------------------
    // USER STREAM
    // -------------------------
    std_string start_user_stream(){
        std_string url = base_url + "/fapi/v1/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};

        auto res = http_request(url, "POST", headers);
        auto j = json::parse(res);

        listen_key = j["listenKey"];

        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){
        std_string url = base_url + "/fapi/v1/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};

        http_request(url + "?listenKey=" + listen_key, "PUT", headers);
    }

    void start_keepalive_loop(){
        keepalive_running = true;

        thread([this](){
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
        }).detach();
    }

    void stop_keepalive() {
        keepalive_running = false;
    }

    // -------------------------
    // SIGNING
    // -------------------------
    std_string sign(const std_string& query){
        unsigned char* digest;
        digest = HMAC(EVP_sha256(),
                      api_secret.c_str(),
                      api_secret.size(),
                      (unsigned char*)query.c_str(),
                      query.size(),
                      NULL, NULL);

        char mdString[65];
        for(int i = 0; i < 32; i++)
            sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);

        return std_string(mdString);
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    json place_limit(const std_string& side, const double& price, 
                    const double& size, const std_string& client_oid, const uint64_t& ts){
        
        ostringstream q;
        q << "newClientOrderId=" << client_oid
          << "&symbol=" << instrument
          << "&side=" << side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(4) << size
          << "&price=" << fixed << setprecision(1) << price
          << "&timestamp=" << ts
          << "&recvWindow=5000";
        
        // q << "newClientOrderId=" << client_oid
        //   << "&symbol=" << instrument
        //   << "&side=" << side
        //   << "&type=LIMIT"
        //   << "&timeInForce=GTC"
        //   << "&quantity=" << fixed << setprecision(qty_precision) << size
        //   << "&price=" << fixed << setprecision(price_precision) << price
        //   << "&timestamp=" << ts
        //   << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res = http_request(url, "POST", headers);
        
        return json::parse(res);
    }

    json cancel_order(const Order& order, const uint64_t& ts){
        
        ostringstream q;
        q << "origClientOrderId=" << order.order_id
          << "&symbol=" << instrument << "&timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res = http_request(url, "DELETE", headers);
        
        return json::parse(res);
    }

    json cancel_all_orders(){
        uint64_t ts = config.now_ms();

        ostringstream q;
        q << "symbol=" << instrument << "&timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/allOpenOrders?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res = http_request(url, "DELETE", headers);
        
        return json::parse(res);
    }

    json place_market(const std_string& client_oid, const double& pos, const std_string& side, const uint64_t& ts){

        ostringstream q;
        q << "newClientOrderId=" << client_oid
          << "&symbol=" << instrument
          << "&side=" << side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(4) << abs(pos)
          << "&reduceOnly=true"
          << "&timestamp=" << ts
          << "&recvWindow=5000";

        // q << "newClientOrderId=" << client_oid
        //   << "&symbol=" << instrument
        //   << "&side=" << side
        //   << "&type=MARKET"
        //   << "&quantity=" << fixed << setprecision(qty_precision) << abs(pos)
        //   << "&reduceOnly=true"
        //   << "&timestamp=" << ts
        //   << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res = http_request(url, "POST", headers);
        
        return json::parse(res);
    }

    double get_position(){
        uint64_t ts = config.now_ms();

        ostringstream q;
        q << "timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v2/positionRisk?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res = http_request(url, "GET", headers);
        
        auto arr = json::parse(res);
        for(auto& p: arr){
            if(p["symbol"] == instrument)
                return stod(p["positionAmt"].get<std_string>());
        }

        return 0.0;
    }
};

class LiveExecution : public Execution {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    BinanceBroker& broker;

    unordered_map<std_string, Order> open_orders;

    double last_bid = 0.0;
    double last_ask = 0.0;
    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double base_size;
    double max_inv;

    mutex orders_mtx;
    DatasetRecorder& recorder;

    LiveExecution(MarketConfig& config, State& state, BinanceBroker& broker,
                  DatasetRecorder& recorder, const json& params)
        : config(config), state(state), broker(broker), recorder(recorder), params(params)
    {
        base_size = params["base_size"];
        max_inv = params["max_inv"];
    }

    Order* get_open_order(const std_string& side) override {
        for(auto& [id, o]: open_orders) if(o.side == side) return &o;
        return nullptr;
    }

    Order* get_order(const std_string& client_oid) override {
        auto it = open_orders.find(client_oid);
        if(it == open_orders.end()) return nullptr;
        return &it->second;
    }
    
    pair<double, double> compute_order_size(const Signal& signal) override {
        double inv = state.inventory;
        double vol = state.get_vol();

        double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

        double inv_scale = 5.0;
        double inv_signal = tanh(inv / inv_scale);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        double risk_penalty = exp(-0.2 * inv * inv);

        double toxicity_penalty = exp(-signal.toxicity.k2 * signal.toxicity.tox);

        double base = base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        return {
            size * bid_multiplier,
            size * ask_multiplier
        };
    }

    // -------------------------
    // RISK GUARD
    // -------------------------
    bool can_quote(const std_string& side) override {
        double inv = state.inventory;

        if(abs(inv) >= max_inv){
            if(side == "BUY" && inv < 0) return true;
            if(side == "SELL" && inv > 0) return true;
            return false;
        }
        return true;
    }

    void place_quotes(const Signal& signal) override {
        
        double desired_bid = signal.my_bid;
        double desired_ask = signal.my_ask;

        double tick = config.tick_size;

        auto [bid_size, ask_size] = compute_order_size(signal);
        uint64_t ts = config.now_ms();

        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        bool bid_change = !bid_order || abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick;
        bool ask_change = !ask_order || abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick;

        current_bid_size = bid_size;
        current_ask_size = ask_size;

        // ---------------- BID ----------------
        if(!bid_order && can_quote("BUY")){
            std_string client_oid = "MM-" + uuid16();

            Order order;
            order.order_id = client_oid;
            order.side = "BUY";
            order.price_tick = config.to_tick(desired_bid);
            order.qty = bid_size;
            order.remaining = bid_size;
            order.status = "PENDING_NEW";
            order.ts = ts;
            order.owner = "self";
            order.signal = signal;

            {
                lock_guard<mutex> lock(orders_mtx);
                open_orders[client_oid] = order;
            }

            open_orders[client_oid].resp = broker.place_limit("BUY", desired_bid, bid_size, client_oid, ts);

            recorder.log_quote(ts, order, "BID", "NEW_SUBMITTED", desired_bid);
        }
        else if(bid_change && bid_order->status != "PENDING_NEW" && bid_order->status != "PENDING_CANCEL"){
            {
                lock_guard<mutex> lock(orders_mtx);
                bid_order->status = "PENDING_CANCEL";
            }

            bid_order->resp = broker.cancel_order(*bid_order, ts);
            recorder.log_quote(ts, *bid_order, "BID", "CANCEL_SUBMITTED", desired_bid);
        }

        // ---------------- ASK (same pattern) ----------------
        if(!ask_order && can_quote("SELL")){
            std_string client_oid = "MM-" + uuid16();

            Order order;
            order.order_id = client_oid;
            order.side = "SELL";
            order.price_tick = config.to_tick(desired_ask);
            order.qty = ask_size;
            order.remaining = ask_size;
            order.status = "PENDING_NEW";
            order.ts = ts;
            order.owner = "self";
            order.signal = signal;

            {
                lock_guard<mutex> lock(orders_mtx);
                open_orders[client_oid] = order;
            }

            open_orders[client_oid].resp = broker.place_limit("SELL", desired_ask, ask_size, client_oid, ts);

            recorder.log_quote(ts, order, "ASK", "NEW_SUBMITTED", desired_ask);
        }
        else if(ask_change && ask_order->status != "PENDING_NEW" && ask_order->status != "PENDING_CANCEL"){
            {
                lock_guard<mutex> lock(orders_mtx);
                ask_order->status = "PENDING_CANCEL";
            }

            ask_order->resp = broker.cancel_order(*ask_order, ts);

            recorder.log_quote(ts, *ask_order, "ASK", "CANCEL_SUBMITTED", desired_ask);
        }
    }

    void cancel_all_orders() override {
        broker.cancel_all_orders();
    }

    void place_market() override {

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        uint64_t ts = config.now_ms();
        std_string client_oid = "MM-" + uuid16();
        double pos = broker.get_position();
        std_string side = (pos > 0) ? "SELL" : "BUY";
        double price = (pos > 0) ? bid_tick : ask_tick;

        Order order;
        order.order_id = client_oid;
        order.side = side;
        order.price_tick = config.to_tick(price);
        order.qty = abs(pos);
        order.remaining = abs(pos);
        order.status = "PENDING_NEW";
        order.ts = ts;
        order.owner = "self";
        order.signal = state.last_signal;
        order.queue_ahead_at_join = 0.0;

        {
            lock_guard<mutex> lock(orders_mtx);
            open_orders[client_oid] = order;
        }

        open_orders[client_oid].resp = broker.place_market(client_oid, pos, side, ts);
    }

    void process_trade(const Trade& trade) override {

        update_trade_flow_buckets(trade);
        update_trade_flow(trade);

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price = config.to_tick(trade.price);

        Order* order = nullptr;
        for(auto& [id, o]: open_orders){
            if(o.side == side && o.price_tick == price && o.status == "LIVE"){
                order = &o;
                break;
            }
        }
        if(!order) return;

        state.last_fill_candidate = *order;
    }

    void update_trade_flow(const Trade& trade) override {
        
        // double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double flow = (trade.side == "BUY" ? trade.qty : -trade.qty);
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    void update_trade_flow_buckets(const Trade& trade) override {

        int64_t price = config.to_tick(trade.price);

        Trade trade_event;
        trade_event.side = trade.side;
        trade_event.price = price;
        trade_event.size = trade.qty;
        trade_event.timestamp = trade.timestamp;
        
        auto& bucket = state.trade_buckets[trade.side][price];
        bucket.push_back(trade_event);

        while(!bucket.empty() && trade_event.timestamp - bucket.front().timestamp > state.window_ms){
            bucket.pop_front();
        }
    }
};

class BinanceUserStream {
public:
    State& state;
    LiveExecution& execution;
    BinanceBroker& broker;
    DatasetRecorder& recorder;

    atomic<bool> connected{false};

    BinanceUserStream(MarketConfig& config, State& state, LiveExecution& execution,
                      BinanceBroker& broker, DatasetRecorder& recorder)
        : state(state), execution(execution), broker(broker), recorder(recorder) {}

    void start(){
        std_string listen_key = broker.start_user_stream();
        std_string url = "wss://stream.binancefuture.com/ws/" + listen_key;

        websocketpp::client<websocketpp::config::asio_client> client;

        client.init_asio();
        client.set_message_handler([this](auto hdl, auto msg){
            on_message(msg->get_payload());
        });

        websocketpp::lib::error_code ec;
        auto conn = client.get_connection(url, ec);

        client.connect(conn);

        thread([&]() {
            client.run();
        }).detach();

        connected = true;
    }

    void UserStream::on_order_update(const OrderUpdate& update) {
        engine->handle_order_update(update);
    }

    void on_message(const std_string& msg){
        auto data = json::parse(msg);

        if(data["e"] != "ORDER_TRADE_UPDATE")
            return;

        auto o = data["o"];

        std_string client_oid = o["c"];
        std_string side = o["S"];
        std_string exec_type = o["x"];
        std_string status = o["X"];
        uint64_t ts = o["T"];

        if(order->broker_sent_ts > 0)
        {
            order->exchange_new_ts = ts;
            order->ack_latency_ms = ts - order->broker_sent_ts;

            execution.record_latency(order->ack_latency_ms);
        }

        Order* order = execution.get_order(client_oid);

        if(!order) return;

        // ---------------- NEW ----------------
        if(exec_type == "NEW"){
            order->status = "LIVE";
            double price = stod(o["p"].get<std_string>());
            double book_size;
            
            if(side == "BUY"){
                execution.last_bid = price;

                auto it = state.market_book.bids.find(order->price_tick);
                book_size = (it != state.market_book.bids.end()) ? it->second : 0.0;
            }
            else if(side == "SELL"){
                execution.last_ask = price;
                
                auto it = state.market_book.asks.find(order->price_tick);
                book_size = (it != state.market_book.asks.end()) ? it->second : 0.0;
            }

            state.last_order_update = order;
            state.my_queue_position[(side == "BUY") ? "bids" : "asks"][order->price_tick] = book_size;
            order->queue_ahead_at_join = book_size;

            recorder.log_quote(ts, *order, (side == "BUY") ? "BID" : "ASK", "NEW", price);
        }

        // ---------------- CANCELLED ----------------
        else if(exec_type == "CANCELED"){
            order->status = "CANCELED";

            state.last_order_update = order;
            state.reset_queue_ahead((side == "BUY") ? "bids" : "asks", order->price_tick);
            execution.open_orders.erase(client_oid);

            recorder.log_quote(ts, *order, (side == "BUY") ? "BID" : "ASK", "CANCELED", 0.0);
        }

        else if(exec_type == "REJECTED"){
            order->status = "REJECTED";

            state.last_order_update = order;
            execution.open_orders.erase(client_oid);

            recorder.log_quote(ts, *order, (side == "BUY") ? "BID" : "ASK", "REJECTED", 0.0);
        }

        // ---------------- TRADE ----------------
        else if(exec_type == "TRADE"){

            // if (toxicity_model) {
            //     ToxicityPrediction p;

            //     p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
            //     p.horizon_ms = toxicity_model->horizon_ms;

            //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
            //     p.fill_price = fill_price;

            //     p.side = (side == "BUY") ? +1 : -1;

            //     state.market_feature_state.toxicity_predictions.push_back(std::move(p));
            // }

            double fill_price = stod(o["L"].get<std_string>());
            double fill_qty = stod(o["l"].get<std_string>());

            if(status == "PARTIALLY_FILLED"){
                order->remaining -= fill_qty;
            }

            if(status == "FILLED"){
                order->status = "FILLED";
                order->remaining = 0.0;

                state.reset_queue_ahead((side == "BUY") ? "bids" : "asks", order->price_tick);
                execution.open_orders.erase(client_oid);
            }

            state.on_fill(fill_price, fill_qty, side, "maker");
            state.last_order_update = order;
            recorder.log_fill(fill_qty, *order, true);

            std_string order_type = o["o"];
            if(order_type == "MARKET"){
                cout << format("{:<5} | {:>10.4f} | {:>8.6f} [{}]",  order->side, fill_price, fill_qty, order->status) << "\n";
            }
        }
    }
};

class PaperExecution : public Execution {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    DatasetRecorder& recorder;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    double base_size;
    double max_inv;

    mt19937 rng;
    uniform_real_distribution<double> dist;
    mutex orders_mtx;

    uint64_t latency_ms;
    priority_queue<LatencyEvent, vector<LatencyEvent>, Compare> latency_queue;
    mutex latency_mtx;

    PaperExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, const json& params)
        : config(config), state(state), recorder(recorder), params(params)
    {
        base_size = params["base_size"].get<double>();
        max_inv = params["max_inv"].get<double>();
        latency_ms = params["latency_ms"].get<uint64_t>();
        
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
    // latency simulation
    // -------------------------
    void place_quotes_latency(const Signal& signal) override {
        lock_guard<mutex> lock(latency_mtx);

        LatencyEvent event;
        event.execute_ts = config.now_ms() + latency_ms;
        event.type = "PLACE_QUOTES";
        event.signal = signal;

        latency_queue.push(event);
    }

    void process_latency_queue() override {
        lock_guard<mutex> lock(latency_mtx);

        while(!latency_queue.empty()){
            auto event = latency_queue.top();
            if(config.now_ms() < event.execute_ts) break;

            latency_queue.pop();
            place_quotes(event.signal);
        }
    }

    void process_trade(const Trade& trade) override {

        update_trade_flow_buckets(trade);
        update_trade_flow(trade);
        match_side(trade);
    }

    void update_trade_flow_buckets(const Trade& trade){

        int64_t price_tick = config.to_tick(trade.price);

        auto& bucket = state.trade_buckets[trade.side][price_tick];
        bucket.push_back(trade);

        while(!bucket.empty() && trade.ts - bucket.front().ts > state.trade_flow_window_ms){
            bucket.pop_front();
        }
    }

    void update_trade_flow(const Trade& trade){
        
        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    double get_trade_rate(const Trade& trade){

        int64_t price_tick = config.to_tick(trade.price);

        auto& bucket = state.trade_buckets[trade.side][price_tick]; //guaranteed to be pruned at update_trade_flow_buckets
        double volume = 0.0;

        for(auto& t: bucket) volume += t.qty;

        return volume / (state.trade_flow_window_ms / 1000.0); // per second (1000ms)
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

        double base = base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        return {
            size * bid_multiplier,
            size * ask_multiplier
        };
    }

    Order* get_open_order(const std_string& side) override {
        for(auto& [client_oid, order]: open_orders) if(order.side == side) return &order;
        return nullptr;
    }

    void cancel_side(const std_string& side){
        lock_guard<mutex> lock(orders_mtx);

        for(auto it = open_orders.begin(); it != open_orders.end();){
            if(it->second.side == side){
                it->second.status = "CANCELED";
                state.last_order_update = it->second;
                
                it = open_orders.erase(it);
            }
            else ++it;
        }
    }

    void cancel_all_orders() override {
        cancel_side("BUY");
        cancel_side("SELL");
    }

    bool can_quote(const std_string& side){
        double inv = state.inventory;

        if(abs(inv) >= max_inv){
            if(side == "BUY" && inv < 0) return true;
            if(side == "SELL" && inv > 0) return true;
            return false;
        }
        return true;
    }

    std_string uuid16(){
        auto u = boost::uuids::random_generator()();
        std_string s = boost::uuids::to_string(u);

        s.erase(remove(s.begin(), s.end(), '-'), s.end());
        return s.substr(0, 16);
    }

    Order* place_limit(const std_string& side, const double& price, const double& size, const Signal& signal){

        lock_guard<mutex> lock(orders_mtx);
        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = config.to_tick(price);

        double book_size = 0.0;
        if(side == "BUY"){
            auto it = state.market_book.bids.find(price_tick);
            if(it != state.market_book.bids.end()) book_size = it->second;
        }
        else{
            auto it = state.market_book.asks.find(price_tick);
            if(it != state.market_book.asks.end()) book_size = it->second;
        }
        state.my_queue_position[(side == "BUY") ? "bids" : "asks"][price_tick] = book_size;

        Order& order = open_orders[client_oid];
        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = size;
        order.remaining = size;
        order.status = "LIVE";
        order.ts = config.now_ms();
        order.owner = "self";
        order.signal = signal;
        order.queue_ahead_at_join = book_size;

        return &order; //return pointer, in case order creation fails and returns nullptr
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

        if(!bid_order && can_quote("BUY")){ // if no bid order
            Order* new_bid_order = place_limit("BUY", desired_bid, bid_size, signal);
            last_bid = desired_bid;
            recorder.log_quote(*new_bid_order, "BID", "NEW");
        }

        else if(abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick){ // if bid change
            state.reset_queue_ahead("bids", bid_order->price_tick);
            cancel_side("BUY");

            if(can_quote("BUY")){
                Order* new_bid_order = place_limit("BUY", desired_bid, bid_size, signal);
                last_bid = desired_bid;
                recorder.log_quote(*new_bid_order, "BID", "REPLACE");
            }
        }

        if(!ask_order && can_quote("SELL")){ // if no ask order
            Order* new_ask_order = place_limit("SELL", desired_ask, ask_size, signal);
            last_ask = desired_ask;
            recorder.log_quote(*new_ask_order, "ASK", "NEW");
        }

        else if(abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick){ // else if ask change
            state.reset_queue_ahead("asks", ask_order->price_tick);
            cancel_side("SELL");

            if(can_quote("SELL")){
                Order* new_ask_order = place_limit("SELL", desired_ask, ask_size, signal);
                last_ask = desired_ask;
                recorder.log_quote(*new_ask_order, "ASK", "REPLACE");
            }
        }
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick && order.status == "LIVE") return &order;
        }
        return nullptr;
    }

    void match_side(const Trade& trade){

        lock_guard<mutex> lock(orders_mtx);

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);
        double qty = trade.qty;

        // -------------------------
        // GET dt (time interval between trades) -> amount of time you allow those events to happen
        // -------------------------
        if(state.last_fill_match_ts != 0){
            state.dt = (trade.ts - state.last_fill_match_ts) / 1000.0; // dt (s) represents interval between last_fill_model_ts and trade ts
        }
        state.last_fill_match_ts = trade.ts;

        // -------------------------
        // FILL CANDIDATE
        // -------------------------
        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;

        // -------------------------
        // QUEUE + FLOW MODEL
        // -------------------------
        double queue_ahead = state.compute_queue_ahead((side == "BUY") ? "bids" : "asks", price_tick);
        double trade_rate = get_trade_rate(trade);

        double lambda_fill = trade_rate / (queue_ahead + 1e-9); // expected fill events per second
        double p_fill = 1.0 - exp(-lambda_fill * state.dt); // probability that at least one fill event happens during that time
        // double p_fill = 1.0 - exp(-lambda_fill * 0.1); // dt = 100ms

        // increase p_fill
        // 1. Higher trade_rate
        // More aggressive market taking liquidity
        // More volume consumes the queue ahead of you
        // Increases lambda_fill
        // 2. Lower queue_ahead
        // Less volume in front of your order
        // Easier for trades to reach you
        // Increases lambda_fill
        // 3. Longer dt
        // More time for the process to act
        // Increases the chance that the queue gets consumed

        // stochastic acceptance
        if(p_fill < dist(rng)) return;

        // -------------------------
        // APPLY FILL
        // -------------------------
        double fill_qty = min(order->remaining, qty);
        order->remaining -= fill_qty;

        state.on_fill(trade.price, fill_qty, order->side, "maker");
        state.last_order_update = *order;
        recorder.log_fill(fill_qty, *order, true);

        if(order->remaining == 0){
            order->status = "FILLED";
            open_orders.erase(order->client_oid);
            state.reset_queue_ahead((side == "BUY") ? "bids" : "asks", order->price_tick);
        }
    }

    void place_market() override {

        lock_guard<mutex> lock(orders_mtx);
        std_string client_oid = "MM-" + uuid16();

        Order& order = open_orders[client_oid];
        order.client_oid = client_oid;
        order.side = (state.inventory > 0) ? "SELL" : "BUY";
        order.price_tick = -1;
        order.qty = abs(state.inventory);
        order.remaining = abs(state.inventory);
        order.status = "LIVE";
        order.ts = config.now_ms();
        order.owner = "self";
        order.queue_ahead_at_join = -1.0;

        execute_market(order);
    }

    std_string orderMarketToString(const Order& order, const double& fill_qty){
        return format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order.side, config.from_tick(order.price_tick), fill_qty, order.status);
    }

    void execute_market(Order& order){

        auto& book = state.market_book;

        if(order.side == "BUY"){
            for(auto it = book.asks.begin(); it != book.asks.end() && order.remaining > 0;){

                double fill_qty = min(order.remaining, it->second);

                order.remaining -= fill_qty;
                it->second -= fill_qty;

                if(order.remaining == 0) order.status = "FILLED";
                else order.status = "PARTIALLY_FILLED";

                order.price_tick = it->first;
                state.last_fill_candidate = order; // for dashboard UI for market orders
                state.last_order_update = order;

                cout << orderMarketToString(order, fill_qty) << "\n";

                state.on_fill(config.from_tick(it->first), fill_qty, "BUY", "taker");

                if(it->second <= 0) it = book.asks.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
        else if(order.side == "SELL") {
            for(auto it = book.bids.begin(); it != book.bids.end() && order.remaining > 0;){

                double fill_qty = min(order.remaining, it->second);

                order.remaining -= fill_qty;
                it->second -= fill_qty;
                
                if(order.remaining == 0) order.status = "FILLED";
                else order.status = "PARTIALLY_FILLED";

                order.price_tick = it->first; // for dashboard UI for market orders
                state.last_fill_candidate = order;
                state.last_order_update = order;
                
                cout << orderMarketToString(order, fill_qty) << "\n";

                state.on_fill(config.from_tick(it->first), fill_qty, "SELL", "taker");

                if(it->second <= 0) it = book.bids.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
    }
};

class Engine {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    MarketMakingStrategy& strategy;
    Execution& execution;
    DatasetRecorder& recorder;

    EventNotifier& execution_event;
    EventNotifier& dashboard_event;
    std_string header;
    std_string latency_ms;

    atomic<bool> running{false};

    Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& execution,
            DatasetRecorder& recorder, EventNotifier& execution_event, EventNotifier& dashboard_event, const json& params)
        : config(config), state(state), strategy(strategy), execution(execution), recorder(recorder),
        execution_event(execution_event), dashboard_event(dashboard_event), params(params)
        {
            ostringstream ss;
            ss << setw(3) << setfill('0') << to_string(params["latency_ms"].get<uint64_t>());
            latency_ms = ss.str();
            build_header();
        }

    void on_trade_event(const Trade& trade){
        recorder.log_trade(trade);
        execution.process_trade(trade);

        {
            lock_guard<mutex> lock(dashboard_event.signal_mtx);
            dashboard_event.signal_pending = true;
        }
        dashboard_event.signal_cv.notify_one();
    }

    void on_depth_event(){
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);

        {
            lock_guard<mutex> lock(execution_event.signal_mtx);
            state.last_signal = signal;
            execution_event.signal_pending = true;
        }

        {
            lock_guard<mutex> lock(dashboard_event.signal_mtx);
            dashboard_event.signal_pending = true;
        }

        execution_event.signal_cv.notify_one();
        dashboard_event.signal_cv.notify_one();
        // cout << "signal sending, locking..., ts: " << signal.ts << "\n";
    }

    std_string tradeToString(const optional<Trade>& trade){
        return trade ? format("{:<5} | {:>10.4f} | {:>8.6f}", trade->side, trade->price, trade->qty) : "—";
    }

    std_string orderPointerToString(const Order* order){
        return order ? format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->qty, order->status) : "—";
    }

    std_string orderOptionalToString(const optional<Order>& order){
        return order ? format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->qty, order->status) : "—";
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

        double bid_queue = state.compute_queue_ahead("bids", config.to_tick(best_bid));
        double ask_queue = state.compute_queue_ahead("asks", config.to_tick(best_ask));

        snap.title.header = header;
        snap.title.regime = state.last_signal ? state.last_signal->regime : "";
        snap.title.pnl_pct = state.get_pnl(mid) / state.initial_cash * 100;

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

        snap.system.time = config.format_ms_precise(config.now_ms());
        snap.system.last_trade_ts = config.format_ms_precise(state.last_trade_ts);
        snap.system.last_depth_ts = config.format_ms_precise(state.last_depth_ts);
        snap.system.latency_ms = latency_ms;

        return snap;
    }

    void build_header(){
        std_string parts;

        auto add = [&](const std_string& s){
            if(s.empty()) return;
            if(!parts.empty()) parts += " | ";
            parts += s;
        };

        add(params["models"]["struct_model"].get<std_string>());
        add(params["models"]["regime_model"].get<std_string>());
        add(params["models"]["micro_signal_model"].get<std_string>());
        add(params["models"]["residual_model"].get<std_string>());
        add(params["models"]["toxicity_model"].get<std_string>());
        add(params["mode"].get<std_string>());
        add(params["exchange"].get<std_string>());
        add(params["instrument"].get<std_string>());

        header = parts;
    }
};

class SnapshotStore {
public:
    mutex mtx;
    Snapshot latest;

    void set(const Snapshot& snap){
        lock_guard<mutex> lock(mtx);
        latest = snap;
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

        if(terminal_thread.joinable()){
            terminal_thread.join();
        }
    }

    Element color_pnl(const double& value){
        if(value > 0) return text("▲ " + format("{:.4f}", value)) | color(Color::Green);
        else if(value < 0) return text("▼ " + format("{:.4f}", abs(value))) | color(Color::Red);
        else return text(format("{:.4f}", value));
    }

    Element color_risk(const double& inventory, const double limit = 10.0) {
        double intensity = min(abs(inventory) / limit, 1.0);
        auto value = text(format("{:.4f}", inventory));

        if(intensity < 0.3) return value | color(Color::Green);
        else if(intensity < 0.7) return hbox({text("▲ "),  value}) | color(Color::Yellow);
        else return hbox({text("▲ "), value}) | color(Color::Red);
    }

    Element centered_inventory_bar(double inv, double max_inv = 10.0, int width = 21){
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
                row_text("Latency", snap.system.latency_ms),
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
                if(ec)return;

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
    SnapshotStore& snapshot_store; 
    std_string host;
    uint16_t port; 

    boost::asio::io_context ioc; 
    tcp::acceptor acceptor; 

    set<shared_ptr<WsSession>> clients; 
    mutex dash_mtx; 
    thread server_thread;

    atomic<bool> running{false}; // keeps io_context alive 
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard; 
    
    DashboardServer(SnapshotStore& snapshot_store, const json& params) 
    : snapshot_store(snapshot_store), acceptor(ioc), guard(boost::asio::make_work_guard(ioc)) 
    { 
        host = params["server_config"]["host"].get<std_string>(); 
        port = params["server_config"]["port"].get<uint16_t>(); 
    }

    void start() { 
        running = true; 
        tcp::endpoint endpoint(boost::asio::ip::make_address(host), port); 
        
        acceptor.open(endpoint.protocol()); 
        acceptor.set_option(tcp::acceptor::reuse_address(true)); 
        acceptor.bind(endpoint); 
        acceptor.listen(); 
        
        server_thread = thread([this](){ 
            cout << "WS RUNNING ON http://localhost:" << port << "\n";
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
            else cout << "ACCEPT ERROR: " << ec.message() << "\n"; 
            if (running) do_accept(); 
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
        acceptor.close(ec); ioc.stop(); 
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
                {"latency_ms", snap.system.latency_ms},
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

    EventNotifier dashboard_event;
    EventNotifier execution_event;
    SnapshotStore snapshot_store;
    DashboardTerminal dashboard_terminal;
    DashboardServer dashboard_server;

    unique_ptr<Execution> execution;
    unique_ptr<BinanceBroker> broker;
    unique_ptr<BinanceUserStream> user_stream;

    unique_ptr<Engine> engine;
    unique_ptr<Feed> feed;

    atomic<bool> engine_running{false};
    atomic<bool> dashboard_running{false};

    vector<thread> threads;

    TradingSystem(const json& params) : 
        params(params), config(params), state(config, params), strategy(config, params), recorder(config, state, params),
        dashboard_terminal(snapshot_store), dashboard_server(snapshot_store, params) {initialize();}

    void initialize(){
        std_string mode = params["mode"].get<std_string>();
        if(mode == "live"){
            broker = make_unique<BinanceBroker>(config, params);
            execution = make_unique<LiveExecution>(config, state, *broker, recorder, params);
            user_stream = make_unique<BinanceUserStream>(state, *dynamic_cast<LiveExecution*>(execution.get()), *broker, recorder);
        }
        
        else if(mode != "live"){
            execution = make_unique<PaperExecution>(config, state, recorder, params);
        }

        engine = make_unique<Engine>(config, state, strategy, *execution, recorder, execution_event, dashboard_event, params);

        std_string exchange = params["exchange"].get<std_string>();
        auto on_trade_event = [this](const Trade& trade) {engine->on_trade_event(trade);};
        auto on_depth_event = [this]() {engine->on_depth_event();};
        auto log_event = [this](const std_string& type, const uint64_t& ts, const std_string& msg) {recorder.log_event(type, ts, msg);};
        auto log_orderbook_snapshot = [this](const json& snapshot) {recorder.log_orderbook_snapshot(snapshot);};
        
        if(exchange == "binance_spot" && mode != "replay"){
            feed = make_unique<BinanceSpotFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_futures" && mode != "replay"){
            feed = make_unique<BinanceFuturesFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_spot" && mode == "replay"){
            feed = make_unique<BinanceSpotReplayFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_futures" && mode == "replay"){
            feed = make_unique<BinanceFuturesReplayFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }
    }

    void start(){
        engine_running = true;
        dashboard_running = true;

        dashboard_terminal.start();
        dashboard_server.start();

        feed->start();

        if(user_stream) user_stream->start();

        start_dashboard_loop();
        
        if(params["latency_ms"].get<uint64_t>() == 0){
            cout << "Starting execution loop\n";
            start_execution_loop();
        }
        else{
            cout << "Starting execution latency loop\n";
            start_execution_latency_loop();
        }
        
        // Wait 5 seconds
        this_thread::sleep_for(seconds(5)); //FOR TESTING
        
        recorder.export_orderbook_snapshot();
        recorder.export_event_parquet();
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

                Snapshot snap = engine->build_snapshot();
                snapshot_store.set(snap);

                dashboard_terminal.refresh();
                dashboard_server.publish();
                // cout << "dashboard signal received, unlocking... ts: " << snap.system.last_depth_ts << "\n";
            }
        });
    }

    void start_execution_loop(){
        threads.emplace_back([this](){
            while(engine_running){
                Signal signal;
                
                {
                    unique_lock<mutex> lock(execution_event.signal_mtx);
                    execution_event.signal_cv.wait(lock, [this]{
                        return execution_event.signal_pending || !engine_running;});

                    if(!engine_running) break;

                    signal = *state.last_signal;
                    execution_event.signal_pending = false;
                }
                execution->place_quotes(signal);
                // cout << "execution signal received, unlocking... ts: " << signal.ts << "\n";
            }
        });
    }

    void start_execution_latency_loop(){ //polling driven
        threads.emplace_back([this](){
            while(engine_running){
                Signal signal;
                bool has_signal = false;

                {
                    unique_lock<mutex> lock(execution_event.signal_mtx);
                    execution_event.signal_cv.wait_for(lock, milliseconds(1), [this]{
                        return execution_event.signal_pending || !engine_running;});

                    if(!engine_running) break;

                    if(execution_event.signal_pending){
                        signal = *state.last_signal;
                        execution_event.signal_pending = false;
                        has_signal = true;
                    }
                }

                // 1. Put new event quote into latency queue
                if(has_signal) execution->place_quotes_latency(signal);

                // 2. Execute orders whose latency expired
                execution->process_latency_queue();
            }
        });
    }

    void run_forever(){
        while(engine_running) this_thread::sleep_for(seconds(1));
        shutdown();
    }
            
    void shutdown(){
        cout << "INTERRUPT RECEIVED - SHUTTING DOWN\n";

        engine_running = false;
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

        //--------------------------------------------------
        // dashboards
        //--------------------------------------------------
        dashboard_running = false;
        dashboard_server.stop();

        //--------------------------------------------------
        // stop FTXUI
        //--------------------------------------------------
        dashboard_terminal.stop();

        for(auto& t: threads){
            if(t.joinable()) t.join();
        }

        cout << "\n";
        recorder.shutdown();
    }
};

int main(){
    CURLcode rc = curl_global_init(CURL_GLOBAL_DEFAULT);

    if(rc != CURLE_OK){
        cerr << "curl init failed: " << curl_easy_strerror(rc) << endl;
        return 1;
    }

    std_string path = "D:\\OneDrive\\Trading\\manifest_live.json";
    
    // cout << "Enter manifest path: ";
    // getline(cin, path);
    // path = path.substr(1, path.size() - 2);
    
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

    system.run_forever();

    curl_global_cleanup();

    return 0;
}