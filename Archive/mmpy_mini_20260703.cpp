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
#include "mmpy_config_orderbook.hpp" //market config & orderbook
#include "mmpy_dashboard.hpp" //dashboard classes
#include "mmpy_feed.hpp" // feeds
#include "mmpy_state.hpp" //state & market_feature_state
#include "mmpy_recorder.hpp" //dataset recorder

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

class RegimeModel {
public:
    using Vec = vector<double>;
    using Mat = vector<vector<double>>;

    Mat means;
    vector<Mat> cov_inv;

    Vec log_det_cov;
    Vec log_weights;

    Vec scaler_mean;
    Vec scaler_scale;

    vector<std_string> regime_labels;

    int K;
    int D;

    // reusable buffers (IMPORTANT OPTIMIZATION)
    Vec x_scaled;
    Vec diff;

    Vec x_input;
    Vec scores;

    static constexpr double LOG_2PI = 1.8378770664093453;

    RegimeModel(const json& a){

        means = a["means"].get<Mat>();
        cov_inv = a["cov_inv"].get<vector<Mat>>();

        log_det_cov = a["log_det_cov"].get<Vec>();
        log_weights = a["log_weights"].get<Vec>();

        scaler_mean = a["scaler_mean"].get<Vec>();
        scaler_scale = a["scaler_scale"].get<Vec>();

        regime_labels = a["regime_labels"].get<vector<std_string>>();

        K = means.size();
        D = means[0].size();

        // allocate once
        x_input.resize(D);
        x_scaled.resize(D);
        diff.resize(D);
        scores.resize(K);
    }

    inline void scale(const Vec& X) {
        for (int i = 0; i < D; i++) {
            x_scaled[i] = (X[i] - scaler_mean[i]) / scaler_scale[i];
        }
    }

    inline void pack_features(const Regime& r, std::vector<double>& X) {
        X[0] = r.volatility;
        X[1] = r.spread;
        X[2] = r.order_imbalance;
        X[3] = r.trade_imbalance;
        X[4] = r.quote_churn;
        X[5] = r.inventory;
        X[6] = r.inventory_vol;
        X[7] = r.microprice_error;
    }

    tuple<std_string, int, double> predict(const Regime& regime){

        pack_features(regime, x_input);
        scale(x_input);

        int best_k = 0;
        double best_score = -1e300;

        // std::vector<double> scores(K);

        for (int k = 0; k < K; k++) {

            const auto& mu = means[k];
            const auto& inv = cov_inv[k];

            // compute diff = x - mu
            for (int i = 0; i < D; i++) {
                diff[i] = x_scaled[i] - mu[i];
            }

            // symmetric quadratic form (OPTIMIZED)
            double quad = 0.0;

            for (int i = 0; i < D; i++) {

                double di = diff[i];
                const double* row = inv[i].data();

                quad += di * row[i] * di;

                for (int j = i + 1; j < D; j++) {
                    double dj = diff[j];
                    quad += 2.0 * di * row[j] * dj;
                }
            }

            double logp =
                log_weights[k]
                - 0.5 * (
                    D * LOG_2PI +
                    log_det_cov[k] +
                    quad
                );

            scores[k] = logp;

            if (logp > best_score){
                best_score = logp;
                best_k = k;
            }
        }

        // log-sum-exp normalization (stable softmax)
        double max_log = best_score;

        double sum = 0.0;
        for (double s : scores)
            sum += exp(s - max_log);

        double log_norm = max_log + log(sum);

        double prob = exp(scores[best_k] - log_norm);

        return {
            regime_labels[best_k],
            best_k,
            prob
        };
    }
};
class MicroSignalModel {
public:
    std_string model;
    double target;
    double horizon_ms;
    double beta;
    double ic;

    MicroSignalModel(const json& artifact){
        model = artifact["model"];
        target = artifact["target"];
        horizon_ms = artifact["horizon_ms"];
        beta = artifact["beta"];
        ic = artifact["ic"];
    }

    double predict(const Features& features){
        double micro_signal = features.microprice_dev / features.mid;
        double fair_bias = ic * beta * micro_signal;
        return fair_bias;
    }
};
using Getter = function<double(const Features&)>;

struct FeatureRegistry {
    static const unordered_map<std::string, Getter>& map() {
        static const unordered_map<std::string, Getter> m = {
            {"mid", [](const Features& f){ return f.mid; }},
            {"fair", [](const Features& f){ return f.fair; }},
            {"skew", [](const Features& f){ return f.skew; }},
            {"microprice", [](const Features& f){ return f.microprice; }},
            {"microprice_dev", [](const Features& f){ return f.microprice_dev; }},
            {"spread", [](const Features& f){ return f.spread; }},
            {"order_imbalance", [](const Features& f){ return f.order_imbalance; }},
            {"trade_imbalance", [](const Features& f){ return f.trade_imbalance; }},
            {"inventory", [](const Features& f){ return f.inventory; }},
            {"volatility", [](const Features& f){ return f.volatility; }},
            {"queue_ahead_bid", [](const Features& f){ return f.queue_ahead_bid; }},
            {"queue_ahead_ask", [](const Features& f){ return f.queue_ahead_ask; }},
        };
    return m;
    }
};

class MLModel {
public:
    // BoosterHandle booster;
    std_string model;
    vector<std_string> feature_cols;
    int target;
    int horizon_ms;

    MLModel(const json& artifact){
        // XGBoosterCreate(nullptr, 0, &booster);
        // XGBoosterLoadModel(booster, model_path.c_str());
        
        model = artifact["model"];
        feature_cols = artifact["feature_cols"];
        target = artifact["target"];
        horizon_ms = artifact["horizon_ms"];
    }

    vector<double> build_vector(const Features& features, const vector<std_string>& feature_cols){
        const auto& reg = FeatureRegistry::map();

        vector<double> out;
        out.reserve(feature_cols.size());

        for(const auto& name: feature_cols){
            auto it = reg.find(name);
            
            if(it == reg.end()) throw runtime_error("Unknown feature: " + name);

            out.push_back(it->second(features));
        }

        return out;
    }

    double predict(const Features& features) const {
        // auto vec = build_vector(features, feature_cols);

        // DMatrixHandle dmat;
        // XGDMatrixCreateFromMat(
        //     vec.data(),
        //     1,
        //     vec.size(),
        //     NAN,
        //     &dmat
        // );

        // bst_ulong out_len;
        // const double* out_result;

        // XGBoosterPredict(booster, dmat, 0, 0, &out_len, &out_result);

        // double pred = out_result[0];
        // XGDMatrixFree(dmat);

        // return pred;
        return 0.0;
    }
};
class MarketMakingStrategy {
public:
    MarketConfig& config;
    const json& params;
    double gamma;

    // Models
    std_string struct_model;
    unique_ptr<MicroSignalModel> micro_signal_model;
    unique_ptr<MLModel> edge_model;
    unique_ptr<RegimeModel> regime_model;
    unique_ptr<MLModel> toxicity_model;

    std_string folder_path;

    MarketMakingStrategy(MarketConfig& config, const json& params)
        : config(config), params(params){
        gamma = params["gamma"].get<double>();
        folder_path = params["folder_path"].get<std_string>();

        struct_model = params["models"]["struct_model"];
        micro_signal_model = load_model<MicroSignalModel>("micro_signal_model");
        edge_model         = load_model<MLModel>("edge_model");
        regime_model       = load_model<RegimeModel>("regime_model");
        toxicity_model     = load_model<MLModel>("toxicity_model");
    }

    template<typename T> unique_ptr<T> load_model(const std_string& model_name){
        if(params["models"][model_name].get<std_string>().empty()) return nullptr;

        cout << model_name << " found...\n";
        std_string file = folder_path + "/" + params["models"][model_name].get<std_string>() + ".json";

        ifstream f(file);
        json artifact;
        f >> artifact;

        cout << "INITIALIZED " << model_name << "\n";

        return make_unique<T>(artifact);
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

    double compute_signal_quality(State& state){
        auto& log = state.market_feature_state.ml_signal_log;

        if(log.size() < 20)
            return 1.0;

        vector<double> p, r;
        p.reserve(log.size());
        r.reserve(log.size());

        for(auto& x: log){
            p.push_back(x.pred);
            r.push_back(x.realized);
        }

        auto mean = [](const vector<double>& v){
            double s = 0.0;
            for(double x: v) s += x;
            return s / v.size();
        };

        double mp = mean(p);
        double mr = mean(r);

        // -----------------------------
        // Pearson IC
        // -----------------------------
        double cov = 0.0, vp = 0.0, vr = 0.0;

        for(size_t i = 0; i < p.size(); i++){
            double dp = p[i] - mp;
            double dr = r[i] - mr;

            cov += dp * dr;
            vp  += dp * dp;
            vr  += dr * dr;
        }

        double ic = 0.0;
        if(vp > 1e-12 && vr > 1e-12)
            ic = cov / sqrt(vp * vr);

        if(isnan(ic))
            ic = 0.0;

        // -----------------------------
        // Rank IC (Spearman)
        // -----------------------------
        vector<size_t> idx(p.size());
        iota(idx.begin(), idx.end(), 0);

        auto rank = [&](const vector<double>& v){
            vector<size_t> id(v.size());
            iota(id.begin(), id.end(), 0);

            sort(id.begin(), id.end(), [&](size_t a, size_t b){ return v[a] < v[b]; });

            vector<double> rnk(v.size());
            for(size_t i = 0; i < id.size(); i++)
                rnk[id[i]] = static_cast<double>(i);

            return rnk;
        };

        vector<double> rp = rank(p);
        vector<double> rr = rank(r);

        double mp_r = mean(rp);
        double mr_r = mean(rr);

        double cov_r = 0.0, vp_r = 0.0, vr_r = 0.0;

        for(size_t i = 0; i < rp.size(); i++){
            double dp = rp[i] - mp_r;
            double dr = rr[i] - mr_r;

            cov_r += dp * dr;
            vp_r  += dp * dp;
            vr_r  += dr * dr;
        }

        double rank_ic = 0.0;
        if(vp_r > 1e-12 && vr_r > 1e-12)
            rank_ic = cov_r / sqrt(vp_r * vr_r);

        if(isnan(rank_ic))
            rank_ic = 0.0;

        // -----------------------------
        // Directional accuracy
        // -----------------------------
        double hits = 0.0;
        for(size_t i = 0; i < p.size(); i++){
            if((p[i] > 0) == (r[i] > 0))
                hits += 1.0;
        }

        double hit_rate = hits / p.size();
        double directional_score = (hit_rate - 0.5) * 2.0;  // [-1, 1]

        // -----------------------------
        // 5. final score
        // -----------------------------
        double pred_vol = sqrt(vp / p.size());
        double stability_penalty = exp(-pred_vol * 50.0);

        double raw = 0.5 * ic + 0.3 * rank_ic + 0.2 * directional_score;
        double signal_quality = stability_penalty * tanh(3.0 * raw);

        return 0.3 + 1.4 * ((signal_quality + 1.0) / 2.0);
    }

    auto detect_regime(const Regime& regime){
        
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
            policy.inventory_target = 0.7;
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

    double compute_struct_delta(const Features& features, const Policy& policy){
        
        if(struct_model != "blended_AS") return 0.0;

        double reservation = features.fair + features.skew;
        double struct_center = features.mid + policy.alpha_struct * (reservation - features.mid);
        double struct_delta = struct_center - features.mid;

        return struct_delta;
    }

    double compute_micro_signal_delta(const Features& features){

        if(micro_signal_model == nullptr) return 0.0;

        double fair_bias = micro_signal_model->predict(features);
        double micro_signal_delta = features.mid * fair_bias;

        return micro_signal_delta;
    }

    pair<double, double> compute_ml_delta(State& state, double struct_delta, 
                                        double micro_signal_delta, const Features& features, const Policy& policy){
        
        if(edge_model == nullptr) return {0.0, 0.0};

        double reservation = features.mid + struct_delta + micro_signal_delta;
        double expected_return = edge_model->predict(features);
        double signal_quality = compute_signal_quality(state);

        MLPred ml_pred;
        ml_pred.ts = state.last_depth_ts;
        ml_pred.pred = expected_return;
        ml_pred.reservation = reservation;
        state.market_feature_state.ml_predictions.push_back(ml_pred);

        double effective_k = policy.k0 * signal_quality;
        double ml_center = reservation * exp(expected_return * effective_k);

        return {ml_center - reservation, signal_quality};
    }

    Toxicity compute_toxicity(const Features& features){
        
        Toxicity toxicity;

        if(toxicity_model == nullptr) return toxicity;

        double prediction = toxicity_model->predict(features);
        toxicity.tox = -prediction;

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

        auto [ml_delta, signal_quality] = compute_ml_delta(state, struct_delta, micro_signal_delta, features, policy);

        double center = mid + struct_delta + micro_signal_delta + ml_delta;
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

        double bid_delta = abs(best_bid - state.market_feature_state.prev_best_bid);
        double ask_delta = abs(best_ask - state.market_feature_state.prev_best_ask);

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
        signal.ml_delta = ml_delta;
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
        signal.signal_quality = signal_quality;
        signal.toxicity = toxicity;

        signal.bid_delta = bid_delta;
        signal.ask_delta = ask_delta;
        signal.quote_churn = bid_delta + ask_delta;

        signal.my_bid = bid;
        signal.my_ask = ask;

        return signal;
    }
};
class Execution {
public:
    virtual ~Execution() = default;
};
class BinanceBroker {
public:
    BinanceBroker(MarketConfig&, const json&) {}
};
class LiveExecution : public Execution {
public:
    LiveExecution(MarketConfig&, State&, BinanceBroker&, DatasetRecorder&, const json&) {}
};
class BinanceUserStream {
public:
    BinanceUserStream(State& state, LiveExecution& execution,
                      BinanceBroker& broker, DatasetRecorder& recorder) {}
    void start(){};
};
class PaperExecution : public Execution {
public:
    PaperExecution(MarketConfig&, State&, DatasetRecorder&, const json&) {}
};
class Engine {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    MarketMakingStrategy& strategy;
    Execution& execution;
    DatasetRecorder& recorder;

    mutex& signal_mtx;
    condition_variable& signal_cv;
    bool& signal_pending;

    std_string struct_model;
    std_string edge_model;
    std_string mode;
    std_string exchange;
    std_string instrument;

    Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& execution, DatasetRecorder& recorder,
            mutex& signal_mtx, condition_variable& signal_cv, bool& signal_pending, const json& params)
        : config(config), state(state), strategy(strategy), execution(execution), recorder(recorder),
        signal_mtx(signal_mtx), signal_cv(signal_cv), signal_pending(signal_pending), params(params)
    {
        struct_model = params["models"]["struct_model"].get<std_string>();
        edge_model   = params["models"]["edge_model"].get<std_string>();
        mode         = params["mode"].get<std_string>();
        exchange     = params["exchange"].get<std_string>();
        instrument   = params["instrument"].get<std_string>();
    }

    void on_trade_event(const Trade& trade){
        recorder.log_trade(trade);
        // execution.process_trade(trade);
    }

    void on_depth_event(){
        if(!state.initialized) return;

        Signal signal = strategy.generate_quotes(state);
        recorder.log_snapshot(signal);

        {
            lock_guard<mutex> lock(signal_mtx);
            state.last_signal = signal;
            signal_pending = true;
        }
        signal_cv.notify_one();
        // cout << "signal sending, locking..., ts: " << signal.ts << "\n";
    }

    std_string tradeToString(const optional<Trade>& trade){
        return trade ? format("{:<5} | {:>10.4f} | {:>8.6f}", trade->side, trade->price, trade->qty) : "—";
    }

    std_string orderToString(const Order* order){
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

        snap.title.struct_model = struct_model;
        snap.title.mode = mode;
        snap.title.exchange = exchange;
        snap.title.instrument = instrument;
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
        snap.market.trade_imbalance = 0.0;
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
        snap.signals.tox = state.last_signal ? state.last_signal->toxicity.tox : 0.0;
        snap.signals.k1 = state.last_signal ? state.last_signal->toxicity.k1 : 0.0;
        snap.signals.k2 = state.last_signal ? state.last_signal->toxicity.k2 : 0.0;

        snap.quotes.my_bid = 0.0;
        snap.quotes.my_ask = 0.0;
        snap.quotes.current_bid_size = 0.0;
        snap.quotes.current_ask_size = 0.0;

        snap.execution.bid_queue = bid_queue;
        snap.execution.ask_queue = ask_queue;
        snap.execution.bid_pressure = bid_queue / (bid_size + 1e-9);
        snap.execution.ask_pressure = ask_queue / (ask_size + 1e-9);
        snap.execution.buy_order = orderToString(nullptr);
        snap.execution.sell_order = orderToString(nullptr);
        snap.execution.last_fill_candidate = orderToString(state.last_fill_candidate);
        snap.execution.last_order_update = orderToString(state.last_order_update);

        snap.risk.inventory = state.inventory;
        snap.risk.realized_pnl = state.realized_pnl;
        snap.risk.unrealized_pnl = state.get_unrealized_pnl(mid);
        snap.risk.fees_paid = state.fees_paid;
        snap.risk.total_pnl = state.get_pnl(mid);

        snap.system.time = config.format_ms_precise(config.now_ms());
        snap.system.last_trade_ts = config.format_ms_precise(state.last_trade_ts);
        snap.system.last_depth_ts = config.format_ms_precise(state.last_depth_ts);

        return snap;
    }
};

class TradingSystem {
public:
    const json& params;

    mutex signal_mtx;
    condition_variable signal_cv;
    bool signal_pending = false;

    MarketConfig config;
    State state;

    MarketMakingStrategy strategy;
    DatasetRecorder recorder;

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

        engine = make_unique<Engine>(config, state, strategy, *execution, recorder, 
            signal_mtx, signal_cv, signal_pending, params);

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

        // dashboard_terminal.start();
        dashboard_server.start();

        feed->start();

        if(user_stream) user_stream->start();

        start_dashboard_loop();
        start_execution_loop();

        // Wait 5 seconds
        this_thread::sleep_for(seconds(5)); //FOR TESTING
        
        recorder.export_orderbook_snapshot();
        recorder.export_event_parquet();
    }

    void start_dashboard_loop(){
        threads.emplace_back([this](){
            while(dashboard_running){
                Snapshot snap = engine->build_snapshot();
                snapshot_store.set(snap);

                // dashboard_terminal.refresh();
                dashboard_server.publish();

                this_thread::sleep_for(milliseconds(20));
            }
        });
    }

    void start_execution_loop(){
        threads.emplace_back([this](){
            while(engine_running){
                Signal signal;
                
                {
                    unique_lock<mutex> lock(signal_mtx);
                    signal_cv.wait(lock, [this]{ return signal_pending || !engine_running;});

                    if(!engine_running) break;

                    signal = *state.last_signal;
                    signal_pending = false;
                }
                // execution->place_quotes(signal);
                // cout << "signal received, unlocking... ts: " << signal.ts << "\n";
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
        dashboard_running = false;

        // dashboard_terminal.stop();
        dashboard_server.stop();

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
    // std_string path = "D:\\OneDrive\\Trading\\manifest_replay.json";
    
    
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