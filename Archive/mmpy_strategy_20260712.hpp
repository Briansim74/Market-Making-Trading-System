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
#include "mmpy_state.hpp" //state & market_feature_state

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
        signal.trade_latency = state.trade_latency;
        signal.depth_latency = state.depth_latency;
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