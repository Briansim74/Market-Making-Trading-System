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

#include "mm_structs.hpp" //structs
#include "mm_config_orderbook.hpp" //market config & orderbook
#include "mm_markprice.hpp" //mark price

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
    double notional_traded = 0.0;

    Hawkes bid_hawkes;
    Hawkes ask_hawkes;
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
                        bid_hawkes.update(depletion, entry.ts); // hawkes process

                        bid_queue_ahead.second = max(0.0, bid_queue_ahead.second - bid_hawkes.beta * depletion);
                        // cout << "bid order queue_ahead @ " << price_tick << ": "
                        // << bid_queue_ahead.second << ", bid_hawkes.beta * depletion: " << bid_hawkes.beta * depletion << "\n";
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
                        ask_hawkes.update(depletion, entry.ts); // hawkes process

                        ask_queue_ahead.second = max(0.0, ask_queue_ahead.second - ask_hawkes.beta * depletion);
                        // cout << "ask order queue_ahead @ " << price_tick << ": "
                        // << ask_queue_ahead.second << ", ask_hawkes.beta * depletion: " << ask_hawkes.beta * depletion << "\n";
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
        notional_traded += fill_value;

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