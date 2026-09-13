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
#include "mm_state.hpp" //state & market feature state
#include "mm_recorder.hpp" //dataset recorder
#include "mm_broker_stream.hpp" // broker & user stream
#include "mm_clock.hpp" // clock

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
        
        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){

        // Convert inventory to USDT notional for risk calculations.
        double inv_usdt = signal.inventory * signal.mid; // USDT per PEPE

        // Inventory scale should be in USDT, not PEPE quantity.
        double inv_scale_usdt = 1.0; // strongly reduce bid/ask size at 1usdt long/short to go to flat inv
        double inv_signal = tanh(inv_usdt / inv_scale_usdt);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        // Also preferably make this USDT-based.
        double vol_penalty = 1.0 / (1.0 + 50.0 * signal.volatility);
        double risk_penalty = exp(-0.2 * pow(inv_usdt / inv_scale_usdt, 2));
        double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

        // Size is USDT notional.
        double size_usdt = config.base_size * vol_penalty * risk_penalty * toxicity_penalty;

        // if(size_usdt < 0.5 * config.base_size) return {0.0, 0.0}; // risk guard to prevent toxic order size

        // Exchange minimum is 1 USDT.
        size_usdt = max(config.base_size, min(size_usdt, 1.5 * config.base_size)); // max 1.5 base size usdt per order

        double bid_size_usdt = size_usdt * bid_multiplier;
        double ask_size_usdt = size_usdt * ask_multiplier;

        // Inventory limits in USDT.
        double max_buy_usdt = max(0.0, config.max_inv - inv_usdt); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell_usdt = max(0.0, config.max_inv + inv_usdt);

        bid_size_usdt = min(bid_size_usdt, max_buy_usdt);
        ask_size_usdt = min(ask_size_usdt, max_sell_usdt);

        // Convert USDT notional -> PEPE quantity.
        double bid_size = bid_size_usdt / signal.mid;
        double ask_size = ask_size_usdt / signal.mid;

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        // Re-check actual notional AFTER normalization.
        if(bid_size_usdt < config.base_size) bid_size = 0.0;
        if(ask_size_usdt < config.base_size) ask_size = 0.0;

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
        if(!bid_order){ // if no bid order
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
        if(!ask_order){ // if no ask order
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

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;

        double fill_qty = 0.0;

        auto& queue_ahead = (side == "BUY") ? state.bid_queue_ahead : state.ask_queue_ahead;

        // cout << "match side order queue ahead before: " << queue_ahead.second << "\n";

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

        // cout << "fill_qty: " << fill_qty << "\n";

        if(fill_qty > 0.0){
            
            if(config.toxicity_model != ""){

                ToxicityPred toxicity_pred;
                toxicity_pred.ts = order->signal.ts; // order ts, when order was made
                toxicity_pred.horizon_ms = order->signal.toxicity.horizon_ms;
                toxicity_pred.pred = order->signal.toxicity.pred;  // IMPORTANT: computed at quote time
                toxicity_pred.fill_price = trade.price;
                toxicity_pred.fill_sign = (side == "BUY") ? 1 : -1;

                state.mfs.toxicity_predictions.push_back(toxicity_pred);
            }

            order->remaining = max(0.0, order->remaining - fill_qty);

            state.on_fill(trade.price, fill_qty, order->side, true);

            if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
            else order->status = "FILLED";

            recorder.log_fill(*order, fill_qty, trade.ts + clock.offset_ms.load(), true);
        }

        // cout << "match side order queue ahead after: " << queue_ahead.second << "\n";

        state.last_order_update = *order;

        if(order->status == "FILLED"){
            state.reset_queue_position(side); // reset queue position
            open_orders.erase(order->client_oid);
        }
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

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0.0) it = book.bids.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }

        open_orders.erase(order->client_oid);
    }

    void apply_stream_update(const Stream& stream) override {}
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

    LiveExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, BinanceBroker& broker, BinanceClock& clock)
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

        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){

        // Convert inventory to USDT notional for risk calculations.
        double inv_usdt = signal.inventory * signal.mid; // USDT per PEPE

        // Inventory scale should be in USDT, not PEPE quantity.
        double inv_scale_usdt = 1.0; // strongly reduce bid/ask size at 1usdt long/short to go to flat inv
        double inv_signal = tanh(inv_usdt / inv_scale_usdt);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        // Also preferably make this USDT-based.
        double vol_penalty = 1.0 / (1.0 + 50.0 * signal.volatility);
        double risk_penalty = exp(-0.2 * pow(inv_usdt / inv_scale_usdt, 2));
        double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

        // Size is USDT notional.
        double size_usdt = config.base_size * vol_penalty * risk_penalty * toxicity_penalty;

        // if(size_usdt < 0.5 * config.base_size) return {0.0, 0.0}; // risk guard to prevent toxic order size

        // Exchange minimum is 1 USDT.
        size_usdt = max(config.base_size, min(size_usdt, 1.5 * config.base_size)); // max 1.5 base size usdt per order

        double bid_size_usdt = size_usdt * bid_multiplier;
        double ask_size_usdt = size_usdt * ask_multiplier;
        
        // Inventory limits in USDT.
        double max_buy_usdt = max(0.0, config.max_inv - inv_usdt); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell_usdt = max(0.0, config.max_inv + inv_usdt);

        bid_size_usdt = min(bid_size_usdt, max_buy_usdt);
        ask_size_usdt = min(ask_size_usdt, max_sell_usdt);

        // Convert USDT notional -> PEPE quantity.
        double bid_size = bid_size_usdt / signal.mid;
        double ask_size = ask_size_usdt / signal.mid;

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        // Re-check actual notional AFTER normalization.
        if(bid_size_usdt < config.base_size) bid_size = 0.0;
        if(ask_size_usdt < config.base_size) ask_size = 0.0;

        // cout << "bid_size: " << bid_size << "\n";
        // cout << "ask_size: " << ask_size << "\n";

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

        cout << "[BROKER] - PLACE LIMIT ORDER - client_oid: " << order.client_oid << 
        ", side: " << order.side << ", status: " << order.status << ", ts: " << order.ts << "\n";

        json resp = broker.place_limit(order, price, size);
        order.resp = resp;

        cout << "[BROKER] - SUCCESSFUL RESP\n";

        if(resp.contains("code")){
            int code = resp["code"];

            if(code == -4003){
                cout << "[BROKER] - Limit order rejected: Quantity less than or equal to zero.\n";
            }

            else if(code == -4164){
                cout << "[BROKER] - Order's notional must be no smaller than 50 (unless you choose reduce only).\n";
            }
            // other exchange errors
            else cout << "[BROKER] - Limit order failed: " << resp.dump() << "\n";

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

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;

        // Position notional in USDT
        double pos_notional = pos * mid;

        // Already within [-10, +10] USDT
        if(abs(pos_notional) <= config.target_notional){
            cout << "[BROKER] - Position already within +/- " << config.target_notional << " USDT: " << pos_notional << " USDT\n";
            return;
        }

        // Amount of notional we need to close
        double close_notional = abs(pos_notional) - config.target_notional;

        // Convert USDT notional back to base-asset quantity
        double close_qty = close_notional / mid;

        cout << "close_notional: " << close_notional << " , close_qty: " << close_qty << "\n";

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

        cout << "[BROKER] - PLACE MARKET ORDER - client_oid: " << order.client_oid << 
        ", side: " << order.side << ", status: " << order.status << ", ts: " << order.ts << "\n";

        json resp = broker.place_market(order);
        order.resp = resp;

        if(resp.contains("code")){
            int code = resp["code"];

            if(code == -4003){
                cout << "Market order rejected: Quantity less than or equal to zero.\n";
            }

            else if(code == -4164){
                cout << "Order's notional must be no smaller than 50 (unless you choose reduce only).\n";
            }
            // other exchange errors
            else cout << "Market order failed: " << resp.dump() << "\n";

            order.ts = clock.now_ms();
            order.status = "REJECTED";
            order.exchange_latency = state.exchange_latency;

            open_orders.erase(order.client_oid);

            return;
        }
    }

    void cancel_order(Order* order){
        
        order->ts = clock.now_ms();
        order->status = "PENDING_CANCEL";
        order->pending_cancel = true;
        
        cout << "[BROKER] - CANCEL LIMIT ORDER - client_oid: " << order->client_oid << 
        ", side: " << order->side << ", status: " << order->status <<", ts: " << order->ts << "\n";

        json resp = broker.cancel_order(*order);

        order->resp = resp;
        cout << "[BROKER] - SUCCESSFUL CANCEL RESP1\n";
        if(resp.contains("code")){ // TO BE DELETED
            int code = resp["code"];

            if(code == -2011){ // Order already filled/canceled
                cout << "[BROKER] - Cancel rejected: order no longer open\n";
            }
            // other exchange errors
            else cout << "[BROKER] - Cancel failed: " << resp.dump() << "\n";
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

    void orderToString(const Order* order, const Stream& stream, const std_string& order_type){
        cout << "[USER STREAM] - " << order_type << ((stream.order_type != "MARKET") ? " LIMIT ORDER" : " MARKET ORDER")
        << " - client_oid: " << order->client_oid << ", side: " << stream.side <<
        ", status: " << order->status << ", ts: " << stream.exchange_ts << "\n";
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

            orderToString(order, stream, "PLACE");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "NEW");
        }

        // -------------------------
        // CANCELED
        // -------------------------
        else if(stream.exec_type == "CANCELED"){
            
            order->status = "CANCELED";
            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            orderToString(order, stream, "CANCEL");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "CANCELED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // EXPIRED - FOR EXPIRED_IN_MATCH, IF EXCHANGE LAGS BEHIND NEW QUOTES
        // -------------------------
        else if(stream.exec_type == "EXPIRED"){

            order->status = "EXPIRED";

            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            orderToString(order, stream, "EXPIRED");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "EXPIRED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // TRADE
        // -------------------------
        else if(stream.exec_type == "TRADE"){

            if(config.toxicity_model != ""){

                ToxicityPred toxicity_pred;
                toxicity_pred.ts = order->signal.ts; // order ts, when order was made
                toxicity_pred.horizon_ms = order->signal.toxicity.horizon_ms;
                toxicity_pred.pred = order->signal.toxicity.pred;  // IMPORTANT: computed at quote time
                toxicity_pred.fill_price = stream.fill_price;
                toxicity_pred.fill_sign = (stream.side == "BUY") ? 1 : -1;

                state.mfs.toxicity_predictions.push_back(toxicity_pred);
            }

            state.on_fill(stream.fill_price, stream.fill_qty, stream.side, stream.is_maker);
            
            if(stream.status == "PARTIALLY_FILLED"){

                order->status = "PARTIALLY_FILLED";
                order->remaining = max(0.0, order->remaining - stream.fill_qty);

                (stream.side == "BUY") ? state.bid_queue_ahead.second = 0.0 : state.ask_queue_ahead.second = 0.0;

                orderToString(order, stream, "PARTIALLY_FILLED");
            }

            else if(stream.status == "FILLED"){

                order->status = "FILLED";
                order->remaining = 0.0;
                
                state.reset_queue_position(stream.side);

                orderToString(order, stream, "FILLED");
            }

            recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, stream.is_maker);

            state.last_order_update = *order;
       
            if(stream.status == "FILLED") open_orders.erase(order->client_oid);
        }
        else{
            cout << "UNKNOWN ORDER UPDATE " << stream.exec_type << "\n";
            throw runtime_error("unknown order update");            
        }
        cout << "stream end\n";
    }
};