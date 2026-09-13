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
#include "mmpy_state.hpp" //state & market feature state
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
            recorder.log_quote(bid_order, "BID", "CANCELED");

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
            recorder.log_quote(ask_order, "ASK", "CANCELED");

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
            // broker = make_unique<BinanceBroker>(config, params);
            // execution = make_unique<LiveExecution>(config, state, *broker, recorder, params);
            // user_stream = make_unique<BinanceUserStream>(state, *dynamic_cast<LiveExecution*>(execution.get()), *broker, recorder);
            cout << "INIT LIVE EXECUTION - NON EXISTENT";
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

        // dashboard_terminal.start();
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

                // dashboard_terminal.refresh();
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

    // std_string path = "D:\\OneDrive\\Trading\\manifest_live.json";
    std_string path = "D:\\OneDrive\\Trading\\manifest_replay.json";
    
    
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