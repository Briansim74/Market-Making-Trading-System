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
#include "mm_dashboard.hpp" //dashboard classes
#include "mm_feed.hpp" // feeds
#include "mm_state.hpp" //state & market feature state
#include "mm_recorder.hpp" //dataset recorder
#include "mm_strategy.hpp" //models & strategy
#include "mm_execution.hpp" //execution
#include "mm_broker_stream.hpp" // broker & user stream
#include "mm_clock.hpp" //clock

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

class Engine {
public:
    MarketConfig& config;
    State& spot_state;
    State& perps_state;
    MarketMakingStrategy& strategy;
    
    Execution& spot_execution; // maker execution
    Execution& perps_execution; // hedge execution

    BinanceClock& clock;
    ExecutionEventQueue& execution_event;
    EventNotifier& dashboard_event;
    SnapshotStore& snapshot_store;
    
    DatasetRecorder& spot_recorder;
    DatasetRecorder& perps_recorder;



    atomic<bool> trading_enabled = true;
    atomic<bool> waiting_for_cancel = false;
    atomic<bool> waiting_for_flatten = false;
    atomic<bool> shutdown_complete = false;
    
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

    std_string header;
    
    Engine(MarketConfig& config, State& spot_state, State& perps_state, MarketMakingStrategy& strategy, Execution& spot_execution, Execution& perps_execution, 
        
        
        BinanceClock& clock,
        ExecutionEventQueue& execution_event, EventNotifier& dashboard_event, SnapshotStore& snapshot_store, 
        
        DatasetRecorder& spot_recorder, DatasetRecorder& perps_recorder)
        : config(config), spot_state(spot_state), perps_state(perps_state), strategy(strategy), spot_execution(spot_execution), perps_execution(perps_execution), 
        
        clock(clock), execution_event(execution_event),
        dashboard_event(dashboard_event), snapshot_store(snapshot_store), spot_recorder(spot_recorder), perps_recorder(perps_recorder) {build_header();}


    void process_event(const ExecutionEvent& ev){
        switch(ev.type){
            case ExecutionEventType::SPOT_TRADE_UPDATE:
                on_trade_event_spot(ev.trade);
                break;

            case ExecutionEventType::PERPS_TRADE_UPDATE:
                on_trade_event_perps(ev.trade);
                break;

            case ExecutionEventType::SPOT_DEPTH_UPDATE:
                on_depth_event_spot(ev.depth);
                break;
            
            case ExecutionEventType::PERPS_DEPTH_UPDATE:
                on_depth_event_futures(ev.depth);
                break;

            case ExecutionEventType::SPOT_STREAM_UPDATE:
                on_stream_event_spot(ev.stream);
                break;

            case ExecutionEventType::PERPS_STREAM_UPDATE:
                on_stream_event_perps(ev.stream);
                break;

            case ExecutionEventType::CANCEL_UPDATE:
                on_cancel_event();
                break;

            case ExecutionEventType::HYPERLIQUID_DEPTH_UPDATE:
                on_depth_event_hyperliquid(ev.depth);
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
            case ExecutionEventType::SPOT_TRADE_UPDATE:
                on_trade_event_spot(ev.trade);
                break;

            case ExecutionEventType::PERPS_TRADE_UPDATE:
                on_trade_event_perps(ev.trade);
                break;

            case ExecutionEventType::SPOT_DEPTH_UPDATE:
                on_depth_event_spot_latency(ev.depth);
                break;
            
            case ExecutionEventType::PERPS_DEPTH_UPDATE:
                on_depth_event_futures_latency(ev.depth);
                break;

            case ExecutionEventType::CANCEL_UPDATE:
                on_cancel_event();
                break;
        }
    }

    void on_trade_event_spot(const Trade& trade){
        spot_state.time = trade.local_ts;
        spot_state.last_trade = trade;
        spot_state.last_trade_ts = trade.ts;
        spot_state.trade_latency = trade.latency;

        spot_recorder.log_trade(trade);
        spot_execution.process_trade(trade);
    }

    void on_trade_event_perps(const Trade& trade){
        perps_state.time = trade.local_ts;
        perps_state.last_trade = trade;
        perps_state.last_trade_ts = trade.ts;
        perps_state.trade_latency = trade.latency;

        perps_recorder.log_trade(trade);
        perps_execution.process_trade(trade);
    }

    void on_depth_event_spot(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        spot_state.time = depth.local_ts;
        spot_state.last_depth_ts = depth.ts;
        spot_state.depth_latency = depth.latency;
        
        auto& book = spot_state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << depth.U << "\n";
            spot_state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA - REMOVE LOCK FOR SINGLE THREADED QUEUE
        // -----------------------------
        spot_state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        spot_state.update_vol();
        spot_state.compute_order_imbalance();
        spot_state.update_market_feature_state();
        spot_state.update_residual_realization();
        spot_state.update_toxicity_realization(); // NEW
        spot_state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!spot_state.initialized) return;

        Signal signal = strategy.generate_quotes(spot_state);
        spot_recorder.log_snapshot(signal);
        spot_state.last_signal = signal;

        if(config.market != "spot" || !trading_enabled) return; // to be changed if shift to error style

        spot_execution.place_quotes(signal);
    }

    void on_depth_event_futures(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        perps_state.time = depth.local_ts;
        perps_state.last_depth_ts = depth.ts;
        perps_state.depth_latency = depth.latency;
        
        auto& book = perps_state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.pu != book.last_update_id){
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << depth.pu << "\n";
            perps_state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        perps_state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        perps_state.update_vol();
        perps_state.compute_order_imbalance();
        perps_state.update_market_feature_state();
        perps_state.update_residual_realization();
        perps_state.update_toxicity_realization(); // NEW
        perps_state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!perps_state.initialized) return;

        Signal signal = strategy.generate_quotes(perps_state);
        perps_recorder.log_snapshot(signal);
        perps_state.last_signal = signal;

        if(config.market != "perps" || !trading_enabled) return;

        perps_execution.place_quotes(signal);
    }

    void on_depth_event_spot_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        spot_state.time = depth.local_ts;
        spot_state.last_depth_ts = depth.ts;
        spot_state.depth_latency = depth.latency;
        
        auto& book = spot_state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << depth.U << "\n";
            spot_state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        spot_state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        spot_state.update_vol();
        spot_state.compute_order_imbalance();
        spot_state.update_market_feature_state();
        spot_state.update_residual_realization();
        spot_state.update_toxicity_realization(); // NEW
        spot_state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!spot_state.initialized) return;

        Signal signal = strategy.generate_quotes(spot_state);
        spot_recorder.log_snapshot(signal);
        spot_state.last_signal = signal;

        if(config.market != "spot" || !trading_enabled) return;

        spot_execution.place_quotes_latency(signal);
    }

    void on_depth_event_futures_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        perps_state.time = depth.local_ts;
        perps_state.last_depth_ts = depth.ts;
        perps_state.depth_latency = depth.latency;
        
        auto& book = perps_state.market_book;

        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(depth.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(depth.pu != book.last_update_id){
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << depth.pu << "\n";
            perps_state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA
        // -----------------------------
        perps_state.update_queue_from_depth(depth);
        book.apply_delta(depth);
        book.last_update_id = depth.u;

        // -----------------------------
        // HEAVY FEATURES
        // -----------------------------
        perps_state.update_vol();
        perps_state.compute_order_imbalance();
        perps_state.update_market_feature_state();
        perps_state.update_residual_realization();
        perps_state.update_toxicity_realization(); // NEW
        perps_state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(!perps_state.initialized) return;

        Signal signal = strategy.generate_quotes(perps_state);
        perps_recorder.log_snapshot(signal);
        perps_state.last_signal = signal;

        if(config.market != "perps" || !trading_enabled) return;

        perps_execution.place_quotes_latency(signal);
    }

    void on_stream_event_spot(const Stream& stream){
        spot_state.time = stream.local_ts;
        spot_execution.apply_stream_update(stream);

        wait_for_cancel();
        wait_for_flatten();
        
        if(!trading_enabled) return;

        if(config.market == "perps"){
            // HedgeSignal hedge_signal = strategy.calculate_hedge(state);
            // execution.place_hedge(hedge_signal);
            // cout << "yes";
        }
        
        else spot_execution.place_quotes(*spot_state.last_signal);
    }

    void on_stream_event_perps(const Stream& stream){
        perps_state.time = stream.local_ts;
        perps_execution.apply_stream_update(stream);

        wait_for_cancel();
        wait_for_flatten();
        
        if(!trading_enabled) return;

        if(config.market == "spot"){
            // HedgeSignal hedge_signal = strategy.calculate_hedge(state);
            // execution.place_hedge(hedge_signal);
            // cout << "yes";
        }
        
        else perps_execution.place_quotes(*perps_state.last_signal);
    }

    void on_depth_event_hyperliquid(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        spot_state.time = depth.local_ts;
        spot_state.last_depth_ts = depth.ts;
        spot_state.depth_latency = depth.latency;
        
        auto& book = spot_state.market_book;

        // // -----------------------------
        // // DROP OLD EVENTS
        // // -----------------------------
        // if(depth.u <= book.last_update_id) return;

        // // -----------------------------
        // // GAP DETECTION
        // // -----------------------------
        // if(depth.U > book.last_update_id + 1){
        //     cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << depth.U << "\n";
        //     state.initialized = false;
        //     return;
        // }

        // -----------------------------
        // APPLY DELTA - REMOVE LOCK FOR SINGLE THREADED QUEUE
        // -----------------------------
        spot_state.update_queue_from_depth(depth);
        // book.apply_delta(depth);
        book.replace_book(depth);
        // book.last_update_id = depth.u; // last update id updated from replace book

        // // -----------------------------
        // // HEAVY FEATURES
        // // -----------------------------
        // state.update_vol();
        // state.compute_order_imbalance();
        // state.update_market_feature_state();
        // state.update_residual_realization();
        // state.update_toxicity_realization(); // NEW
        // state.update_performance();

        // // -----------------------------
        // // STRATEGY ONLY AFTER INIT
        // // -----------------------------
        // if(!state.initialized) return;

        // Signal signal = strategy.generate_quotes(state);
        // recorder.log_snapshot(signal);
        // state.last_signal = signal;

        // if(!trading_enabled) return; // to be changed if shift to error style

        // // execution.place_quotes(signal);
    }

    void on_cancel_event(){
        cout << "CANCELLING OPEN ORDERS\n";
        spot_execution.cancel_all_orders(); // to be changed to spot + perps
        waiting_for_cancel = true;
        if(config.mode == "stochastic" || config.mode == "replay"){
            wait_for_cancel();
            wait_for_flatten();
        }
        cout << "CANCELLING OPEN ORDERS1\n";
    }

    void wait_for_cancel(){
        if(waiting_for_cancel && (!spot_execution.get_open_order("BUY") && !spot_execution.get_open_order("SELL"))){ // to be changed to spot + perps
            
            cout << "NO MORE OPEN ORDERS, PLACING MARKET\n";
            waiting_for_cancel = false;
            waiting_for_flatten = true;

            spot_execution.place_market();
            cout << "wait_for_cancel done\n";
        }
    }

    void wait_for_flatten(){
        // if(waiting_for_flatten && abs(state.inventory) <= 1e-9){
        // Already within [-10, +10] USDT
        if(waiting_for_flatten && abs(spot_state.inventory * spot_state.market_book.mid()) <= config.target_notional){ // to be changed to spot + perps
 
            cout << "abs(state.inventory * state.market_book.mid(): " << abs(spot_state.inventory * spot_state.market_book.mid()) << "\n";
            waiting_for_flatten = false;

            Snapshot snap = build_snapshot();
            snapshot_store.set(move(snap));

            {
                lock_guard<mutex> lock(dashboard_event.signal_mtx);
                dashboard_event.signal_pending = true;
            }

            dashboard_event.signal_cv.notify_one();
            
            {
                lock_guard<mutex> lock(shutdown_mtx);
                shutdown_complete = true;
            }
            shutdown_cv.notify_one();
        }
    }

    void stop(){
        cout << "STOPPING ENGINE\n";
        trading_enabled = false;

        ExecutionEvent ev;
        ev.type = ExecutionEventType::CANCEL_UPDATE;
        execution_event.push(ev);

        unique_lock lock(shutdown_mtx);
        shutdown_cv.wait(lock, [&]{return shutdown_complete.load();});

        cout << "ENGINE STOPPED\n";
    }

    std_string tradeToString(const optional<Trade>& trade){
        return trade ? format("{:<5} | {:>10.10f} | {:>8.6f}", trade->side, trade->price, trade->qty) : "—";
    }

    std_string orderPointerToString(Order* order){
        return order ? format("{:<5} | {:>10.10f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->remaining, order->status) : "—";
    }

    std_string orderOptionalToString(const optional<Order>& order){
        return order ? format("{:<5} | {:>10.10f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->remaining, order->status) : "—";
    }

    Snapshot build_snapshot(){
        Snapshot snap;

        State& maker_state = (config.market == "spot") ? spot_state : perps_state;
        State& hedge_state = (config.market == "spot") ? perps_state : spot_state;

        Execution& maker_execution = (config.market == "spot") ? spot_execution : perps_execution;
        Execution& hedge_execution = (config.market == "spot") ? perps_execution : spot_execution;
        
        auto& book = maker_state.market_book;
 
        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double spread = best_ask - best_bid;
        double microprice = (best_ask * bid_size + best_bid * ask_size) /(bid_size + ask_size + 1e-9);

        double bid_queue = (maker_state.bid_queue_ahead.first == bid_tick) ? maker_state.bid_queue_ahead.second : 0.0;
        double ask_queue = (maker_state.ask_queue_ahead.first == ask_tick) ? maker_state.ask_queue_ahead.second : 0.0;

        snap.title.header = header;
        snap.title.regime = maker_state.last_signal ? maker_state.last_signal->regime : "";
        snap.title.pnl_pct = maker_state.get_pnl(mid) / config.initial_cash * 100;

        snap.market.mid = mid;
        snap.market.microprice = microprice;
        snap.market.spread = spread;
        snap.market.best_bid = best_bid;
        snap.market.best_ask = best_ask;
        snap.market.bid_size = bid_size;
        snap.market.ask_size = ask_size;
        snap.market.ewma_vol = maker_state.get_vol();
        snap.market.order_imbalance = maker_state.order_imbalance;
        snap.market.trade_imbalance = maker_state.trade_imbalance;
        snap.market.trade = tradeToString(maker_state.last_trade);

        snap.regime.regime = maker_state.last_signal ? maker_state.last_signal->regime : "";
        snap.regime.confidence = maker_state.last_signal ? maker_state.last_signal->regime_prob : 0.0;

        snap.signals.fair = maker_state.last_signal ? maker_state.last_signal->fair : 0.0;
        snap.signals.skew = maker_state.last_signal ? maker_state.last_signal->skew : 0.0;
        snap.signals.reservation = maker_state.last_signal ? maker_state.last_signal->reservation : 0.0;
        snap.signals.alpha_trade_imb = maker_state.last_signal ? maker_state.last_signal->alpha_trade_imb : 0.0;
        snap.signals.alpha_struct = maker_state.last_signal ? maker_state.last_signal->alpha_struct : 0.0;
        snap.signals.alpha_residual = maker_state.last_signal ? maker_state.last_signal->alpha_residual : 0.0;
        snap.signals.spread_multiplier = maker_state.last_signal ? maker_state.last_signal->spread_multiplier : 0.0;
        snap.signals.inventory_target = maker_state.last_signal ? maker_state.last_signal->inventory_target : 0.0;
        snap.signals.tox = maker_state.last_signal ? maker_state.last_signal->toxicity.tox : 0.0;
        snap.signals.k_spread = maker_state.last_signal ? maker_state.last_signal->toxicity.k_spread : 0.0;
        snap.signals.k_order_size = maker_state.last_signal ? maker_state.last_signal->toxicity.k_order_size : 0.0;
        snap.signals.residual_signal_quality = maker_state.last_signal ? maker_state.last_signal->residual_signal_quality : 0.0;
        snap.signals.toxicity_signal_quality = maker_state.last_signal ? maker_state.last_signal->toxicity_signal_quality : 0.0;
        
        snap.quotes.my_spread = maker_state.last_signal ? maker_state.last_signal->my_spread : 0.0;
        snap.quotes.my_bid = maker_execution.get_last_bid();
        snap.quotes.my_ask = maker_execution.get_last_ask();
        snap.quotes.current_bid_size = maker_execution.get_current_bid_size();
        snap.quotes.current_ask_size = maker_execution.get_current_ask_size();

        snap.execution.bid_queue = bid_queue;
        snap.execution.ask_queue = ask_queue;
        snap.execution.bid_pressure = bid_queue / (bid_size + 1e-9);
        snap.execution.ask_pressure = ask_queue / (ask_size + 1e-9);

        snap.execution.buy_order = orderPointerToString(maker_execution.get_open_order("BUY"));
        snap.execution.sell_order = orderPointerToString(maker_execution.get_open_order("SELL"));
        snap.execution.last_fill_candidate = orderOptionalToString(maker_state.last_fill_candidate);
        snap.execution.last_order_update = orderOptionalToString(maker_state.last_order_update);
        
        snap.risk.inventory = maker_state.inventory * mid; // USDT normalized
        snap.risk.realized_pnl = maker_state.realized_pnl;
        snap.risk.unrealized_pnl = maker_state.get_unrealized_pnl(mid);
        snap.risk.fees_paid = maker_state.fees_paid;
        snap.risk.total_pnl = maker_state.get_pnl(mid);

        snap.system.time = config.format_ms_precise(maker_state.time);
        snap.system.last_trade_ts = config.format_ms_precise(maker_state.last_trade_ts);
        snap.system.last_depth_ts = config.format_ms_precise(maker_state.last_depth_ts);
        snap.system.trade_latency = maker_state.trade_latency;
        snap.system.depth_latency = maker_state.depth_latency;
        snap.system.exchange_latency = maker_state.exchange_latency;

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

class TradingSystem {
public:

//     struct TradingSystem {
//     SpotState spot_state;
//     PerpState perp_state;

//     PortfolioState portfolio_state;
//     HedgeController hedge_controller;

//     unique_ptr<SpotBroker> spot_broker;
//     unique_ptr<PerpBroker> perp_broker;

//     unique_ptr<SpotExecution> spot_execution;
//     unique_ptr<PerpExecution> perp_execution;
// };


    const ordered_json& params;
    MarketConfig config;

    State spot_state;
    State perps_state;

    MarketMakingStrategy strategy;
    DatasetRecorder spot_recorder;
    DatasetRecorder perps_recorder;
    BinanceClock clock;

    ExecutionEventQueue execution_event;
    EventNotifier dashboard_event;
    SnapshotStore snapshot_store;
    DashboardTerminal dashboard_terminal;
    DashboardServer dashboard_server;

    // Strategy roles
    // unique_ptr<Execution> execution;
    unique_ptr<Execution> spot_execution;
    unique_ptr<Execution> perps_execution;

    // unique_ptr<BinanceBroker> broker;
    unique_ptr<BinanceBroker> spot_broker;
    unique_ptr<BinanceBroker> perps_broker;
    // unique_ptr<BinanceUserStream> user_stream;

    unique_ptr<BinanceUserStream> spot_user_stream;
    unique_ptr<BinanceUserStream> perps_user_stream;

    unique_ptr<Engine> engine;
    // unique_ptr<Feed> feed;
    unique_ptr<Feed> spot_feed;
    unique_ptr<Feed> perps_feed;

    atomic<bool> execution_loop_running{false};
    atomic<bool> dashboard_loop_running{false};
    atomic<bool> shutdown_requested{false};

    asio::io_context ioc;
    asio::signal_set signals;
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

    vector<thread> threads;

    TradingSystem(const ordered_json& params) :
        params(params), config(params), spot_state(config), perps_state(config), 
        
        strategy(config), spot_recorder(config, spot_state, params), perps_recorder(config, perps_state, params),
        clock(config), dashboard_terminal(snapshot_store), dashboard_server(config, snapshot_store),
        signals(ioc, SIGINT, SIGTERM) {initialize();}

    void initialize(){
        if(config.mode == "live" && config.market == "spot" && config.instrument == "pepeusdt"){
            spot_broker = make_unique<BinanceSpotBroker>(config, clock);
            spot_execution = make_unique<LiveExecution>(config, spot_state, spot_recorder, *spot_broker, clock);
            spot_user_stream = make_unique<BinanceSpotUserStream>(config, *spot_broker, execution_event, clock);
            
            if(config.target_hedge_ratio > 0.0){
                perps_broker = make_unique<BinanceFuturesBroker>(config, clock);
                perps_execution = make_unique<LiveExecution>(config, perps_state, perps_recorder, *perps_broker, clock);
                perps_user_stream = make_unique<BinanceFuturesUserStream>(config, *perps_broker, execution_event, clock);
            }
        }

        else if((config.mode == "testnet" || config.mode == "live") && config.market == "futures"){
            perps_broker = make_unique<BinanceFuturesBroker>(config, clock);
            perps_execution = make_unique<LiveExecution>(config, perps_state, perps_recorder, *perps_broker, clock);
            perps_user_stream = make_unique<BinanceFuturesUserStream>(config, *perps_broker, execution_event, clock);
        }
        
        else if(config.mode != "live"){
            spot_execution = make_unique<PaperExecution>(config, spot_state, spot_recorder, clock);
        }

        if(config.exchange == "binance" && config.market == "spot" && config.mode != "replay"){
            spot_feed = make_unique<BinanceSpotFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode != "replay"){
            perps_feed = make_unique<BinanceFuturesFeed>(config, perps_state, execution_event, perps_recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "spot" && config.mode == "replay"){
            spot_feed = make_unique<BinanceSpotReplayFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode == "replay"){
            perps_feed = make_unique<BinanceFuturesReplayFeed>(config, perps_state, execution_event, perps_recorder, clock);
        }

        else if(config.exchange == "polymarket"){
            spot_feed = make_unique<PolymarketFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }

        else if(config.exchange == "hyperliquid"){
            spot_feed = make_unique<HyperLiquidFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }


        // Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& maker_execution, Execution& hedge_execution, 
        
        
        // BinanceClock& clock,
        // ExecutionEventQueue& execution_event, EventNotifier& dashboard_event, SnapshotStore& snapshot_store, 
        
        // DatasetRecorder& maker_recorder, DatasetRecorder& hedge_recorder)
        // : config(config), state(state), strategy(strategy), maker_execution(maker_execution), hedge_execution(hedge_execution), 
        
        // clock(clock), execution_event(execution_event),
        // dashboard_event(dashboard_event), snapshot_store(snapshot_store), maker_recorder(maker_recorder), hedge_recorder(hedge_recorder) {build_header();}

        engine = make_unique<Engine>(config, spot_state, perps_state, strategy, *spot_execution, *perps_execution, 
            
            
            clock, execution_event, dashboard_event, snapshot_store, spot_recorder, perps_recorder);
    }

    void start(){
        execution_loop_running = true;
        dashboard_loop_running = true;

        start_signal_handler();
        threads.emplace_back([this](){ioc.run();});

        clock.start();

        // dashboard_terminal.start();
        dashboard_server.start();

        // Start feeds / streams (only those initialized)
        if(spot_feed) spot_feed->start();
        if(perps_feed) perps_feed->start();

        if(spot_user_stream) spot_user_stream->start();
        if(perps_user_stream) perps_user_stream->start();

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
            while(true){
                {
                    unique_lock<mutex> lock(dashboard_event.signal_mtx);
                    dashboard_event.signal_cv.wait(lock, [this]{
                        return dashboard_event.signal_pending || !dashboard_loop_running;});

                    if(!dashboard_loop_running) break;
                    dashboard_event.signal_pending = false;
                }

                // dashboard_terminal.refresh();
                dashboard_server.publish();
            }
        });
    }

    void start_execution_loop(){
        threads.emplace_back([this](){
            while(true){
                ExecutionEvent ev;

                if(!execution_event.pop(ev, execution_loop_running)) break;

                engine->process_event(ev);
            }
        });
    }

    void start_execution_latency_loop(){ //polling driven
        threads.emplace_back([this](){
            while(true){
                ExecutionEvent ev;
                bool state_changed = false;

                if(execution_event.pop_timeout(ev, execution_loop_running, 1ms)){
                    engine->process_event_latency(ev);
                    state_changed = true;
                }
                else if(!execution_loop_running) break;

                // if(execution->process_latency_queue()){
                //     state_changed = true;
                // }

                if(spot_execution->process_latency_queue() || perps_execution->process_latency_queue()){
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

    void start_signal_handler(){
        signals.async_wait([this](const boost::system::error_code& ec, int signal){
            if(ec) return;

            {
                lock_guard<mutex> lock(shutdown_mtx);
                shutdown_requested = true;
            }
            shutdown_cv.notify_one();
            }
        );
    }

    void wait_for_shutdown(){
        unique_lock<mutex> lock(shutdown_mtx);
        shutdown_cv.wait(lock, [this]{return shutdown_requested.load();});

        shutdown();
    }

    void shutdown(){
        cout << "INTERRUPT RECEIVED - SHUTTING DOWN\n";

        if(spot_feed) spot_feed->stop();
        if(perps_feed) perps_feed->stop();

        //--------------------------------------------------
        // engine
        //--------------------------------------------------
        engine->stop();

        //--------------------------------------------------
        // execution loop
        //--------------------------------------------------
        execution_loop_running = false;
        execution_event.cv.notify_all();

        //--------------------------------------------------
        // dashboards
        //--------------------------------------------------
        cout << "STOPPING DASH\n";
        dashboard_loop_running = false;
        dashboard_event.signal_cv.notify_all();

        // dashboard_terminal.stop();
        dashboard_server.stop();

        clock.stop();

        if(spot_broker) spot_broker->stop();
        if(perps_broker) perps_broker->stop();

        if(spot_user_stream) spot_user_stream->stop();
        if(perps_user_stream) perps_user_stream->stop();

        ioc.stop();

        for(auto& t: threads) if(t.joinable()) t.join();

        spot_recorder.stop();
        perps_recorder.stop();
    }
};

int main(){
    std_string path1 = "D://OneDrive//Trading//Market Making//manifest.json";
    std_string path2 = "C://Users//brian//OneDrive//Trading//Market Making//manifest.json";

    ifstream f(path1);

    if(!f.is_open()){
        f.open(path2);
        // cout << path2 << "\n";
    }
    // else cout << path1 << "\n";

    if(!f.is_open()){
        cerr << "Cannot open manifest\n";
        return 1;
    }

    ordered_json params;
    f >> params;

    TradingSystem system(params);

    system.start();
    system.wait_for_shutdown();
    return 0;
}