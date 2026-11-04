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
#include "mm_markprice.hpp" // funding rate

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
    
    Execution& spot_execution;
    Execution& perps_execution;

    // NEW
    State& maker_state;
    State& hedge_state;
    Execution& maker_execution;
    Execution& hedge_execution;

    BinanceClock& clock;
    ExecutionEventQueue& execution_event;
    EventNotifier& dashboard_event;
    SnapshotStore& snapshot_store;
    
    DatasetRecorder& spot_recorder;
    DatasetRecorder& perps_recorder;

    atomic<bool> spot_execution_enabled = false;
    atomic<bool> perps_execution_enabled = false;

    atomic<bool> trading_enabled = true;
    atomic<bool> waiting_for_cancel = false;
    atomic<bool> waiting_for_flatten = false;

    atomic<bool> shutdown_complete = false;
    
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

    std_string header;
    
    Engine(MarketConfig& config, State& spot_state, State& perps_state, MarketMakingStrategy& strategy, Execution& spot_execution, Execution& perps_execution, BinanceClock& clock,
        ExecutionEventQueue& execution_event, EventNotifier& dashboard_event, SnapshotStore& snapshot_store, DatasetRecorder& spot_recorder, DatasetRecorder& perps_recorder)
        : config(config), spot_state(spot_state), perps_state(perps_state), strategy(strategy), spot_execution(spot_execution), perps_execution(perps_execution), 
        
        maker_state(config.market == "spot" ? spot_state : perps_state),
        hedge_state(config.market == "spot" ? perps_state : spot_state),

        maker_execution(config.market == "spot" ? spot_execution : perps_execution),
        hedge_execution(config.market == "spot" ? perps_execution : spot_execution),
        
        
        clock(clock),
        execution_event(execution_event), dashboard_event(dashboard_event), snapshot_store(snapshot_store), spot_recorder(spot_recorder), perps_recorder(perps_recorder)
        {
            initialize();
            build_header();
        }
    
    void initialize(){
        if(config.market == "spot"){
            spot_execution_enabled = true;
            if(config.hedge_enabled) perps_execution_enabled = true;
        }
        else if(config.market != "spot"){
            perps_execution_enabled = true;
            if(config.hedge_enabled) spot_execution_enabled = true;
        }
    }

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
                on_depth_event_perps(ev.depth);
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

            case ExecutionEventType::MARKPRICE_UPDATE:
                on_markprice_event_perps(ev.stream);
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
                on_depth_event_perps_latency(ev.depth);
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
        if(config.market == "spot"){  // SPOT MAKER
            Signal signal = strategy.generate_quotes(spot_state);
            spot_recorder.log_snapshot(signal);
            spot_state.last_signal = signal;

            if(!trading_enabled) return;

            spot_execution.place_quotes(signal);
        }

        else{ // SPOT HEDGE
            Signal hedge_signal = strategy.calculate_hedge(perps_state, spot_state);
            spot_recorder.log_snapshot(hedge_signal);
            spot_state.last_hedge_signal = hedge_signal;

            if(!trading_enabled || !spot_execution_enabled) return;

            spot_execution.place_quotes(hedge_signal);
        }
    }

    void on_depth_event_perps(const Depth& depth){
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

        if(config.market != "spot"){  // PERPS MAKER
            Signal signal = strategy.generate_quotes(perps_state); // TO GENERATE SIGNAL FOR MAKER, BUT HEDGE SIGNAL IS BASIC SIGNALS ONLY
            perps_recorder.log_snapshot(signal);
            perps_state.last_signal = signal;

            if(!trading_enabled) return;

            perps_execution.place_quotes(signal);
        }

        else{ // PERPS HEDGE
            Signal hedge_signal = strategy.calculate_hedge(spot_state, perps_state);
            perps_recorder.log_snapshot(hedge_signal); // CHANGE TO LOG HEDGE SNAPSHOTS / NEW FEATURES???
            perps_state.last_hedge_signal = hedge_signal;

            if(!trading_enabled || !perps_execution_enabled) return; // for hedging / letting perps feed keep running while hedging is disabled

            perps_execution.place_quotes(hedge_signal);
        }
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

        if(config.market == "spot"){  // SPOT MAKER
            Signal signal = strategy.generate_quotes(spot_state);
            spot_recorder.log_snapshot(signal);
            spot_state.last_signal = signal;

            if(!trading_enabled) return;

            spot_execution.place_quotes_latency(signal);
        }

        else{ // SPOT HEDGE
            Signal hedge_signal = strategy.calculate_hedge(perps_state, spot_state);
            spot_recorder.log_snapshot(hedge_signal);
            spot_state.last_hedge_signal = hedge_signal;

            if(!trading_enabled || !spot_execution_enabled) return;
        }
    }

    void on_depth_event_perps_latency(const Depth& depth){
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

        if(config.market != "spot"){  // PERPS MAKER
            Signal signal = strategy.generate_quotes(perps_state); // TO GENERATE SIGNAL FOR MAKER, BUT HEDGE SIGNAL IS BASIC SIGNALS ONLY

            perps_recorder.log_snapshot(signal);
            perps_state.last_signal = signal;

            if(!trading_enabled) return; // for hedging

            perps_execution.place_quotes_latency(signal);
        }

        else{ // PERPS HEDGE
            Signal hedge_signal = strategy.calculate_hedge(spot_state, perps_state);
            perps_recorder.log_snapshot(hedge_signal);
            perps_state.last_hedge_signal = hedge_signal;

            if(!trading_enabled || !perps_execution_enabled) return;
        }
    }

    void on_stream_event_spot(const Stream& stream){
        spot_state.time = stream.local_ts;
        spot_execution.apply_stream_update(stream);

        wait_for_cancel();
        wait_for_flatten();
        
        if(!trading_enabled) return;

        // spot is maker, standard spot execution
        if(config.market == "spot"){
            spot_execution.place_quotes(*spot_state.last_signal);

            // hedge is perps and enabled
            if(config.hedge_enabled){
                Signal hedge_signal = strategy.calculate_hedge(spot_state, perps_state);
                perps_recorder.log_snapshot(hedge_signal);
                perps_state.last_hedge_signal = hedge_signal;

                perps_execution.place_quotes(hedge_signal);
            }
        }

        // perps is maker, spot hedge reacts to any changes in hedge state
        else{
            Signal hedge_signal = strategy.calculate_hedge(perps_state, spot_state);
            spot_recorder.log_snapshot(hedge_signal);
            spot_state.last_hedge_signal = hedge_signal;
        
            spot_execution.place_quotes(hedge_signal);
        }
    }

    void on_stream_event_perps(const Stream& stream){
        perps_state.time = stream.local_ts;
        perps_execution.apply_stream_update(stream);

        wait_for_cancel();
        wait_for_flatten();
        
        if(!trading_enabled) return;

        // perps is maker, standard perps execution
        if(config.market != "spot"){
            perps_execution.place_quotes(*perps_state.last_signal);

            // hedge is spot and enabled
            if(config.hedge_enabled){ // spot hedge reacts to all perps stream events
                Signal hedge_signal = strategy.calculate_hedge(perps_state, spot_state);
                spot_recorder.log_snapshot(hedge_signal);
                spot_state.last_hedge_signal = hedge_signal;

                spot_execution.place_quotes(hedge_signal);
            }
        }

        // spot is maker, perps hedge reacts to any changes in hedge state
        else{
            Signal hedge_signal = strategy.calculate_hedge(spot_state, perps_state);
            perps_recorder.log_snapshot(hedge_signal);
            perps_state.last_hedge_signal = hedge_signal;
            
            perps_execution.place_quotes(hedge_signal);
        }
    }

    void on_markprice_event_perps(const Stream& stream){
        perps_state.funding_rate = stream.funding_rate;
        perps_state.next_funding_ts = stream.next_funding_ts;
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

        if(spot_execution_enabled) spot_execution.cancel_all_orders();
        if(perps_execution_enabled) perps_execution.cancel_all_orders();

        // spot_execution.cancel_all_orders(); // to be changed to spot + perps
        waiting_for_cancel = true;
        if(config.mode == "stochastic" || config.mode == "replay"){
            wait_for_cancel();
            wait_for_flatten();
        }
        cout << "CANCELLING OPEN ORDERS1\n";
    }

    void wait_for_cancel(){
        if(waiting_for_cancel){
            if(spot_execution_enabled && perps_execution_enabled && !spot_execution.get_open_order("BUY") && !spot_execution.get_open_order("SELL")
                && !perps_execution.get_open_order("BUY") && !perps_execution.get_open_order("SELL")){
                
                cout << "NO MORE OPEN SPOT & PERP ORDERS, PLACING MARKET\n";
                waiting_for_cancel = false;
                waiting_for_flatten = true;

                spot_execution.place_market();
                perps_execution.place_market();
                cout << "wait_for_cancel done\n";
            }

            else if(spot_execution_enabled && !perps_execution_enabled && !spot_execution.get_open_order("BUY") && !spot_execution.get_open_order("SELL")){
                
                cout << "NO MORE OPEN SPOT ORDERS, PLACING MARKET\n";
                waiting_for_cancel = false;
                waiting_for_flatten = true;

                spot_execution.place_market();
                cout << "wait_for_cancel done\n";
            }

            else if(perps_execution_enabled && !spot_execution_enabled && !perps_execution.get_open_order("BUY") && !perps_execution.get_open_order("SELL")){
                
                cout << "NO MORE OPEN PERP ORDERS, PLACING MARKET\n";
                waiting_for_cancel = false;
                waiting_for_flatten = true;

                perps_execution.place_market();
                cout << "wait_for_cancel done\n";
            }
        }
    }

    void wait_for_flatten(){

        // Already within [-10, +10] USDT
        if(waiting_for_flatten){
            if(spot_execution_enabled && perps_execution_enabled && abs(spot_state.inventory * spot_state.market_book.mid()) <= config.target_notional &&
                abs(perps_state.inventory * perps_state.market_book.mid()) <= config.target_notional){ // to be changed to spot + perps
            
                cout << "abs(spot_state.inventory * spot_state.market_book.mid(): " << abs(spot_state.inventory * spot_state.market_book.mid()) << "\n";
                cout << "abs(perps_state.inventory * perps_state.market_book.mid(): " << abs(perps_state.inventory * perps_state.market_book.mid()) << "\n";
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

            else if(spot_execution_enabled && !perps_execution_enabled && abs(spot_state.inventory * spot_state.market_book.mid()) <= config.target_notional){
                
                cout << "abs(spot_state.inventory * spot_state.market_book.mid(): " << abs(spot_state.inventory * spot_state.market_book.mid()) << "\n";
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

            else if(perps_execution_enabled && !spot_execution_enabled && abs(perps_state.inventory * perps_state.market_book.mid()) <= config.target_notional){
                
                cout << "abs(perps_state.inventory * perps_state.market_book.mid(): " << abs(perps_state.inventory * perps_state.market_book.mid()) << "\n";
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
            order->side, config.from_tick(order->price_tick, config.market), order->remaining, order->status) : "—";
    }

    std_string orderOptionalToString(const optional<Order>& order){
        return order ? format("{:<5} | {:>10.10f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick, config.market), order->remaining, order->status) : "—";
    }

    Snapshot build_snapshot(){ // TO CHANGE TO ALL DERIVED FROM LAST SIGNALS
        Snapshot snap;

        snap.title.header = header;
        snap.title.regime = maker_state.last_signal->regime;
        snap.title.pnl_pct = maker_state.get_pnl(maker_state.last_signal->mid) / config.markets[config.market].initial_cash * 100;

        // MAKER
        double maker_bid_queue = (maker_state.bid_queue_ahead.first == maker_state.last_signal->bid_tick) ? maker_state.bid_queue_ahead.second : 0.0;
        double maker_ask_queue = (maker_state.ask_queue_ahead.first == maker_state.last_signal->ask_tick) ? maker_state.ask_queue_ahead.second : 0.0;

        snap.maker.market.instrument = config.markets[config.market].instrument;
        snap.maker.market.mid = maker_state.last_signal->mid;
        snap.maker.market.microprice = maker_state.last_signal->microprice;
        snap.maker.market.spread = maker_state.last_signal->spread;
        snap.maker.market.best_bid = maker_state.last_signal->best_bid;
        snap.maker.market.best_ask = maker_state.last_signal->best_ask;
        snap.maker.market.bid_size = maker_state.last_signal->bid_size;
        snap.maker.market.ask_size = maker_state.last_signal->ask_size;
        snap.maker.market.ewma_vol = maker_state.last_signal->volatility;
        snap.maker.market.order_imbalance = maker_state.last_signal->order_imbalance;
        snap.maker.market.trade_imbalance = maker_state.last_signal->trade_imbalance;
        snap.maker.market.trade = tradeToString(maker_state.last_trade);

        snap.maker.regime.regime = maker_state.last_signal->regime;
        snap.maker.regime.confidence = maker_state.last_signal->regime_prob;

        snap.maker.signals.fair =  maker_state.last_signal->fair;
        snap.maker.signals.skew = maker_state.last_signal->skew;
        snap.maker.signals.reservation = maker_state.last_signal->reservation;
        snap.maker.signals.alpha_trade_imb = maker_state.last_signal->alpha_trade_imb;
        snap.maker.signals.alpha_struct = maker_state.last_signal->alpha_struct;
        snap.maker.signals.alpha_residual = maker_state.last_signal->alpha_residual;
        snap.maker.signals.spread_multiplier = maker_state.last_signal->spread_multiplier;
        snap.maker.signals.inventory_target = maker_state.last_signal->inventory_target;
        snap.maker.signals.tox = maker_state.last_signal->toxicity.tox;
        snap.maker.signals.k_spread = maker_state.last_signal->toxicity.k_spread;
        snap.maker.signals.k_order_size = maker_state.last_signal->toxicity.k_order_size;
        snap.maker.signals.residual_signal_quality = maker_state.last_signal->residual_signal_quality;
        snap.maker.signals.toxicity_signal_quality = maker_state.last_signal->toxicity_signal_quality;
        
        snap.maker.quotes.my_spread = maker_state.last_signal->my_spread;
        snap.maker.quotes.my_bid = maker_execution.get_last_bid();
        snap.maker.quotes.my_ask = maker_execution.get_last_ask();
        snap.maker.quotes.current_bid_size = maker_execution.get_current_bid_size();
        snap.maker.quotes.current_ask_size = maker_execution.get_current_ask_size();

        snap.maker.execution.bid_queue = maker_bid_queue;
        snap.maker.execution.ask_queue = maker_ask_queue;
        snap.maker.execution.bid_pressure = maker_bid_queue / (maker_state.last_signal->bid_size + 1e-9);
        snap.maker.execution.ask_pressure = maker_ask_queue / (maker_state.last_signal->ask_size + 1e-9);

        snap.maker.execution.buy_order = orderPointerToString(maker_execution.get_open_order("BUY"));
        snap.maker.execution.sell_order = orderPointerToString(maker_execution.get_open_order("SELL"));
        snap.maker.execution.last_fill_candidate = orderOptionalToString(maker_state.last_fill_candidate);
        snap.maker.execution.last_order_update = orderOptionalToString(maker_state.last_order_update);
        
        snap.maker.risk.inventory = maker_state.inventory * maker_state.last_signal->mid; // USDT normalized
        snap.maker.risk.realized_pnl =  maker_state.last_signal->realized_pnl;
        snap.maker.risk.unrealized_pnl = maker_state.last_signal->unrealized_pnl;
        snap.maker.risk.fees_paid =  maker_state.last_signal->fees_paid;
        snap.maker.risk.total_pnl =  maker_state.last_signal->total_pnl;

        snap.maker.system.time = config.format_ms_precise(maker_state.time);
        snap.maker.system.last_trade_ts = config.format_ms_precise(maker_state.last_trade_ts);
        snap.maker.system.last_depth_ts = config.format_ms_precise(maker_state.last_depth_ts);
        snap.maker.system.trade_latency = maker_state.trade_latency;
        snap.maker.system.depth_latency = maker_state.depth_latency;
        snap.maker.system.exchange_latency = maker_state.exchange_latency;

        // HEDGE
        double hedge_bid_queue = (hedge_state.bid_queue_ahead.first == hedge_state.last_hedge_signal->bid_tick) ? hedge_state.bid_queue_ahead.second : 0.0;
        double hedge_ask_queue = (hedge_state.ask_queue_ahead.first == hedge_state.last_hedge_signal->ask_tick) ? hedge_state.ask_queue_ahead.second : 0.0;

        snap.hedge.market.instrument = config.markets[config.hedge].instrument;
        snap.hedge.market.mid = hedge_state.last_hedge_signal->hedge_mid;
        snap.hedge.market.microprice = hedge_state.last_hedge_signal->microprice;
        snap.hedge.market.spread = hedge_state.last_hedge_signal->spread;
        snap.hedge.market.best_bid = hedge_state.last_hedge_signal->best_bid;
        snap.hedge.market.best_ask = hedge_state.last_hedge_signal->best_ask;
        snap.hedge.market.bid_size = hedge_state.last_hedge_signal->bid_size;
        snap.hedge.market.ask_size = hedge_state.last_hedge_signal->ask_size;
        snap.hedge.market.ewma_vol = hedge_state.last_hedge_signal->volatility;
        snap.hedge.market.order_imbalance = hedge_state.last_hedge_signal->order_imbalance;
        snap.hedge.market.trade_imbalance = hedge_state.last_hedge_signal->trade_imbalance;
        snap.hedge.market.basis = hedge_state.last_hedge_signal->basis;
        snap.hedge.market.funding_rate =  hedge_state.last_hedge_signal->funding_rate;
        snap.hedge.market.next_funding_ts = config.format_ms_precise(hedge_state.last_hedge_signal->next_funding_ts);
        snap.hedge.market.trade = tradeToString(hedge_state.last_trade);

        snap.hedge.regime.regime = hedge_state.last_hedge_signal->regime;
        snap.hedge.regime.confidence = hedge_state.last_hedge_signal->regime_prob;

        snap.hedge.signals.maker_delta = maker_state.inventory * hedge_state.last_hedge_signal->maker_mid;
        snap.hedge.signals.hedge_delta = hedge_state.inventory * hedge_state.last_hedge_signal->hedge_mid;
        snap.hedge.signals.net_delta = snap.hedge.signals.maker_delta + snap.hedge.signals.hedge_delta;
        snap.hedge.signals.hedge_threshold = config.hedge_threshold;
        snap.hedge.signals.hedge_ratio = config.hedge_ratio;
        snap.hedge.signals.target_hedge = hedge_state.last_hedge_signal->target_hedge;
        snap.hedge.signals.hedge_decision = hedge_state.last_hedge_signal->hedge_decision;
        
        snap.hedge.quotes.my_spread = hedge_state.last_hedge_signal->my_spread;
        snap.hedge.quotes.my_bid = hedge_execution.get_last_bid();
        snap.hedge.quotes.my_ask = hedge_execution.get_last_ask();
        snap.hedge.quotes.current_bid_size = hedge_execution.get_current_bid_size();
        snap.hedge.quotes.current_ask_size = hedge_execution.get_current_ask_size();

        snap.hedge.execution.bid_queue = hedge_bid_queue;
        snap.hedge.execution.ask_queue = hedge_ask_queue;
        snap.hedge.execution.bid_pressure = hedge_bid_queue / (hedge_state.last_hedge_signal->bid_size + 1e-9);
        snap.hedge.execution.ask_pressure = hedge_ask_queue / (hedge_state.last_hedge_signal->ask_size + 1e-9);

        snap.hedge.execution.buy_order = orderPointerToString(hedge_execution.get_open_order("BUY"));
        snap.hedge.execution.sell_order = orderPointerToString(hedge_execution.get_open_order("SELL"));
        snap.hedge.execution.last_fill_candidate = orderOptionalToString(hedge_state.last_fill_candidate);
        snap.hedge.execution.last_order_update = orderOptionalToString(hedge_state.last_order_update);
        
        snap.hedge.risk.inventory = hedge_state.last_hedge_signal->inventory * hedge_state.last_hedge_signal->hedge_mid; // USDT normalized
        snap.hedge.risk.realized_pnl = hedge_state.last_hedge_signal->realized_pnl;
        snap.hedge.risk.unrealized_pnl = hedge_state.last_hedge_signal->unrealized_pnl;
        snap.hedge.risk.fees_paid = hedge_state.last_hedge_signal->fees_paid;
        snap.hedge.risk.funding = hedge_state.last_hedge_signal->funding;
        snap.hedge.risk.total_pnl = hedge_state.last_hedge_signal->total_pnl;

        snap.hedge.system.time = config.format_ms_precise(hedge_state.time);
        snap.hedge.system.last_trade_ts = config.format_ms_precise(hedge_state.last_trade_ts);
        snap.hedge.system.last_depth_ts = config.format_ms_precise(hedge_state.last_depth_ts);
        snap.hedge.system.trade_latency = hedge_state.trade_latency;
        snap.hedge.system.depth_latency = hedge_state.depth_latency;
        snap.hedge.system.exchange_latency = hedge_state.exchange_latency;

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
        add(config.markets[config.market].instrument);

        header = parts;
    }
};

class TradingSystem {
public:
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

    unique_ptr<Execution> spot_execution;
    unique_ptr<Execution> perps_execution;

    unique_ptr<BinanceBroker> spot_broker;
    unique_ptr<BinanceBroker> perps_broker;

    unique_ptr<BinanceUserStream> spot_user_stream;
    unique_ptr<BinanceUserStream> perps_user_stream;
    
    unique_ptr<BinanceMarkPriceStream> perps_markprice_stream;

    unique_ptr<Engine> engine;
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
        params(params), config(params), spot_state(config, "spot"), perps_state(config, (config.mode == "live") ? "perps" : "testnet_perps"),
        strategy(config), spot_recorder(config, spot_state, params, "spot"), perps_recorder(config, perps_state, params, "perps"),
        clock(config), dashboard_terminal(snapshot_store), dashboard_server(config, snapshot_store),
        signals(ioc, SIGINT, SIGTERM) {initialize();}

    void initialize(){
        if(config.mode == "live" && config.market == "spot"){
            spot_broker = make_unique<BinanceSpotBroker>(config, clock);
            spot_execution = make_unique<LiveExecution>(config, spot_state, spot_recorder, *spot_broker, clock, "spot");
            spot_user_stream = make_unique<BinanceSpotUserStream>(config, *spot_broker, execution_event, clock);

            if(config.hedge_enabled){
                perps_broker = make_unique<BinancePerpsBroker>(config, clock);
                perps_execution = make_unique<LiveExecution>(config, perps_state, perps_recorder, *perps_broker, clock, "perps");
                perps_user_stream = make_unique<BinancePerpsUserStream>(config, *perps_broker, execution_event, clock);
                
                perps_markprice_stream = make_unique<BinanceMarkPriceStream>(config, execution_event, clock);
            }
        }

        else if(config.mode == "live" && config.market == "perps"){
            perps_broker = make_unique<BinancePerpsBroker>(config, clock);
            perps_execution = make_unique<LiveExecution>(config, perps_state, perps_recorder, *perps_broker, clock, "perps");
            perps_user_stream = make_unique<BinancePerpsUserStream>(config, *perps_broker, execution_event, clock);

            if(config.hedge_enabled){
                spot_broker = make_unique<BinanceSpotBroker>(config, clock);
                spot_execution = make_unique<LiveExecution>(config, spot_state, spot_recorder, *spot_broker, clock, "spot");
                spot_user_stream = make_unique<BinanceSpotUserStream>(config, *spot_broker, execution_event, clock);
            }
        }

        else if((config.mode == "testnet" || config.mode == "live") && config.market == "testnet_perps"){
            perps_broker = make_unique<BinancePerpsBroker>(config, clock);
            perps_execution = make_unique<LiveExecution>(config, perps_state, perps_recorder, *perps_broker, clock, "testnet_perps");
            perps_user_stream = make_unique<BinancePerpsUserStream>(config, *perps_broker, execution_event, clock);
        }
        
        else if(config.mode != "live" && config.market == "spot"){
            spot_execution = make_unique<PaperExecution>(config, spot_state, spot_recorder, clock, "spot");

            if(config.hedge_enabled) perps_execution = make_unique<PaperExecution>(config, spot_state, spot_recorder, clock, "perps");
        }

        else if(config.mode != "live" && config.market == "perps"){
            perps_execution = make_unique<PaperExecution>(config, spot_state, spot_recorder, clock, "perps");

            if(config.hedge_enabled) spot_execution = make_unique<PaperExecution>(config, spot_state, spot_recorder, clock, "spot");
        }

        if(config.exchange == "binance" && config.market == "spot" && config.mode != "replay"){
            spot_feed = make_unique<BinanceSpotFeed>(config, spot_state, execution_event, spot_recorder, clock);

            if(config.hedge_enabled){
                perps_feed = make_unique<BinancePerpsFeed>(config, perps_state, execution_event, perps_recorder, clock, "perps");
            }
        }

        else if(config.exchange == "binance" && config.market == "perps" && config.mode != "replay"){
            perps_feed = make_unique<BinancePerpsFeed>(config, perps_state, execution_event, perps_recorder, clock, "perps");

            if(config.hedge_enabled){
                spot_feed = make_unique<BinanceSpotFeed>(config, spot_state, execution_event, spot_recorder, clock);
            }
        }

        else if(config.exchange == "binance" && config.market == "testnet_perps" && config.mode != "replay"){
            perps_feed = make_unique<BinancePerpsFeed>(config, perps_state, execution_event, perps_recorder, clock, "testnet_perps");
        }

        else if(config.exchange == "binance" && config.market == "spot" && config.mode == "replay"){
            spot_feed = make_unique<BinanceSpotReplayFeed>(config, spot_state, execution_event, spot_recorder, clock);

            if(config.hedge_enabled){
                perps_feed = make_unique<BinancePerpsReplayFeed>(config, perps_state, execution_event, perps_recorder, clock, "perps");
            }
        }

        else if(config.exchange == "binance" && config.market == "testnet_perps" && config.mode == "replay"){
            perps_feed = make_unique<BinancePerpsReplayFeed>(config, perps_state, execution_event, perps_recorder, clock, "testnet_perps");
        }

        else if(config.exchange == "polymarket"){
            spot_feed = make_unique<PolymarketFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }

        else if(config.exchange == "hyperliquid"){
            spot_feed = make_unique<HyperLiquidFeed>(config, spot_state, execution_event, spot_recorder, clock);
        }

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

        if(perps_markprice_stream) perps_markprice_stream->start();

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

        if(perps_markprice_stream) perps_markprice_stream->stop();

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