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
    State& state;
    MarketMakingStrategy& strategy;
    Execution& execution;
    BinanceClock& clock;
    ExecutionEventQueue& execution_event;
    EventNotifier& dashboard_event;
    SnapshotStore& snapshot_store;
    DatasetRecorder& recorder;

    atomic<bool> trading_enabled = true;
    atomic<bool> waiting_for_cancel = false;
    atomic<bool> waiting_for_flatten = false;
    atomic<bool> shutdown_complete = false;
    
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

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

            case ExecutionEventType::CANCEL_UPDATE:
                on_cancel_event();
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

            case ExecutionEventType::CANCEL_UPDATE:
                on_cancel_event();
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

        if(!trading_enabled) return; // to be changed if shift to error style

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

        if(!trading_enabled) return;

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

        if(!trading_enabled) return;

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

        if(!trading_enabled) return;

        execution.place_quotes_latency(signal);
    }

    void on_stream_event(const Stream& stream){
        state.time = stream.local_ts;
        execution.apply_stream_update(stream);

        wait_for_cancel();
        wait_for_flatten();
        
        if(!trading_enabled) return;
        
        execution.place_quotes(*state.last_signal);
    }

    void on_cancel_event(){
        cout << "CANCELLING OPEN ORDERS\n";
        execution.cancel_all_orders();
        waiting_for_cancel = true;
        if(config.mode != "live"){
            wait_for_cancel();
            wait_for_flatten();
        }
    }

    void on_mark_price_event(const Stream& stream){
        state.time = stream.local_ts;
        state.mark_price = stream.price;
    }

    void wait_for_cancel(){
        if(waiting_for_cancel && (!execution.get_open_order("BUY") && !execution.get_open_order("SELL"))){
            
            cout << "NO MORE OPEN ORDERS, PLACING MARKET\n";
            waiting_for_cancel = false;
            waiting_for_flatten = true;

            execution.place_market();
        }
    }

    void wait_for_flatten(){
        if(waiting_for_flatten && abs(state.inventory) <= 1e-9){
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
        snap.signals.alpha_trade_imb = state.last_signal ? state.last_signal->alpha_trade_imb : 0.0;
        snap.signals.alpha_struct = state.last_signal ? state.last_signal->alpha_struct : 0.0;
        snap.signals.alpha_residual = state.last_signal ? state.last_signal->alpha_residual : 0.0;
        snap.signals.spread_multiplier = state.last_signal ? state.last_signal->spread_multiplier : 0.0;
        snap.signals.inventory_target = state.last_signal ? state.last_signal->inventory_target : 0.0;
        snap.signals.residual_signal_quality = state.last_signal ? state.last_signal->residual_signal_quality : 0.0;
        snap.signals.tox = state.last_signal ? state.last_signal->toxicity.tox : 0.0;
        snap.signals.k_spread = state.last_signal ? state.last_signal->toxicity.k_spread : 0.0;
        snap.signals.k_order_size = state.last_signal ? state.last_signal->toxicity.k_order_size : 0.0;

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
        
        // snap.risk.inventory = state.inventory;
        snap.risk.inventory = state.inventory * mid; // USDT normalized
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

class TradingSystem {
public:
    const ordered_json& params;
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

    atomic<bool> execution_loop_running{false};
    atomic<bool> dashboard_loop_running{false};
    atomic<bool> shutdown_requested{false};

    asio::io_context ioc;
    asio::signal_set signals;
    mutex shutdown_mtx;
    condition_variable shutdown_cv;

    vector<thread> threads;

    TradingSystem(const ordered_json& params) :
        params(params), config(params), state(config), strategy(config), recorder(config, state, params),
        clock(config), dashboard_terminal(snapshot_store), dashboard_server(config, snapshot_store),
        signals(ioc, SIGINT, SIGTERM) {initialize();}

    void initialize(){
        if(config.mode == "live" && config.market == "spot" && config.instrument == "pepeusdt"){
            broker = make_unique<BinanceSpotBroker>(config, clock);
            execution = make_unique<LiveExecution>(config, state, recorder, *broker, clock);
            user_stream = make_unique<BinanceSpotUserStream>(config, *broker, execution_event, clock);
        }

        if(config.mode == "live" && config.market == "futures"){
            broker = make_unique<BinanceFuturesBroker>(config, clock);
            execution = make_unique<LiveExecution>(config, state, recorder, *broker, clock);
            user_stream = make_unique<BinanceFuturesUserStream>(config, *broker, execution_event, clock);
        }
        
        else if(config.mode != "live"){
            execution = make_unique<PaperExecution>(config, state, recorder, clock);
        }

        if(config.exchange == "binance" && config.market == "spot" && config.mode != "replay"){
            feed = make_unique<BinanceSpotFeed>(config, state, execution_event, recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode != "replay"){
            feed = make_unique<BinanceFuturesFeed>(config, state, execution_event, recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "spot" && config.mode == "replay"){
            feed = make_unique<BinanceSpotReplayFeed>(config, state, execution_event, recorder, clock);
        }

        else if(config.exchange == "binance" && config.market == "futures" && config.mode == "replay"){
            feed = make_unique<BinanceFuturesReplayFeed>(config, state, execution_event, recorder, clock);
        }

        else if(config.exchange == "polymarket"){
            feed = make_unique<PolymarketFeed>(config, state, execution_event, recorder, clock);
        }

        engine = make_unique<Engine>(config, state, strategy, *execution, clock, execution_event, dashboard_event, snapshot_store, recorder);
    }

    void start(){
        execution_loop_running = true;
        dashboard_loop_running = true;

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
        feed->stop();

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

        if(broker) broker->stop();

        if(user_stream) user_stream->stop();

        ioc.stop();

        for(auto& t: threads) if(t.joinable()) t.join();

        recorder.stop();
    }
};

int main(){
    std_string path1 = "D://OneDrive//Trading//Market Making//manifest.json";
    std_string path2 = "C://Users//brian//OneDrive//Trading//Market Making//manifest.json";

    ifstream f(path1);

    if(!f.is_open()){
        f.open(path2);
        cout << path2 << "\n";
    }
    else cout << path1 << "\n";

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