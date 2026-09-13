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

    atomic<bool> shutdown = false;
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

            case ExecutionEventType::MARKET_UPDATE:
                on_market_event();
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

            case ExecutionEventType::MARKET_UPDATE:
                on_market_event();
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

        if(shutdown) return;

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

        if(shutdown) return;

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

        if(shutdown) return;

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

        if(shutdown) return;

        execution.place_quotes_latency(signal);
    }

    void stop(){
        shutdown = true;

        ExecutionEvent ev;
        ev.type = ExecutionEventType::CANCEL_UPDATE;
        execution_event.push(ev);
    }

    void wait_until_shutdown_complete(){
        unique_lock lock(shutdown_mtx);

        shutdown_cv.wait(lock, [&]{
            return shutdown_complete.load();
        });
    }

    void on_stream_event(const Stream& stream){
        state.time = stream.local_ts;
        execution.apply_stream_update(stream);

        cout
        << "AFTER STREAM: inv=" << state.inventory
        << " buy="
        << (execution.get_open_order("BUY") != nullptr)
        << " sell="
        << (execution.get_open_order("SELL") != nullptr)
        << "\n";

        if(waiting_for_cancel){
            if(!execution.get_open_order("BUY") && !execution.get_open_order("SELL")){
                cout << "no more open orders, placing market...\n";
                waiting_for_cancel = false;
                waiting_for_flatten = true;

                execution.place_market();
            }
        }

        if(waiting_for_flatten && abs(state.inventory) <= 1e-9){
            waiting_for_flatten = false;
            cout << "inv flattened...\n";
            {
                lock_guard<mutex> lock(shutdown_mtx);
                shutdown_complete = true;
            }
            shutdown_cv.notify_one();
        }
        
        if(shutdown) return;
        
        execution.place_quotes(*state.last_signal);
    }

    void on_cancel_event(){
        cout << "cancelling all orders...\n";
        execution.cancel_all_orders();
        waiting_for_cancel = true;
    }

    void on_market_event(){
        execution.place_market();
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
        cout << "no more new quotes placed\n";
        engine->stop();

        feed->stop();

        //--------------------------------------------------
        // cancel orders
        //--------------------------------------------------
        cout << "CLOSING OPEN POSITIONS\n";

        // engine->stop;
        // execution->cancel_all_orders();

        // ExecutionEvent ev;
        // ev.type = ExecutionEventType::CANCEL_UPDATE;
        // execution_event.push(ev);

        // cout << "CLOSING OPEN POSITIONS1\n";
        // while(execution->get_open_order("BUY") || execution->get_open_order("SELL")){
        //     this_thread::sleep_for(milliseconds(5000));
        // }
        // this_thread::sleep_for(milliseconds(3000));
        //--------------------------------------------------
        // flatten inventory
        //--------------------------------------------------
        cout << "market\n";

        // ev.type = ExecutionEventType::MARKET_UPDATE;
        // execution_event.push(ev);
        engine->wait_until_shutdown_complete();

        // while(abs(state.inventory) > 1e-9){
        //     this_thread::sleep_for(milliseconds(3000));
        // }
        cout << "\n";

        //--------------------------------------------------
        // dashboards
        //--------------------------------------------------
        cout << "dash\n";
        dashboard_running = false;
        dashboard_event.signal_cv.notify_all();

        // dashboard_terminal.stop();
        dashboard_server.stop();

        //--------------------------------------------------
        // now stop engine
        //--------------------------------------------------
        cout << "execution_event\n";
        engine_running = false;

        // wake execution thread if it is blocked in cv.wait()
        execution_event.wake();

        cout << "clock\n";
        clock.stop();

        cout << "broker\n";
        if(broker) broker->stop_keepalive();
        cout << "broker1\n";
        cout << "user_stream\n";
        if(user_stream) user_stream->stop();

        cout << "ioc\n";
        ioc.stop();

        cout << "threads\n";
        for(auto& t: threads){
            if(t.joinable()) t.join();
        }

        cout << "recorder\n";
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