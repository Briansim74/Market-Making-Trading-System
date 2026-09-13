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
#include <unordered_map>
#include <condition_variable>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <filesystem>

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

class RegimeModel {};
class MicroSignalModel {};
struct FeatureRegistry {};
class MLModel {};
struct Policy {};
struct Features {};
class MarketMakingStrategy {
public:
    MarketMakingStrategy(MarketConfig&, const json&) {};
};
struct SnapshotRow {
    uint64_t ts;
    std_string symbol;

    double best_bid;
    double best_ask;
    double mid;
    double microprice;
    double microprice_dev;
    double microprice_error;

    int64_t best_bid_tick;
    int64_t best_ask_tick;
    int64_t mid_tick;

    double spread;

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
    double reservation;
    double alpha_order_imb;
    double alpha_trade_imb;
    double alpha_struct;

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
struct TradeRow {};
struct QuoteRow {};
struct FillRow {};
class DatasetRecorder {
public:
    MarketConfig& config;
    State& state;
    const json& params;

    vector<SnapshotRow> snapshots;
    vector<TradeRow> trades;
    vector<QuoteRow> quotes;
    vector<FillRow> fills;
    
    vector<EventsRow> events;
    json orderbook_snapshot;

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
        : config(config), state(state), params(params)
    {
        create_run_dir();
    }

    std_string build_file_path(const std_string& file, int file_id){
        ostringstream ss;
        ss << folder_path << '/' << file << '_' << setw(6) << setfill('0') << file_id << ".parquet";
        return ss.str();
    }

    void create_run_dir(){
        std_string current_run_path = params["folder_path"].get<std_string>();

        auto now = system_clock::now();
        time_t now_t = system_clock::to_time_t(now);

        tm tm{};
        gmtime_s(&tm, &now_t);

        ostringstream ss;
        ss << put_time(&tm, "%Y%m%d_%H%M%S");

        folder_path = current_run_path + "/runs/run_" + ss.str();
        filesystem::create_directories(folder_path);

        events_path = folder_path + "/" + params["files"]["replay_events"]["events"].get<std_string>();
        orderbook_snapshot_path = folder_path + "/" + params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();
        snapshots_path = build_file_path("snapshots", snapshots_id++);
        trades_path =build_file_path("trades", trades_id++);
        quotes_path = build_file_path("quotes", quotes_id++);
        fills_path = build_file_path("fills", fills_id++);

        cout << "events_path: " << events_path << "\n";
        cout << "orderbook_snapshot_path: " << orderbook_snapshot_path << "\n";
        cout << "snapshots_path: " << snapshots_path << "\n";
        cout << "trades_path: " << trades_path << "\n";
        cout << "quotes_path: " << quotes_path << "\n";
        cout << "fills_path: " << fills_path << "\n";
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

    queue<Signal>& signal_queue;

    std_string struct_model;
    std_string edge_model;
    std_string mode;
    std_string exchange;
    std_string instrument;

    Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& execution,
            DatasetRecorder& recorder, const json& params, queue<Signal>& signal_queue)
        : config(config), params(params), state(state), strategy(strategy), execution(execution),
        recorder(recorder), signal_queue(signal_queue)
    {
        struct_model = params["models"]["struct_model"].get<std_string>();
        edge_model   = params["models"]["edge_model"].get<std_string>();
        mode         = params["mode"].get<std_string>();
        exchange     = params["exchange"].get<std_string>();
        instrument   = params["instrument"].get<std_string>();
    }

    void on_trade_event(const Trade& trade){
        // recorder.log_trade(trade, toupper(instrument));
        // execution.process_trade(trade);
    }

    void on_depth_event(){
        // if(!state.initialized) return;
        // Signal signal = strategy.on_market_update(state);

        // if(signal_queue.size() < 1000){
        //     signal_queue.push(signal);
        // }
        // recorder.log_snapshot(state.last_depth_ts, signal.value(), toupper(instrument));
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
        snap.title.regime = "regime_test";
        snap.title.pnl_pct = 0.0;

        snap.market.mid = mid;
        snap.market.microprice = microprice;
        snap.market.spread = spread;
        snap.market.bid = best_bid;
        snap.market.ask = best_ask;
        snap.market.bid_size = bid_size;
        snap.market.ask_size = ask_size;
        snap.market.ewma_vol = state.get_vol();
        snap.market.order_imbalance = state.order_imbalance;
        snap.market.trade_imbalance = 0.0;
        snap.market.trade = tradeToString(state.last_trade);

        snap.regime.regime = "regime_test";
        snap.regime.confidence = 0.0;

        snap.signals.fair_value = 0.0;
        snap.signals.skew = 0.0;
        snap.signals.reservation = 0.0;
        snap.signals.alpha_order_imb = 0.0;
        snap.signals.alpha_trade_imb = 0.0;
        snap.signals.alpha_struct = 0.0;
        snap.signals.k0 = 0.0;
        snap.signals.spread_multiplier = 0.0;
        snap.signals.inventory_target = 0.0;
        snap.signals.tox = 0.0;
        snap.signals.k1 = 0.0;
        snap.signals.k2 = 0.0;

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

    queue<Signal> signal_queue;
    mutex signal_mtx;

    MarketConfig config;
    State state;

    MarketMakingStrategy strategy;
    DatasetRecorder recorder;

    unique_ptr<Execution> execution;

    unique_ptr<BinanceBroker> broker;
    unique_ptr<BinanceUserStream> user_stream;

    unique_ptr<Engine> engine;

    unique_ptr<Feed> feed;

    SnapshotStore snapshot_store;
    DashboardTerminal dashboard_terminal;
    DashboardServer dashboard_server;

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

        engine = make_unique<Engine>(config, state, strategy, *execution, recorder, params, signal_queue);

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
        threads.emplace_back([this](){ //placeholder fake thread
            while(engine_running){
                this_thread::sleep_for(milliseconds(1));
            }
        });
        // threads.emplace_back([this](){
        //     while(engine_running){
                // // lock_guard<mutex>lock(signal_mtx);

                // if(signal_queue.empty()){
                //     this_thread::sleep_for(milliseconds(1));
                //     continue;
                // }

                // Signal signal = signal_queue.front();

                // signal_queue.pop();

                // state.last_signal = signal;

                // execution->place_quotes(signal);

                // process_latency_queue(signal);
        //     }
        // });
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