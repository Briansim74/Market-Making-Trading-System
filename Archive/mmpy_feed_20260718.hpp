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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>

#include <curl/curl.h>
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

#include "mmpy_structs.hpp" //structs
#include "mmpy_config_orderbook.hpp" //market config & orderbook
#include "mmpy_state.hpp" //state & market_feature_state

#include "mmpy_clock.hpp" // clock

#include <boost/beast/http.hpp>

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

namespace http = boost::beast::http;

class Feed {
public:
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual ~Feed() = default;
};

class BinanceSpotFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;
    std_string instrument;
    std_string instrument_upper;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    vector<Depth> depth_buffer;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    BinanceSpotFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))
                    {
                        instrument = params["instrument"].get<std_string>();
                        instrument_upper = params["instrument"].get<std_string>();
                        transform(instrument_upper.begin(), instrument_upper.end(), instrument_upper.begin(), [](unsigned char c){return toupper(c);});
                    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binance.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binance.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binance.com", "/ws/" + instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "TRADE WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = uint64_t(doc["T"]);
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.ts);

            log_event("trade", trade.ts, msg);

            ExecutionEvent ev;
            ev.type = ExecutionEventType::TRADE_UPDATE;
            ev.trade = trade;
            execution_event.push(ev);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binance.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binance.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binance.com", "/ws/" + instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "DEPTH WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = uint64_t(doc["E"]);
            depth.U = uint64_t(doc["U"]);
            depth.u = uint64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", depth.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(depth);
                    continue;
                }
            }
            
            ExecutionEvent ev;
            ev.type = ExecutionEventType::DEPTH_UPDATE_SPOT;
            ev.depth = depth;
            execution_event.push(ev);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceSpotFeed::trade_loop, this);
        depth_thread = thread(&BinanceSpotFeed::depth_loop, this);

        cout << "LIVE SPOT SOCKETS STARTED\n";

        // wait for first message
        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{ return first_depth_received.load(); });
        }

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance(instrument_upper, 1000);

        log_orderbook_snapshot(snapshot);

        // -------------------------
        // WAIT FOR STREAM ALIGNMENT (YOUR GATE FIX)
        // -------------------------
        bool valid = false;

        for(int i = 0; i < 500; i++){
            {
                lock_guard<mutex> lock(buffer_mtx);

                if(!depth_buffer.empty() && depth_buffer.back().u > snapshot_id){
                    valid = true;
                    break;
                }
            }
            this_thread::sleep_for(milliseconds(10));
        }

        if(!valid) throw runtime_error("Stream not aligned (no post-snapshot events)");
        
        vector<Depth> buffered;
        {
            lock_guard lock(buffer_mtx);
            buffered = depth_buffer;   // copy, don't swap
        }
        
        sort(buffered.begin(), buffered.end(), [](const auto& a, const auto& b) {return a.U < b.U;});

        auto it = find_if(buffered.begin(), buffered.end(), [&](const Depth& d){
            return d.U <= snapshot_id + 1 && snapshot_id + 1 <= d.u;});

        if(it == buffered.end()) throw runtime_error("Couldn't synchronize order book");
        
        // -------------------------
        // APPLY REPLAY
        // -------------------------
        for(; it != buffered.end(); ++it){
            cout << "BUFFER U: " << it->U << " snapshot_id + 1: " << snapshot_id + 1 << " u: " << it->u << "\n";

            state.market_book.apply_delta(*it);
            state.market_book.last_update_id = it->u;
            state.update_vol();
        }

        cout << "BOOK SYNCHRONIZED\n";
        
        // -------------------------
        // LIVE MODE
        // -------------------------
        {
            lock_guard<mutex> lock(buffer_mtx);
            buffering = false;
        }
        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING BINANCE FEED\n";

        running = false;

        if(trade_thread.joinable()) trade_thread.join();
        if(depth_thread.joinable()) depth_thread.join();

        cout << "BINANCE FEED STOPPED\n";
    }
};

class BinanceFuturesFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;
    std_string instrument;
    std_string instrument_upper;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    vector<Depth> depth_buffer;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    BinanceFuturesFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))
                    {
                        instrument = params["instrument"].get<std_string>();
                        instrument_upper = params["instrument"].get<std_string>();
                        transform(instrument_upper.begin(), instrument_upper.end(), instrument_upper.begin(), [](unsigned char c){return toupper(c);});
                    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void trade_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binancefuture.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binancefuture.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binancefuture.com", "/ws/" + instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "TRADE WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = uint64_t(doc["T"]);
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.ts);

            log_event("trade", trade.ts, msg);

            ExecutionEvent ev;
            ev.type = ExecutionEventType::TRADE_UPDATE;
            ev.trade = trade;
            execution_event.push(ev);
        }
    }

    void depth_loop(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve("stream.binancefuture.com", "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), "stream.binancefuture.com");
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws_stream ws(move(ssl_sock));
        ws.handshake("stream.binancefuture.com", "/ws/" + instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            ws.read(buffer, ec);
            if(ec){
                cout << "DEPTH WS ERROR: " << ec.message() << endl;
                break;
            }

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = uint64_t(doc["E"]);
            depth.pu = uint64_t(doc["pu"]);
            depth.U = uint64_t(doc["U"]);
            depth.u = uint64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", depth.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(depth);
                    continue;
                }
            }

            ExecutionEvent ev;
            ev.type = ExecutionEventType::DEPTH_UPDATE_FUTURES;
            ev.depth = depth;
            execution_event.push(ev);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceFuturesFeed::trade_loop, this);
        depth_thread = thread(&BinanceFuturesFeed::depth_loop, this);

        cout << "LIVE FUTURES SOCKETS STARTED\n";

        // wait for first message
        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{ return first_depth_received.load(); });
        }

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance(instrument_upper, 1000);

        log_orderbook_snapshot(snapshot);

        // -------------------------
        // WAIT FOR STREAM ALIGNMENT (YOUR GATE FIX)
        // -------------------------
        bool valid = false;

        for(int i = 0; i < 500; i++){
            {
                lock_guard<mutex> lock(buffer_mtx);
                if(!depth_buffer.empty() && depth_buffer.back().u > snapshot_id){
                    valid = true;
                    break;
                }
            }
            this_thread::sleep_for(milliseconds(10));
        }

        if(!valid) throw runtime_error("Stream not aligned (no post-snapshot events)");

        vector<Depth> buffered;
        {
            lock_guard lock(buffer_mtx);
            buffered = depth_buffer;   // copy, don't swap
        }
        
        sort(buffered.begin(), buffered.end(), [](const auto& a, const auto& b) {return a.U < b.U;});

        auto it = find_if(buffered.begin(), buffered.end(), [&](const Depth& d){
            return d.U <= snapshot_id && snapshot_id <= d.u;});

        if(it == buffered.end()) throw runtime_error("Couldn't synchronize order book");

        // -------------------------
        // APPLY REPLAY
        // -------------------------
        for(; it != buffered.end(); ++it){
            cout << "BUFFER pu: " << it->pu << " U: " << it->U << " snapshot_id: "<< snapshot_id << " u: " << it->u << '\n';

            state.market_book.apply_delta(*it);
            state.market_book.last_update_id = it->u;
            state.update_vol();
        }

        cout << "BOOK SYNCHRONIZED\n";
        
        // -------------------------
        // LIVE MODE
        // -------------------------
        {
            lock_guard<mutex> lock(buffer_mtx);
            buffering = false;
        }
        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    // =========================
    // STOP
    // =========================
    void stop() override {
        cout << "STOPPING BINANCE FEED\n";

        running = false;

        if(trade_thread.joinable()) trade_thread.join();
        if(depth_thread.joinable()) depth_thread.join();

        cout << "BINANCE FEED STOPPED\n";
    }
};

class BinanceSpotReplayFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    std_string events_path;
    std_string orderbook_snapshot_path;
    vector<EventsRow> events;
    json orderbook_snapshot;

    function<void(const Trade&)> on_trade_event;
    function<void()> on_depth_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;

    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0;
    optional<uint64_t> last_ts;
    double speed_multiplier = 5.0;

    simdjson::ondemand::parser parser;
    
    BinanceSpotReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))
    {
        events_path   = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["events"].get<std_string>();

        orderbook_snapshot_path = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();

        initialize();
    }

    void initialize(){
        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventsRow e;
            e.type = type_col->GetString(i);
            e.ts = static_cast<uint64_t>(ts_col->Value(i));
            e.msg = msg_col->GetString(i);
            events.push_back(e);
        }

        // stable_sort(events.begin(), events.end(), [](const auto& a, const auto& b) {return a.ts < b.ts;});

        cout << "Events: " << n << "\n";
    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void on_trade_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = uint64_t(doc["T"]);
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = 0;
        
        log_event("trade", trade.ts, msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = uint64_t(doc["E"]);
        depth.U  = uint64_t(doc["U"]);
        depth.u  = uint64_t(doc["u"]);
        depth.latency = 0;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", depth.ts, msg);
        
        if(!snapshot_aligned.load()){
            if(depth.U <= state.market_book.last_update_id + 1 && state.market_book.last_update_id + 1 <= depth.u){
                cout << "BUFFER U: " << depth.U << " snapshot_id + 1: " << state.market_book.last_update_id + 1 << " u: " << depth.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(depth);
                state.market_book.last_update_id = depth.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        ExecutionEvent ev;
        ev.type = ExecutionEventType::DEPTH_UPDATE_SPOT;
        ev.depth = depth;
        execution_event.push(ev);
    }

    void run(){
        while(running && i < events.size()){
            auto& event = events[i];

            if(last_ts.has_value()){
                double dt = (event.ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = event.ts;

            if(event.type == "trade") on_trade_message(event.msg);
            else if(event.type == "depth") on_depth_message(event.msg);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY SPOT SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        log_orderbook_snapshot(snapshot);

        replay_thread = thread(&BinanceSpotReplayFeed::run, this);

        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{return first_depth_received.load(); });
        }

        while(!snapshot_aligned.load()) this_thread::sleep_for(milliseconds(1));

        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING REPLAY\n";
        running = false;

        if(replay_thread.joinable()) replay_thread.join();

        cout << "REPLAY STOPPED\n";
    }
};

class BinanceFuturesReplayFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;

    std_string events_path;
    std_string orderbook_snapshot_path;
    vector<EventsRow> events;
    json orderbook_snapshot;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread replay_thread;

    mutex cv_mtx; //condition variable lock
    condition_variable cv;
    
    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    size_t i = 0;
    optional<uint64_t> last_ts;
    double speed_multiplier = 5.0;

    simdjson::ondemand::parser parser;
    
    BinanceFuturesReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, BinanceClock& clock,
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), execution_event(execution_event), clock(clock), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot))

    {
        events_path   = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["events"].get<std_string>();

        orderbook_snapshot_path = params["folder_path"].get<std_string>() + "/" +
                        params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();

        initialize();
    }

    void initialize(){
        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto msg_col = static_pointer_cast<StringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventsRow e;
            e.type = type_col->GetString(i);
            e.ts = static_cast<uint64_t>(ts_col->Value(i));
            e.msg = msg_col->GetString(i);
            events.push_back(e);
        }

        // stable_sort(events.begin(), events.end(), [](const auto& a, const auto& b) {return a.ts < b.ts;}); //assuming not sorted yet

        cout << "Events: " << n << "\n";
    }

    void parse_book(simdjson::ondemand::object obj, Depth& entry){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            entry.ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void on_trade_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = uint64_t(doc["T"]);
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = 0;

        log_event("trade", trade.ts, msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = uint64_t(doc["E"]);
        depth.pu = uint64_t(doc["pu"]);
        depth.U  = uint64_t(doc["U"]);
        depth.u  = uint64_t(doc["u"]);
        depth.latency = 0;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", depth.ts, msg);
        
        if(!snapshot_aligned.load()){
            if(depth.U <= state.market_book.last_update_id && state.market_book.last_update_id <= depth.u){
                cout << "BUFFER pu: " << depth.pu << " U: " << depth.U << " snapshot_id: " << state.market_book.last_update_id << " u: " << depth.u << "\n";
                snapshot_aligned = true;

                state.market_book.apply_delta(depth);
                state.market_book.last_update_id = depth.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        ExecutionEvent ev;
        ev.type = ExecutionEventType::DEPTH_UPDATE_FUTURES;
        ev.depth = depth;
        execution_event.push(ev);
    }

    void run(){
        while(running && i < events.size()){
            auto& event = events[i];

            if(last_ts.has_value()){
                double dt = (event.ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / speed_multiplier)));
            }

            last_ts = event.ts;

            if(event.type == "depth") on_depth_message(event.msg);
            else if(event.type == "trade") on_trade_message(event.msg);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY FUTURES SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        log_orderbook_snapshot(snapshot);

        replay_thread = thread(&BinanceFuturesReplayFeed::run, this);

        {
            unique_lock<mutex> lock(cv_mtx);
            cv.wait_for(lock, 5s, [&]{return first_depth_received.load(); });
        }

        while(!snapshot_aligned.load()) this_thread::sleep_for(milliseconds(1));

        state.initialized = true;

        cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING REPLAY\n";
        running = false;

        if(replay_thread.joinable()) replay_thread.join();

        cout << "REPLAY STOPPED\n";
    }
};