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
#include "mm_state.hpp" //state & market_feature_state
#include "mm_recorder.hpp" //dataset recorder
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

class Feed {
public:
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual ~Feed() = default;
};

class PolymarketFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unique_ptr<ws_stream> market_ws;
    unique_ptr<ws_stream> depth_ws;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    vector<Depth> depth_buffer;
    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;

    thread depth_thread;
    thread market_thread;

    PolymarketFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, 
                    DatasetRecorder& recorder, BinanceClock& clock):
        config(config), state(state), recorder(recorder), execution_event(execution_event), clock(clock) {}

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

    // void parse_book(
    //     simdjson::ondemand::object obj,
    //     Depth& depth
    // ) {
    //     depth.asset_id =
    //         string(obj["asset_id"].get_string());

    //     for (auto b : obj["bids"]) {

    //         auto arr = b.get_object();

    //         double p =
    //             arr["price"].get_double_in_string();

    //         double q =
    //             arr["size"].get_double_in_string();

    //         depth.bids.emplace_back(
    //             config.to_tick(p),
    //             q
    //         );
    //     }

    //     for (auto a : obj["asks"]) {

    //         auto arr = a.get_object();

    //         double p =
    //             arr["price"].get_double_in_string();

    //         double q =
    //             arr["size"].get_double_in_string();

    //         depth.asks.emplace_back(
    //             config.to_tick(p),
    //             q
    //         );
    //     }

    //     depth.last_trade_price =
    //         obj["last_trade_price"]
    //             .get_double_in_string();
    // }

    void market_loop() {
        string hostname = "ws-subscriptions-clob.polymarket.com";
        asio::io_context ioc;

        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(hostname, "443");

        tcp::socket socket(ioc);
        asio::connect(socket, results);

        ssl_stream ssl_sock(std::move(socket), ctx);

        SSL_set_tlsext_host_name(
            ssl_sock.native_handle(),
            hostname.c_str()
        );

        ssl_sock.handshake(ssl::stream_base::client);

        market_ws = make_unique<ws_stream>(std::move(ssl_sock));

        market_ws->handshake(
            hostname,
            "/ws/market"
        );

        // Subscribe
        json sub = {
            {"type", "market"},
            {"assets_ids", {
                "95640549996905293510297955529586737178285694064878854803914367446504739764113",
                "43681720319477188597878188429504394796986333625436226983885469009299707562545",
            }}
        };

        market_ws->write(asio::buffer(sub.dump()));

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            market_ws->read(buffer, ec);

            if(ec) break;
            string msg = beast::buffers_to_string(buffer.data());
            cout << msg << "\n";
            // buffer.consume(buffer.size());

            // simdjson::padded_string json(msg);

            // auto doc = parser.iterate(json);

            // parse_market_event(doc.get_object(), msg);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        market_thread = thread(&PolymarketFeed::market_loop, this);

        cout << "LIVE SPOT SOCKETS STARTED\n";

        // // wait for first message
        // {
        //     unique_lock<mutex> lock(cv_mtx);
        //     cv.wait_for(lock, 5s, [&]{ return first_depth_received.load(); });
        // }

        // auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance();

        // recorder.export_orderbook_snapshot(snapshot);

        // // -------------------------
        // // WAIT FOR STREAM ALIGNMENT (YOUR GATE FIX)
        // // -------------------------
        // bool valid = false;

        // for(int i = 0; i < 500; i++){
        //     {
        //         lock_guard<mutex> lock(buffer_mtx);

        //         if(!depth_buffer.empty() && depth_buffer.back().u > snapshot_id){
        //             valid = true;
        //             break;
        //         }
        //     }
        //     this_thread::sleep_for(milliseconds(10));
        // }

        // if(!valid) throw runtime_error("Stream not aligned (no post-snapshot events)");
        
        // vector<Depth> buffered;
        // {
        //     lock_guard lock(buffer_mtx);
        //     buffered = depth_buffer;   // copy, don't swap
        // }
        
        // sort(buffered.begin(), buffered.end(), [](const auto& a, const auto& b) {return a.U < b.U;});

        // auto it = find_if(buffered.begin(), buffered.end(), [&](const Depth& d){
        //     return d.U <= snapshot_id + 1 && snapshot_id + 1 <= d.u;});

        // if(it == buffered.end()) throw runtime_error("Couldn't synchronize order book");
        
        // // -------------------------
        // // APPLY REPLAY
        // // -------------------------
        // for(; it != buffered.end(); ++it){
        //     cout << "BUFFER U: " << it->U << " snapshot_id + 1: " << snapshot_id + 1 << " u: " << it->u << "\n";

        //     state.market_book.apply_delta(*it);
        //     state.market_book.last_update_id = it->u;
        //     state.update_vol();
        // }

        // cout << "BOOK SYNCHRONIZED\n";
        
        // // -------------------------
        // // LIVE MODE
        // // -------------------------
        // {
        //     lock_guard<mutex> lock(buffer_mtx);
        //     buffering = false;
        // }
        // state.initialized = true;

        // cout << "LIVE BOOK RUNNING\n";
    }

    void stop() override {
        cout << "STOPPING BINANCE FEED\n";

        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*market_ws).cancel(ec);
        // beast::get_lowest_layer(*depth_ws).cancel(ec);

        if(market_thread.joinable()) market_thread.join();
        // if(depth_thread.joinable()) depth_thread.join();

        cout << "BINANCE FEED STOPPED\n";
    }
};

class BinanceSpotFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    ExecutionEventQueue& execution_event;
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unique_ptr<ws_stream> trade_ws;
    unique_ptr<ws_stream> depth_ws;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    vector<Depth> depth_buffer;
    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;

    thread depth_thread;
    thread trade_thread;

    BinanceSpotFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event, 
                    DatasetRecorder& recorder, BinanceClock& clock):
        config(config), state(state), recorder(recorder), execution_event(execution_event), clock(clock) {}

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
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        trade_ws = make_unique<ws_stream>(move(ssl_sock));
        trade_ws->handshake(config.hostname, "/ws/" + config.instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            trade_ws->read(buffer, ec);
            if(ec) break;

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = int64_t(doc["T"]);
            trade.local_ts = clock.now_ms();
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.local_ts, trade.ts);

            recorder.log_event("trade", trade.ts, trade.local_ts, trade.latency, msg);

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
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        depth_ws = make_unique<ws_stream>(move(ssl_sock));
        depth_ws->handshake(config.hostname, "/ws/" + config.instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            depth_ws->read(buffer, ec);
            if(ec) break;

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = int64_t(doc["E"]);
            depth.local_ts = clock.now_ms();
            depth.U = int64_t(doc["U"]);
            depth.u = int64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.local_ts, depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            recorder.log_event("depth", depth.ts, depth.local_ts, depth.latency, msg);

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

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance();

        recorder.export_orderbook_snapshot(snapshot);

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

        boost::system::error_code ec;
        beast::get_lowest_layer(*trade_ws).cancel(ec);
        beast::get_lowest_layer(*depth_ws).cancel(ec);

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
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unique_ptr<ws_stream> trade_ws;
    unique_ptr<ws_stream> depth_ws;

    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};
    
    vector<Depth> depth_buffer;
    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;

    thread depth_thread;
    thread trade_thread;

    BinanceFuturesFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event,
                       DatasetRecorder& recorder, BinanceClock& clock):
        config(config), state(state), execution_event(execution_event), recorder(recorder), clock(clock) {}

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
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        trade_ws = make_unique<ws_stream>(move(ssl_sock));
        trade_ws->handshake(config.hostname, "/ws/" + config.instrument + "@trade");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            trade_ws->read(buffer, ec);
            if(ec) break;

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Trade trade;
            trade.ts = int64_t(doc["T"]);
            trade.local_ts = clock.now_ms();
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.latency = clock.compute_feed_latency(trade.local_ts, trade.ts);

            recorder.log_event("trade", trade.ts, clock.now_ms(), trade.latency, msg);

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
        auto results = resolver.resolve(config.hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        depth_ws = make_unique<ws_stream>(move(ssl_sock));
        depth_ws->handshake(config.hostname, "/ws/" + config.instrument + "@depth@100ms");

        beast::flat_buffer buffer;
        simdjson::ondemand::parser parser;

        while(running){
            boost::system::error_code ec;
            depth_ws->read(buffer, ec);
            if(ec) break;

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            simdjson::padded_string json(msg);
            auto doc = parser.iterate(json);

            Depth depth;
            depth.ts = int64_t(doc["E"]);
            depth.local_ts = clock.now_ms();
            depth.pu = int64_t(doc["pu"]);
            depth.U = int64_t(doc["U"]);
            depth.u = int64_t(doc["u"]);
            depth.latency = clock.compute_feed_latency(depth.local_ts, depth.ts);
            parse_book(doc.get_object(), depth);

            first_depth_received = true;
            cv.notify_all();

            recorder.log_event("depth", depth.ts, clock.now_ms(), depth.latency, msg);

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

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_binance();

        recorder.export_orderbook_snapshot(snapshot);

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

        boost::system::error_code ec;
        beast::get_lowest_layer(*trade_ws).cancel(ec);
        beast::get_lowest_layer(*depth_ws).cancel(ec);

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
    DatasetRecorder& recorder;
    BinanceClock& clock;

    vector<EventRow> events;
    json orderbook_snapshot;

    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    mutex cv_mtx; //condition variable lock
    condition_variable cv;

    thread replay_thread;

    size_t i = 0;
    optional<int64_t> last_ts;

    simdjson::ondemand::parser parser;
    
    BinanceSpotReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event,
                          DatasetRecorder& recorder, BinanceClock& clock):
        config(config), state(state), execution_event(execution_event), recorder(recorder), clock(clock) {initialize();}

    void initialize(){
        std_string orderbook_snapshot_path = config.folder_path + "/orderbook_snapshot.json";
        std_string events_path = config.folder_path + "/events.parquet";

        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        cout << "events_path: " << events_path << "\n";
        if(!fs::exists(events_path)){
            cout << "File not found: " + events_path + ", run feature engineering\n";
        }

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<LargeStringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto local_ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("local_ts")->chunk(0)
        );

        auto latency_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("latency")->chunk(0)
        );

        auto msg_col = static_pointer_cast<LargeStringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventRow e;
            e.type = type_col->GetString(i);
            e.ts = ts_col->Value(i);
            e.local_ts = local_ts_col->Value(i);
            e.latency = latency_col->Value(i);
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

    void on_trade_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = int64_t(doc["T"]);
        trade.local_ts = e.local_ts;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = e.latency;
        
        recorder.log_event("trade", trade.ts, trade.local_ts, trade.latency, e.msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = int64_t(doc["E"]);
        depth.local_ts = e.local_ts;
        depth.U  = int64_t(doc["U"]);
        depth.u  = int64_t(doc["u"]);
        depth.latency = e.latency;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        recorder.log_event("depth", depth.ts, depth.local_ts, depth.latency, e.msg);

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
            EventRow& e = events[i];

            if(last_ts.has_value()){
                double dt = (e.local_ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / config.speed_multiplier)));
            }

            last_ts = e.local_ts;

            if(e.type == "trade") on_trade_message(e);
            else if(e.type == "depth") on_depth_message(e);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY SPOT SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        recorder.export_orderbook_snapshot(snapshot);

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
    DatasetRecorder& recorder;
    BinanceClock& clock;

    vector<EventRow> events;
    json orderbook_snapshot;

    atomic<bool> running{false};
    atomic<bool> snapshot_aligned{false};
    atomic<bool> first_depth_received{false};

    mutex cv_mtx; //condition variable lock
    condition_variable cv;

    thread replay_thread;

    size_t i = 0; //provides wrap around loop
    optional<int64_t> last_ts;

    simdjson::ondemand::parser parser;
    
    BinanceFuturesReplayFeed(MarketConfig& config, State& state, ExecutionEventQueue& execution_event,
                             DatasetRecorder& recorder, BinanceClock& clock):
        config(config), state(state), execution_event(execution_event), recorder(recorder), clock(clock) {initialize();}

    void initialize(){
        std_string orderbook_snapshot_path = config.folder_path + "/orderbook_snapshot.json";
        std_string events_path = config.folder_path + "/events.parquet";

        cout << "snapshot path: " << orderbook_snapshot_path << "\n";
        ifstream f(orderbook_snapshot_path);
        f >> orderbook_snapshot;

        cout << "events_path: " << events_path << "\n";
        if(!fs::exists(events_path)){
            cout << "File not found: " + events_path + ", run feature engineering\n";
        }

        parquet::arrow::FileReaderBuilder builder;
        PARQUET_THROW_NOT_OK(builder.OpenFile(events_path, false));

        unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(builder.Build(&reader));

        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        auto type_col = static_pointer_cast<LargeStringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto local_ts_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("local_ts")->chunk(0)
        );

        auto latency_col = static_pointer_cast<Int64Array>(
            table->GetColumnByName("latency")->chunk(0)
        );

        auto msg_col = static_pointer_cast<LargeStringArray>(
            table->GetColumnByName("msg")->chunk(0)
        );

        size_t n = table->num_rows();

        events.clear();
        events.reserve(n);

        for(size_t i = 0; i < n; i++){
            EventRow e;
            e.type = type_col->GetString(i);
            e.ts = ts_col->Value(i);
            e.local_ts = local_ts_col->Value(i);
            e.latency = latency_col->Value(i);
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

    void on_trade_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.ts    = int64_t(doc["T"]);
        trade.local_ts    = e.local_ts;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.latency = e.latency;

        recorder.log_event("trade", trade.ts, trade.local_ts, trade.latency, e.msg);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::TRADE_UPDATE;
        ev.trade = trade;
        execution_event.push(ev);
    }

    void on_depth_message(const EventRow& e){

        simdjson::padded_string json(e.msg);
        auto doc = parser.iterate(json);

        Depth depth;
        depth.ts = int64_t(doc["E"]);
        depth.local_ts = e.local_ts;
        depth.pu = int64_t(doc["pu"]);
        depth.U  = int64_t(doc["U"]);
        depth.u  = int64_t(doc["u"]);
        depth.latency = e.latency;
        parse_book(doc.get_object(), depth);

        first_depth_received = true;
        cv.notify_all();
  
        recorder.log_event("depth", depth.ts, depth.local_ts, depth.latency, e.msg);
        
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
            EventRow& e = events[i];

            if(last_ts.has_value()){
                double dt = (e.local_ts - *last_ts) / 1000.0;
                this_thread::sleep_for(chrono::duration<double>(max(0.0, dt / config.speed_multiplier)));
            }

            last_ts = e.local_ts;

            if(e.type == "depth") on_depth_message(e);
            else if(e.type == "trade") on_trade_message(e);

            i++;
        }
    }

    void start() override {
        running = true;

        snapshot_aligned = false;
        first_depth_received = false;

        cout << "REPLAY FUTURES SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        recorder.export_orderbook_snapshot(snapshot);

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