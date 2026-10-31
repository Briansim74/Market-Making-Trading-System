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

class BinanceMarkPriceStream {
public:
    MarketConfig& config;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;
    std_string market;

    atomic<bool> running{false};
    atomic<bool> connected{false};

    thread stream_thread;
    simdjson::ondemand::parser parser;

    unique_ptr<ws_stream> ws;

    mutex connection_mtx;
    condition_variable connection_cv;

    BinanceMarkPriceStream(MarketConfig& config, ExecutionEventQueue& execution_event, BinanceClock& clock)
        : config(config), execution_event(execution_event), clock(clock), market((config.mode == "live") ? "perps" : "testnet_perps") {}

    void start(){
        running = true;
        stream_thread = thread([this](){run();});
        wait_until_connected();
    }

    void wait_until_connected(){
        unique_lock<mutex> lock(connection_mtx);
        connection_cv.wait(lock, [this]{return connected.load();});
    }

    void run(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.markets[market].hostname, "443");

        // TCP
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // TLS
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.markets[market].hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // Websocket
        ws = make_unique<ws_stream>(move(ssl_sock));
        ws->handshake(config.markets[market].hostname, "/market/ws/" + config.markets[market].instrument + "@markPrice");

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        beast::flat_buffer buffer;
        cout << "MARKPRICE STREAM STARTED\n";

        while(running){
            boost::system::error_code ec;
            ws->read(buffer, ec);
            if(ec) break;

            string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            on_message(msg);
        }
    }

    void stop(){
        cout << "STOPPING MARKPRICE STREAM\n";
        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*ws).cancel(ec);

        if(stream_thread.joinable()) stream_thread.join();

        cout << "MARKPRICE STREAM STOPPED\n";
    }

    void on_message(const string& msg){
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        if(std_string(doc["e"]) != "markPriceUpdate") return;
        
        Stream stream;
        stream.price = double(doc["p"].get_double_in_string());
        stream.exchange_ts = int64_t(doc["E"]);
        stream.funding_rate = double(doc["r"].get_double_in_string());
        stream.next_funding_ts = int64_t(doc["T"]);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::MARKPRICE_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};