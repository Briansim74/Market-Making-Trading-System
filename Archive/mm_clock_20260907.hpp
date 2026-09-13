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

class BinanceClock {
public:
    MarketConfig& config;

    atomic<int64_t> offset_ms{0};
    atomic<int64_t> rtt_ms{0};

    atomic<bool> running{false};
    atomic<bool> connected{false};

    mutex connection_mtx;
    mutex clock_mtx;
    condition_variable connection_cv;
    condition_variable clock_cv;

    thread clock_thread;

    BinanceClock(MarketConfig& config): config(config) {}

    int64_t now_ms() const {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    int64_t compute_feed_latency(const int64_t& local_ts, const int64_t& feed_ts) const {
        return (local_ts - offset_ms.load()) - feed_ts;
    }

    int64_t compute_exchange_latency(const int64_t& stream_ts, const int64_t& local_ts) const {
        return stream_ts - (local_ts - offset_ms.load());
    }

    void start(){
        running = true;
        clock_thread = thread(&BinanceClock::clock_sync_loop, this);
        wait_until_connected();
    }

    void wait_until_connected(){
        unique_lock<mutex> lock(connection_mtx);
        connection_cv.wait(lock, [this]{return connected.load();});
    }

    void clock_sync_loop(){
        while(running){
            try{
                sync_once();
            }
            catch(const exception& e){
                cout << "[CLOCK] - CLOCK ERROR: " << e.what() << "\n";
            }

            unique_lock<mutex> lock(clock_mtx);
            clock_cv.wait_for(lock, seconds(5), [this]{return !running.load();});
        }
    }

    void sync_once(){
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.base_url, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        simdjson::ondemand::parser parser;

        int64_t best_offset = 0;
        int64_t best_rtt = INT64_MAX;

        for(int i = 0; i < 5; i++){
            http::request<http::empty_body> req{http::verb::get, "/" + config.endpoint + "/time", 11};

            req.keep_alive(true);
            req.set(http::field::host, config.base_url);
            req.set(http::field::user_agent, config.struct_model);

            // Wall clock (for Binance comparison)
            int64_t t0_wall = now_ms();

            // Monotonic clock (for RTT)
            auto t0 = steady_clock::now();

            http::write(ssl_sock, req);
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(ssl_sock, buffer, res);

            auto t1 = steady_clock::now();
            int64_t t1_wall = now_ms();

            simdjson::padded_string json(res.body());
            auto doc = parser.iterate(json);

            int64_t server_ms = int64_t(doc["serverTime"]);
            int64_t rtt = duration_cast<milliseconds>(t1 - t0).count();

            // Midpoint of the wall clock
            int64_t midpoint_wall = (t0_wall + t1_wall) / 2;
            int64_t offset = midpoint_wall - server_ms;

            if(rtt < best_rtt){
                best_rtt = rtt;
                best_offset = offset;
            }

            this_thread::sleep_for(milliseconds(100));
        }

        offset_ms.store(best_offset);
        rtt_ms.store(best_rtt);

        cout << "[CLOCK] - best_offset: " << best_offset << " ms" << ", best_rtt: " << best_rtt << " ms\n";

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        boost::system::error_code ec;
        ssl_sock.shutdown(ec);
    }

    void stop(){
        running = false;
        clock_cv.notify_one();

        if(clock_thread.joinable()) clock_thread.join();
    }
};