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

class HttpClient {
public:
    MarketConfig& config;
    std_string market;

    asio::io_context ioc;
    ssl::context ctx;
    ssl_stream ssl_sock;

    mutex http_mtx; // execution and broker keepalive mtx

    HttpClient(MarketConfig& config, std_string market) : config(config), market(market), ctx(ssl::context::tlsv12_client), ssl_sock(ioc, ctx) {}

    void initialize(){
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.markets[market].base_url, "443");

        asio::connect(ssl_sock.next_layer(), results);

        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.markets[market].base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        cout << "[HTTP] - broker connected: " << config.markets[market].base_url << "\n";
    }

    void reconnect(){
        cout << "[HTTP] - broker reconnecting\n";

        beast::error_code ec;
        ssl_sock.shutdown(ec);

        ssl_sock.next_layer().shutdown(tcp::socket::shutdown_both, ec);
        ssl_sock.next_layer().close(ec);

        // Recreate the socket
        ssl_sock = ssl_stream(ioc, ctx);

        initialize();
    }

    string request(http::verb method, const string& target,
                const vector<string>& headers = {}, const string& body = ""){

        for(int attempt = 1; attempt <= 3; attempt++){

            beast::error_code ec;
            http::response<http::string_body> res;

            // cout << "[HTTP] - ATTEMPT " << attempt << "\n";

            {
                lock_guard<mutex> lock(http_mtx);
                http::request<http::string_body> req{method, target, 11};

                req.set(http::field::host, config.markets[market].base_url);
                req.set(http::field::user_agent, config.struct_model);

                for(const auto& h: headers){
                    auto pos = h.find(":");

                    if(pos != string::npos){
                        std_string key = h.substr(0, pos);
                        std_string value = h.substr(pos + 1);

                        while(!value.empty() && value[0] == ' ')
                            value.erase(value.begin());

                        req.set(key,value);
                    }
                }

                if(!body.empty()){
                    req.body() = body;
                    req.prepare_payload();
                }

                beast::flat_buffer buffer;

                http::write(ssl_sock, req, ec);
                http::read(ssl_sock, buffer, res, ec);

                if(ec){
                    reconnect();
                    continue;
                }
            }

            if(res.result_int() >= 400){
                cout << "[HTTP] - ERROR: status >= 400: HTTP " + to_string(res.result_int()) + ": " + res.body();
            }
            
            cout << "\n===== HTTP RESPONSE =====\n";
            if(method != http::verb::get) cout << res << endl;
            cout << "\n===== HTTP RESPONSE END =====\n";
            return res.body();
        }
        return "http error";
    }
};

class BinanceBroker {
public:
    virtual std_string open_user_stream() = 0;
    virtual void stop() = 0;
    virtual std_string sign(const std_string&) = 0;
    virtual double get_position() = 0;
    virtual json place_limit(const Order&, const double&, const double&) = 0;
    virtual json place_market(const Order&) = 0;
    virtual json cancel_order(const Order&) = 0;
    virtual ~BinanceBroker() = default;
};

class BinanceSpotBroker : public BinanceBroker {
public:
    MarketConfig& config;
    BinanceClock& clock;
    HttpClient http;

    BinanceSpotBroker(MarketConfig& config, BinanceClock& clock)
        : config(config), clock(clock), http(config, "spot")
        {
            http.initialize();
            get_fee_rates();
        }

    std_string open_user_stream(){
        return "";
    }

    void stop(){}

    // -------------------------
    // SIGNING
    // -------------------------
    std_string sign(const std_string& query){
        unsigned char* digest;
        digest = HMAC(EVP_sha256(), config.api_secret.c_str(), config.api_secret.size(),
                      (unsigned char*)query.c_str(), query.size(), NULL, NULL);

        char mdString[65];
        for(int i = 0; i < 32; i++)
            sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);

        return std_string(mdString);
    }

    // -------------------------
    // ACCOUNT BALANCE
    // -------------------------
    void get_fee_rates(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "symbol=" << config.markets["spot"].instrument_upper
        << "&timestamp=" << ts
        << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "https://" + config.markets["spot"].base_url + "/" + config.markets["spot"].endpoint + "/account/commission?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        config.markets["spot"].maker_fee_rate = stod(arr["standardCommission"]["maker"].get<std_string>());
        config.markets["spot"].taker_fee_rate = stod(arr["standardCommission"]["taker"].get<std_string>());

        cout << "[BROKER] - maker_fee_rate: " << config.markets["spot"].maker_fee_rate << " (" << config.markets["spot"].maker_fee_rate * 100 << "%)\n";
        cout << "[BROKER] - taker_fee_rate: " << config.markets["spot"].taker_fee_rate << " (" << config.markets["spot"].taker_fee_rate * 100 << "%)\n";
    }

    double get_position(){

        std_string asset = config.markets["spot"].instrument_upper.substr(0, config.markets["spot"].instrument_upper.size() - 4);

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets["spot"].endpoint + "/account?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        for(auto& balance: arr["balances"]){
            if(balance["asset"] == asset) return stod(balance["free"].get<std_string>());
        }

        return 0.0;
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    json place_limit(const Order& order, const double& price, const double& size){

        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets["spot"].instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.markets["spot"].qty_precision) << size
          << "&price=" << fixed << setprecision(config.markets["spot"].price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets["spot"].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "&newClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets["spot"].instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.markets["spot"].qty_precision) << order.qty
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets["spot"].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json cancel_order(const Order& order){

        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets["spot"].instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets["spot"].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::delete_, url, headers);
        return json::parse(res);
    }
};

class BinanceUserStream {
public:
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual ~BinanceUserStream() = default;
};

class BinanceSpotUserStream : public BinanceUserStream {
public:
    MarketConfig& config;
    BinanceBroker& broker;
    ExecutionEventQueue& execution_event;
    BinanceClock& clock;
    
    atomic<bool> running{false};
    atomic<bool> connected{false};
    
    thread stream_thread;
    simdjson::ondemand::parser parser;

    unique_ptr<ws_stream> ws;

    mutex connection_mtx;
    condition_variable connection_cv;

    BinanceSpotUserStream(MarketConfig& config, BinanceBroker& broker, ExecutionEventQueue& execution_event, BinanceClock& clock)
        : config(config), broker(broker), execution_event(execution_event), clock(clock) {}

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
        const string hostname = "ws-" + config.markets["spot"].base_url;
        const string target = "/ws-" + config.markets["spot"].endpoint;

        cout << "[USER STREAM] - resolving " << hostname << "\n";

        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(hostname, "443");

        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws = make_unique<ws_stream>(move(ssl_sock));
        ws->handshake(hostname, target);

        // -------------------------------------------------
        // SUBSCRIBE
        // -------------------------------------------------
        subscribe_user_data_stream();

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        // -------------------------------------------------
        // READ LOOP
        // -------------------------------------------------
        beast::flat_buffer buffer;

        while(running){
            boost::system::error_code ec;
            ws->read(buffer, ec);
            if(ec){
                if(running) cerr << "[USER STREAM] - read error: " << ec.message() << "\n";
                break;
            }

            string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            on_message(msg);
        }
    }

    // ---------------------------------------------------------
    // USER DATA STREAM SUBSCRIPTION
    // ---------------------------------------------------------
    void subscribe_user_data_stream(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "apiKey=" << config.api_key
          << "&recvWindow=5000"
          << "&timestamp=" << ts;

        string query = q.str();
        string signature = broker.sign(query);

        json request = {
            {"id", config.struct_model},
            {"method", "userDataStream.subscribe.signature"},
            {"params", {
                {"apiKey", config.api_key},
                {"timestamp", ts},
                {"recvWindow", 5000},
                {"signature", signature}
            }}
        };

        string msg = request.dump();
        ws->write(asio::buffer(msg));

        // -----------------------------------------------------
        // Wait for subscription response
        // -----------------------------------------------------
        beast::flat_buffer buffer;

        while(running){
            boost::system::error_code ec;
            ws->read(buffer, ec);
            if(ec) throw runtime_error("subscription read failed: " + ec.message());

            string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            cout << "[USER STREAM] - subscription response: " << msg << "\n";

            auto resp = json::parse(msg);

            if(!resp.contains("status")) continue;

            int status = resp["status"].get<int>();

            if(status != 200){
                throw runtime_error("userDataStream subscription failed: " + msg);
            }

            if(resp.contains("result") && resp["result"].contains("subscriptionId")){
                cout << "[USER STREAM] - subscribed, subscriptionId=" << resp["result"]["subscriptionId"] << "\n";
                return;
            }
        }

        throw runtime_error("userDataStream subscription aborted");
    }

    void stop(){
        cout << "STOPPING SPOT USER STREAM\n";
        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*ws).cancel(ec);

        if(stream_thread.joinable()) stream_thread.join();

        cout << "SPOT USER STREAM STOPPED\n";
    }

    void on_message(const std_string& msg){
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        simdjson::ondemand::object o = doc["event"].get_object();

        if(std_string(o["e"]) != "executionReport") return;
        cout << msg << "\n";

        Stream stream;
        stream.client_oid = (!std_string(o["C"]).empty()) ? std_string(o["C"]) : std_string(o["c"]);
        stream.side = std_string(o["S"]);
        stream.status = std_string(o["X"]);
        stream.exec_type = std_string(o["x"]);
        stream.order_type = std_string(o["o"]);
        stream.price = double(o["p"].get_double_in_string());
        stream.qty = double(o["q"].get_double_in_string());
        stream.fill_price = double(o["L"].get_double_in_string());
        stream.fill_qty = double(o["l"].get_double_in_string());
        stream.fees_paid = double(o["n"].get_double_in_string());
        stream.exchange_ts = int64_t(o["T"]);
        stream.local_ts = clock.now_ms();
        stream.is_maker = o["m"].get<bool>();

        ExecutionEvent ev;
        ev.type = ExecutionEventType::SPOT_STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};

class BinancePerpsBroker : public BinanceBroker {
public:
    MarketConfig& config;
    BinanceClock& clock;
    std_string market;
    HttpClient http;

    std_string listen_key;
    atomic<bool> keepalive_running{false};

    mutex keepalive_mtx;
    condition_variable keepalive_cv;

    thread keepalive_thread;

    BinancePerpsBroker(MarketConfig& config, BinanceClock& clock)
        : config(config), clock(clock), market((config.mode == "live") ? "perps" : "testnet_perps"), http(config, market)
        {
            http.initialize();
            get_fee_rates();
        }

    // -------------------------
    // USER STREAM
    // -------------------------
    std_string open_user_stream(){

        std_string url = "/" + config.markets[market].endpoint + "/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        auto j = json::parse(res);

        listen_key = j["listenKey"];
        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){

        std_string url = "/" + config.markets[market].endpoint + "/listenKey?listenKey=" + listen_key;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        http.request(http::verb::put, url, headers);
    }

    void start_keepalive_loop(){
        keepalive_running = true;

        keepalive_thread = thread([this](){
            unique_lock<mutex> lock(keepalive_mtx);

            while(true){
                if(keepalive_cv.wait_for(lock, minutes(20),
                    [this]{return !keepalive_running;})) break;

                try{
                    keepalive_listen_key();
                    cout << "[keepalive sent]\n";
                }
                catch(...){
                    cout << "[keepalive error]\n";
                }
            }
        });
    }

    void stop(){
        keepalive_running = false;
        keepalive_cv.notify_one();

        if(keepalive_thread.joinable()) keepalive_thread.join();
    }

    // -------------------------
    // SIGNING
    // -------------------------
    std_string sign(const std_string& query){
        unsigned char* digest;
        digest = HMAC(EVP_sha256(), config.api_secret.c_str(), config.api_secret.size(),
                      (unsigned char*)query.c_str(), query.size(), NULL, NULL);

        char mdString[65];
        for(int i = 0; i < 32; i++)
            sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);

        return std_string(mdString);
    }

    // -------------------------
    // ACCOUNT BALANCE
    // -------------------------
    void get_fee_rates(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "symbol=" << config.markets[market].instrument_upper
        << "&timestamp=" << ts
        << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "https://" + config.markets[market].base_url + "/" + config.markets[market].endpoint + "/commissionRate?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        config.markets[market].maker_fee_rate = stod(arr["makerCommissionRate"].get<std_string>());
        config.markets[market].taker_fee_rate = stod(arr["takerCommissionRate"].get<std_string>());

        cout << market << "_maker_fee_rate: " << config.markets[market].maker_fee_rate << " (" << config.markets[market].maker_fee_rate * 100 << "%)\n";
        cout << market << "_taker_fee_rate: " << config.markets[market].taker_fee_rate << " (" << config.markets[market].taker_fee_rate * 100 << "%)\n";
    }

    double get_position(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/fapi/v2/positionRisk?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        for(auto& p: arr){
            if(p["symbol"] == config.markets[market].instrument_upper) return stod(p["positionAmt"].get<std_string>());
        }
        return 0.0;
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    json place_limit(const Order& order, const double& price, const double& size){
        
        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets[market].instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.markets[market].qty_precision) << size
          << "&price=" << fixed << setprecision(config.markets[market].price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets[market].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets[market].instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.markets[market].qty_precision) << order.qty
          << "&reduceOnly=true"
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets[market].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};
        
        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json cancel_order(const Order& order){
        
        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << config.markets[market].instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.markets[market].endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::delete_, url, headers);
        return json::parse(res);
    }
};

class BinancePerpsUserStream : public BinanceUserStream {
public:
    MarketConfig& config;
    BinanceBroker& broker;
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

    BinancePerpsUserStream(MarketConfig& config, BinanceBroker& broker, ExecutionEventQueue& execution_event, BinanceClock& clock)
        : config(config), broker(broker), execution_event(execution_event), clock(clock), market((config.mode == "live") ? "perps" : "testnet_perps") {}

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
        std_string listen_key = broker.open_user_stream();

        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();

        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.markets[market].hostname, "443");
        
        // -------------------------
        // STEP 1: TCP SOCKET
        // -------------------------
        tcp::socket socket(ioc);
        asio::connect(socket, results);

        // -------------------------
        // STEP 2: TLS LAYER
        // -------------------------
        ssl_stream ssl_sock(move(socket), ctx);
        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.markets[market].hostname.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        // -------------------------
        // STEP 3: WEBSOCKET LAYER
        // -------------------------
        ws = make_unique<ws_stream>(move(ssl_sock));
        ws->handshake(config.markets[market].hostname, "/private/ws?listenKey=" + listen_key);
        // ws->handshake(config.markets[market].hostname, "/private/ws?listenKey=" + listen_key + "&events=ORDER_TRADE_UPDATE"); // filters to only receive order trade updates

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        beast::flat_buffer buffer;
        cout << "PERPS STREAM STARTED\n";
        while(running){
            boost::system::error_code ec;
            ws->read(buffer, ec);
            if(ec) break;

            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            on_message(msg);
        }
    }

    void stop(){
        cout << "STOPPING PERPS USER STREAM\n";
        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*ws).cancel(ec);

        if(stream_thread.joinable()) stream_thread.join();

        cout << "PERPS USER STREAM STOPPED\n";
    }

    void on_message(const std_string& msg){

        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);
        cout << "PERPS STREAM: " << msg << "\n";
        if(std_string(doc["e"]) != "ORDER_TRADE_UPDATE") return;
        cout << msg << "\n";

        simdjson::ondemand::object o = doc["o"].get_object();

        Stream stream;
        stream.client_oid = std_string(o["c"]);
        stream.side = std_string(o["S"]);
        stream.status = std_string(o["X"]);
        stream.exec_type = std_string(o["x"]);
        stream.order_type = std_string(o["o"]);
        stream.price = double(o["p"].get_double_in_string());
        stream.qty = double(o["q"].get_double_in_string());
        stream.fill_price = double(o["L"].get_double_in_string());
        stream.fill_qty = double(o["l"].get_double_in_string());
        stream.fees_paid = double(o["n"].get_double_in_string());
        stream.exchange_ts = int64_t(o["T"]);
        stream.local_ts = clock.now_ms();
        stream.is_maker = o["m"].get<bool>();

        ExecutionEvent ev;
        ev.type = ExecutionEventType::PERPS_STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};