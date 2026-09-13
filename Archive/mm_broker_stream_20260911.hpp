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

    asio::io_context ioc;
    ssl::context ctx;
    ssl_stream ssl_sock;

    atomic<bool> running{false};
    condition_variable http_cv;

    mutex http_mtx; // execution and broker keepalive mtx
    thread http_thread;

    HttpClient(MarketConfig& config) : config(config), ctx(ssl::context::tlsv12_client), ssl_sock(ioc, ctx) {}

    void start(){
        running = true;
        // http_thread = thread(&HttpClient::reconnect_loop, this);
    }

    // void reconnect_loop(){
    //     while(running){
    //         unique_lock<mutex> lock(http_mtx);
    //         http_cv.wait_for(lock, minutes(2), [this]{return !running.load();});
            
    //         try{
    //             reconnect();
    //         }
    //         catch(const exception& e){
    //             cout << "[HTTP] - HTTP CLIENT ERROR: " << e.what() << "\n";
    //         }
    //     }
    // }

    void reconnect(){
        cout << "[HTTP] - broker reconnecting...\n";

        beast::error_code ec;
        ssl_sock.shutdown(ec);

        ssl_sock.next_layer().shutdown(tcp::socket::shutdown_both, ec);
        ssl_sock.next_layer().close(ec);

        // Recreate the socket
        ssl_sock = ssl_stream(ioc, ctx);

        initialize();
    }

    void initialize(){
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.base_url, "443");

        asio::connect(ssl_sock.next_layer(), results);

        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        cout << "[HTTP] - broker connected: " << config.base_url << "\n";
    }

    string request(http::verb method, const string& target,
                const vector<string>& headers = {}, const string& body = ""){

        lock_guard<mutex> lock(http_mtx);
        http::request<http::string_body> req{method, target, 11};

        req.set(http::field::host, config.base_url);
        req.set(http::field::user_agent, config.struct_model);


        // req.set(http::field::connection, "keep-alive"); //DEBUG

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

        // check if alive - to be removed
        beast::error_code ec;

        http::write(ssl_sock, req, ec);

        cout << "[HTTP] WRITE END: " << ec.message() << '\n';
     
        // http::response<http::string_body> res;
        // http::read(ssl_sock, buffer, res);

        // http::read(ssl_sock, buffer, res, ec);

        // cout << "[HTTP] READ END: "
        //     << ec.value() << " - "
        //     << ec.message() << '\n';

        // cout << "[HTTP] status: "
        //     << res.result_int() << '\n';

        // cout << "[HTTP] keep_alive: "
        //     << res.keep_alive() << '\n';

        // cout << "[HTTP] body size: "
        //     << res.body().size() << '\n';

        // cout << "[HTTP] body: "
        //     << res.body() << '\n';

        // cout << "[HTTP] RESPONSE HEADERS:\n";

        // for(auto const& field : res){
        //     cout << "[HTTP] "
        //         << field.name_string()
        //         << ": "
        //         << field.value()
        //         << '\n';
        // }


        http::response_parser<http::string_body> parser;

        http::read(ssl_sock, buffer, parser, ec);

        cout << "[HTTP] READ END: "
            << ec.value()
            << " - "
            << ec.message()
            << "\n";

        cout << "[HTTP] parser.is_done(): "
            << parser.is_done()
            << "\n";

        if(ec){
            cout << "[HTTP] READ ERROR reconnnecting: "
                << ec.value() << " - "
                << ec.message() << "\n";

            reconnect();

            return "test";
        }

        auto& res = parser.get();

        cout << "[HTTP] status: "
            << res.result_int()
            << "\n";

        cout << "[HTTP] body size: "
            << res.body().size()
            << "\n";

        cout << "[HTTP] body: ["
            << res.body()
            << "]\n";

        cout << "[HTTP] TEST KEEP ALIVE: "
            << res.keep_alive() << "\n";

        cout << "[HTTP] RESPONSE HEADERS:\n";

        for(auto const& field : res){
            cout << "[HTTP] "
                << field.name_string()
                << ": "
                << field.value()
                << "\n";
        }

        if(res.result_int() >= 400){
            cout << "[HTTP] ERROR: HTTP "
                << res.result_int()
                << ": "
                << res.body()
                << '\n';
        }

        // if(!parser.is_done()){
        //     cout << "[HTTP] PARSER NOT DONE... RECONNECTING...\n";
        //     reconnect();
        //     cout << "test\n";
        //     return R"({"test":"parser_not_done"})";;
        // }

        cout << "http3\n";
        if(res.result_int() >= 400){
            cout << "[HTTP] - ERROR: status >= 400: HTTP " + to_string(res.result_int()) + ": " + res.body();
        }
        
        cout << "\n===== HTTP RESPONSE =====\n";
        if(method != http::verb::get) cout << res << endl;

        return res.body();
    }

    void stop(){
        running = false;
        // http_cv.notify_one();

        // if(http_thread.joinable()) http_thread.join();
        cout << "HTTP CLIENT STOPPED\n";
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
        : config(config), clock(clock), http(config)
        {
            http.initialize();
            http.start();
            get_fee_rates();
        }

    std_string open_user_stream(){
        return "";
    }

    void stop(){
        http.stop();
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
        q << "symbol=" << config.instrument_upper
        << "&timestamp=" << ts
        << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "https://" + config.base_url + "/" + config.endpoint + "/account/commission?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        config.maker_fee_rate = stod(arr["standardCommission"]["maker"].get<std_string>());
        config.taker_fee_rate = stod(arr["standardCommission"]["taker"].get<std_string>());

        cout << "[BROKER] - maker_fee_rate: " << config.maker_fee_rate << " (" << config.maker_fee_rate * 100 << "%)\n";
        cout << "[BROKER] - taker_fee_rate: " << config.taker_fee_rate << " (" << config.taker_fee_rate * 100 << "%)\n";
    }

    double get_position(){

        std_string asset = config.instrument_upper.substr(0, config.instrument_upper.size() - 4);

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/account?" + query + "&signature=" + signature;
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
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << size
          << "&price=" << fixed << setprecision(config.price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};
        cout << "[BROKER] - 1\n";
        auto res = http.request(http::verb::post, url, headers);
        cout << "[BROKER] - 2\n";
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "&newClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << order.qty
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json cancel_order(const Order& order){

        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);
        cout << "[BROKER] - cancel query: " << query << "\n";
        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};
        cout << "[BROKER] - 1a\n";
        auto res = http.request(http::verb::delete_, url, headers);
        cout << "[BROKER] - 1.1a\n";
        if(res == "test"){
            cout << "replacing cancel\n";
            auto res = http.request(http::verb::delete_, url, headers);
        }

        cout << "[BROKER] - 2a\n";
        return json::parse(res);
    }
};

class BinanceFuturesBroker : public BinanceBroker {
public:
    MarketConfig& config;
    BinanceClock& clock;
    HttpClient http;

    std_string listen_key;
    atomic<bool> keepalive_running{false};

    mutex keepalive_mtx;
    condition_variable keepalive_cv;

    thread keepalive_thread;

    BinanceFuturesBroker(MarketConfig& config, BinanceClock& clock)
        : config(config), clock(clock), http(config)
        {
            http.initialize();
            get_fee_rates();
        }

    // -------------------------
    // USER STREAM
    // -------------------------
    std_string open_user_stream(){

        std_string url = "/" + config.endpoint + "/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        auto j = json::parse(res);

        listen_key = j["listenKey"];
        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){

        std_string url = "/" + config.endpoint + "/listenKey?listenKey=" + listen_key;
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
        q << "symbol=" << config.instrument_upper
        << "&timestamp=" << ts
        << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "https://" + config.base_url + "/" + config.endpoint + "/commissionRate?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::get, url, headers);
        auto arr = json::parse(res);

        config.maker_fee_rate = stod(arr["makerCommissionRate"].get<std_string>());
        config.taker_fee_rate = stod(arr["takerCommissionRate"].get<std_string>());

        cout << "maker_fee_rate: " << config.maker_fee_rate << " (" << config.maker_fee_rate * 100 << "%)\n";
        cout << "taker_fee_rate: " << config.taker_fee_rate << " (" << config.taker_fee_rate * 100 << "%)\n";
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
            if(p["symbol"] == config.instrument_upper) return stod(p["positionAmt"].get<std_string>());
        }
        return 0.0;
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    json place_limit(const Order& order, const double& price, const double& size){
        
        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << size
          << "&price=" << fixed << setprecision(config.price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << order.qty
          << "&reduceOnly=true"
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};
        
        auto res = http.request(http::verb::post, url, headers);
        return json::parse(res);
    }

    json cancel_order(const Order& order){
        
        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << config.instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = "/" + config.endpoint + "/order?" + query + "&signature=" + signature;
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
        const string hostname = "ws-" + config.base_url;
        const string target = "/ws-" + config.endpoint;

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
        cout << "STOPPING USER STREAM\n";
        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*ws).cancel(ec);

        if(stream_thread.joinable()) stream_thread.join();

        cout << "USER STREAM STOPPED\n";
    }

    void on_message(const std_string& msg){
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);
        cout << "msg1: " << msg << "\n";
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
        ev.type = ExecutionEventType::STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};

class BinanceFuturesUserStream : public BinanceUserStream {
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

    BinanceFuturesUserStream(MarketConfig& config, BinanceBroker& broker, ExecutionEventQueue& execution_event, BinanceClock& clock)
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
        std_string listen_key = broker.open_user_stream();

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
        ws = make_unique<ws_stream>(move(ssl_sock));
        ws->handshake(config.hostname, "/ws/" + listen_key);

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        beast::flat_buffer buffer;

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
        cout << "STOPPING USER STREAM\n";
        running = false;

        boost::system::error_code ec;
        beast::get_lowest_layer(*ws).cancel(ec);

        if(stream_thread.joinable()) stream_thread.join();

        cout << "USER STREAM STOPPED\n";
    }

    void on_message(const std_string& msg){
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

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
        ev.type = ExecutionEventType::STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};