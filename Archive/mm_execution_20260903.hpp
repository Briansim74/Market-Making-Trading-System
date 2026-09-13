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
#include "mm_state.hpp" //state & market feature state
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

class Execution {
public:
    virtual double get_last_bid() = 0;
    virtual double get_last_ask() = 0;
    virtual double get_current_bid_size() = 0;
    virtual double get_current_ask_size() = 0;
    virtual void place_quotes_latency(const Signal&) = 0; //paper
    virtual bool process_latency_queue() = 0; //paper
    virtual void process_trade(const Trade&) = 0;
    virtual Order* get_open_order(const std_string&) = 0;
    virtual void cancel_all_orders() = 0;
    virtual void place_quotes(const Signal&) = 0;
    virtual void place_market() = 0;
    virtual void apply_stream_update(const Stream&) = 0;
    virtual ~Execution() = default;
};

class PaperExecution : public Execution {
public:
    MarketConfig& config;
    State& state;
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    mt19937 rng;
    uniform_real_distribution<double> dist;

    priority_queue<LatencyEvent, vector<LatencyEvent>, Compare> latency_queue;

    PaperExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, BinanceClock& clock)
        : config(config), state(state), recorder(recorder), clock(clock)
    {
        state.exchange_latency = config.exchange_latency;
        
        rng.seed(random_device{}());
        dist = uniform_real_distribution<double>(0.0, 1.0);
    }

    double get_last_bid() override {
        return last_bid;
    }

    double get_last_ask() override {
        return last_ask;
    }

    double get_current_bid_size() override {
        return current_bid_size;
    }

    double get_current_ask_size() override {
        return current_ask_size;
    }

    // -------------------------
    // LATENCY SIMULATION
    // -------------------------
    void place_quotes_latency(const Signal& signal) override {

        LatencyEvent event;
        event.execute_ts = clock.now_ms() + config.exchange_latency;
        event.type = "PLACE_QUOTES";
        event.signal = signal;

        latency_queue.push(event);
    }

    bool process_latency_queue() override {

        bool changed = false;

        while(!latency_queue.empty()){
            auto event = latency_queue.top();
            if(clock.now_ms() < event.execute_ts) break;

            latency_queue.pop();
            place_quotes(event.signal);

            changed = true;
        }
        return changed;
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick &&
            (order.status == "LIVE" || order.status == "PARTIALLY_FILLED")) return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {

        update_trade_flow(trade);
        match_side(trade);
    }

    void update_trade_flow(const Trade& trade){

        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;
        
        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){

        // Convert inventory to USDT notional for risk calculations.
        double inv_usdt = signal.inventory * signal.mid; // USDT per PEPE

        // Inventory scale should be in USDT, not PEPE quantity.
        double inv_scale_usdt = 1.0; // strongly reduce bid/ask size at 1usdt long/short to go to flat inv
        double inv_signal = tanh(inv_usdt / inv_scale_usdt);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        // Also preferably make this USDT-based.
        double vol_penalty = 1.0 / (1.0 + 50.0 * signal.volatility);
        double risk_penalty = exp(-0.2 * pow(inv_usdt / inv_scale_usdt, 2));
        double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

        // Size is USDT notional.
        double size_usdt = config.base_size * vol_penalty * risk_penalty * toxicity_penalty;

        // Exchange minimum is 1 USDT.
        size_usdt = max(config.base_size, min(size_usdt, 1.5 * config.base_size)); // max 1.5 base size usdt per order

        double bid_size_usdt = size_usdt * bid_multiplier;
        double ask_size_usdt = size_usdt * ask_multiplier;

        // Inventory limits in USDT.
        double max_buy_usdt = max(0.0, config.max_inv - inv_usdt); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell_usdt = max(0.0, config.max_inv + inv_usdt);

        bid_size_usdt = min(bid_size_usdt, max_buy_usdt);
        ask_size_usdt = min(ask_size_usdt, max_sell_usdt);

        // Convert USDT notional -> PEPE quantity.
        double bid_size = bid_size_usdt / signal.mid;
        double ask_size = ask_size_usdt / signal.mid;

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        // Re-check actual notional AFTER normalization.
        if(bid_size_usdt < config.base_size) bid_size = 0.0;
        if(ask_size_usdt < config.base_size) ask_size = 0.0;

        return {bid_size, ask_size};
    }

    // pair<double, double> compute_order_size(const Signal& signal){
    //     double inv = state.inventory;
    //     double vol = state.get_vol();

    //     double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

    //     double inv_scale = 5.0;
    //     double inv_signal = tanh(inv / inv_scale);

    //     double bid_multiplier = exp(-inv_signal);
    //     double ask_multiplier = exp(inv_signal);

    //     double risk_penalty = exp(-0.2 * inv * inv);

    //     double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

    //     double base = config.base_size * vol_penalty * risk_penalty;
    //     double size = base * toxicity_penalty;

    //     size = max(0.05, min(size, 2.0));

    //     double bid_size = size * bid_multiplier;
    //     double ask_size = size * ask_multiplier;

    //     // inventory limits
    //     double max_buy = max(0.0, config.max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
    //     double max_sell = max(0.0, config.max_inv + inv);

    //     bid_size = min(bid_size, max_buy);
    //     ask_size = min(ask_size, max_sell);

    //     // exchange LOT_SIZE normalization
    //     bid_size = config.normalize_qty(bid_size);
    //     ask_size = config.normalize_qty(ask_size);

    //     return {bid_size, ask_size};
    // }

    Order* get_open_order(const std_string& side) override {     
        for(auto& [client_oid, order]: open_orders) if(order.side == side) return &order;
        return nullptr;
    }

    std_string uuid16(){
        auto u = boost::uuids::random_generator()();
        std_string s = boost::uuids::to_string(u);

        s.erase(remove(s.begin(), s.end(), '-'), s.end());
        return s.substr(0, 16);
    }

    void place_limit(const std_string& side, const double& price, const double& size, const Signal& signal){

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = config.to_tick(price);
        int64_t ts = clock.now_ms();

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = size;
        order.remaining = size;
        order.status = "LIVE";
        order.ts = ts;
        order.live_ts = ts;
        order.exchange_latency = state.exchange_latency;
        order.owner = "self";
        order.signal = signal;
        order.queue_ahead_at_join = state.set_queue_position(side, price_tick);

        open_orders[client_oid] = order;   // <-- insert into map

        recorder.log_quote(order, (side == "BUY") ? "BID" : "ASK", "NEW");
    }

    void place_market() override {

        double pos = state.inventory;
        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();
        
        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = (pos > 0) ? bid_tick : ask_tick;
        int64_t ts = clock.now_ms();
        std_string side = (pos > 0) ? "SELL" : "BUY";

        Order& order = open_orders[client_oid]; // <-- insert into map
        
        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = abs(pos);
        order.remaining = abs(pos);
        order.status = "LIVE";
        order.ts = ts;
        order.live_ts = ts;
        order.exchange_latency = state.exchange_latency;
        order.owner = "self";
        order.signal = *state.last_signal;
        order.queue_ahead_at_join = 0.0;

        open_orders[client_oid] = order;   // <-- insert into map

        execute_market(&order);
    }

    void cancel_order(Order* order){

        order->ts = clock.now_ms();
        order->status = "CANCELED";
        order->exchange_latency = state.exchange_latency;

        state.reset_queue_position(order->side);

        state.last_order_update = *order;

        recorder.log_quote(*order, (order->side == "BUY") ? "BID" : "ASK", "CANCELED");

        open_orders.erase(order->client_oid);
    }

    void cancel_all_orders() override {
        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        if(bid_order && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")) cancel_order(bid_order);

        if(ask_order && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")) cancel_order(ask_order);
    }

    void place_quotes(const Signal& signal) override {

        double desired_bid = signal.my_bid;
        double desired_ask = signal.my_ask;

        auto [bid_size, ask_size] = compute_order_size(signal);
        double tick = config.tick_size;

        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        current_bid_size = bid_size;
        current_ask_size = ask_size;

        // -------------------------
        // BID ORDERS
        // -------------------------
        if(!bid_order){ // if no bid order
            if(bid_size > 0.0){
                place_limit("BUY", desired_bid, bid_size, signal);
                last_bid = desired_bid;
            }
        }
        
        // if bid change
        else if(abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick && !bid_order->pending_cancel &&
            (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")){
            cancel_order(bid_order);

            if(bid_size > 0.0){
                place_limit("BUY", desired_bid, bid_size, signal);
                last_bid = desired_bid;
            }
        }

        // -------------------------
        // ASK ORDERS
        // -------------------------
        if(!ask_order){ // if no ask order
            if(ask_size > 0.0){
                place_limit("SELL", desired_ask, ask_size, signal);
                last_ask = desired_ask;
            }
        }

        // else if ask change
        else if(abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick && !ask_order->pending_cancel &&
            (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")){
            cancel_order(ask_order);

            if(ask_size > 0.0){
                place_limit("SELL", desired_ask, ask_size, signal);
                last_ask = desired_ask;
            }
        }
    }

    void match_side(const Trade& trade){

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;

        double fill_qty = 0.0;

        auto& queue_ahead = (side == "BUY") ? state.bid_queue_ahead : state.ask_queue_ahead;

        // cout << "match side order queue ahead before: " << queue_ahead.second << "\n";

        if(queue_ahead.second > 0.0){
            double removed_qty = min(queue_ahead.second, trade.qty);

            //remove matched trade qty from queue_ahead
            queue_ahead.second = max(0.0, queue_ahead.second - removed_qty);

            //get the remaining trade qty after using it for updating queue_ahead
            double remaining_trade_qty = trade.qty - removed_qty;

            //--------------------------------------------------
            // Trade reaches us
            //--------------------------------------------------
            if(remaining_trade_qty > 0.0){ //if there is still excess trade_qty after using it for updating queue_ahead
                fill_qty = min(order->remaining, remaining_trade_qty); // update fills
            }
        }
        
        //--------------------------------------------------
        // Already at front
        //--------------------------------------------------
        else{
            fill_qty = min(order->remaining, trade.qty); // update fills
        }

        // cout << "fill_qty: " << fill_qty << "\n";

        if(fill_qty > 0.0){
            order->remaining = max(0.0, order->remaining - fill_qty);

            state.on_fill(trade.price, fill_qty, order->side, true);
            // cout << "order->side: " << " side: " << side << "\n";

            if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
            else order->status = "FILLED";

            recorder.log_fill(*order, fill_qty, trade.ts + clock.offset_ms.load(), true);
        }

        // cout << "match side order queue ahead after: " << queue_ahead.second << "\n";

        state.last_order_update = *order;

        if(order->status == "FILLED"){
            state.reset_queue_position(side); // reset queue position
            open_orders.erase(order->client_oid);
        }
    }

    void execute_market(Order* order){

        auto& book = state.market_book;

        if(order->side == "BUY"){
            for(auto it = book.asks.begin(); it != book.asks.end() && order->remaining > 0;){
                
                order->price_tick = it->first; //set to current market order price

                double fill_qty = min(order->remaining, it->second);
                order->remaining = max(0.0, order->remaining - fill_qty);
                it->second = max(0.0, it->second - fill_qty);

                if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
                else order->status = "FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "BUY", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0.0) it = book.asks.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
        else if(order->side == "SELL"){
            for(auto it = book.bids.begin(); it != book.bids.end() && order->remaining > 0;){

                order->price_tick = it->first;
                
                double fill_qty = min(order->remaining, it->second);
                order->remaining = max(0.0, order->remaining - fill_qty);
                it->second = max(0.0, it->second - fill_qty);
                
                if(order->remaining > 0.0) order->status = "PARTIALLY_FILLED";
                else order->status = "FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "SELL", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0.0) it = book.bids.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }

        open_orders.erase(order->client_oid);
    }

    void apply_stream_update(const Stream& stream) override {}
};

class HttpClient {
public:
    MarketConfig& config;

    asio::io_context ioc;
    ssl::context ctx;
    ssl_stream ssl_sock;

    mutex mtx; // execution and broker keepalive mtx

    HttpClient(MarketConfig& config) : config(config), ctx(ssl::context::tlsv12_client), ssl_sock(ioc, ctx) {}

    void initialize(){
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        auto results = resolver.resolve(config.base_url, "443");

        asio::connect(ssl_sock.next_layer(), results);

        SSL_set_tlsext_host_name(ssl_sock.native_handle(), config.base_url.c_str());
        ssl_sock.handshake(ssl::stream_base::client);

        cout << "[HTTP] broker connected: " << config.base_url << "\n";
    }

    string request(http::verb method, const string& target,
                const vector<string>& headers = {}, const string& body = ""){

        lock_guard<mutex> lock(mtx);

        http::request<http::string_body> req{method, target, 11};

        req.set(http::field::host, config.base_url);
        req.set(http::field::user_agent, "mm-engine");

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

        http::write(ssl_sock, req);
        http::response<http::string_body> res;
        http::read(ssl_sock, buffer, res);

        if(res.result_int() >= 400){
            cout << "ERROR: status >= 400: HTTP " + to_string(res.result_int()) + ": " + res.body();
        }

        cout << "\n===== HTTP RESPONSE =====\n";
        if(method != http::verb::get) cout << res << endl;

        return res.body();
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

    // std_string listen_key;
    // atomic<bool> keepalive_running{false};

    // mutex keepalive_mtx;
    // condition_variable keepalive_cv;

    // thread keepalive_thread;

    BinanceSpotBroker(MarketConfig& config, BinanceClock& clock)
        : config(config), clock(clock), http(config) {http.initialize();}

    // // -------------------------
    // // USER STREAM
    // // -------------------------
    std_string open_user_stream(){
        return "";
    }
    //     std_string url = config.endpoint + "/listenKey";
    //     vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

    //     auto res = http.request(http::verb::post, url, headers);
    //     auto j = json::parse(res);

    //     listen_key = j["listenKey"];
    //     start_keepalive_loop();
    //     return listen_key;
    // }

    // void keepalive_listen_key(){
    //     std_string url = config.endpoint + "/listenKey?listenKey=" + listen_key;
    //     vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

    //     http.request(http::verb::put, url, headers);
    // }

    // void start_keepalive_loop(){
    //     keepalive_running = true;

    //     keepalive_thread = thread([this](){
    //         unique_lock<mutex> lock(keepalive_mtx);

    //         while(true){
    //             if(keepalive_cv.wait_for(lock, minutes(20),
    //                 [this]{return !keepalive_running;})) break;

    //             try{
    //                 keepalive_listen_key();
    //                 cout << "[keepalive sent]\n";
    //             }
    //             catch(...){
    //                 cout << "[keepalive error]\n";
    //             }
    //         }
    //     });
    // }

    void stop(){
        // keepalive_running = false;
        // keepalive_cv.notify_one();

        // if(keepalive_thread.joinable()) keepalive_thread.join();
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
    double get_position(){
        std_string asset = "PEPE";
        // std_string asset = = config.instrument_upper.substr(0, config.instrument_upper.size() - 4);

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = config.endpoint + "/account?" + query + "&signature=" + signature;
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::delete_, url, headers);
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
        : config(config), clock(clock), http(config) {http.initialize();}

    // -------------------------
    // USER STREAM
    // -------------------------
    std_string open_user_stream(){
        std_string url = config.endpoint + "/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::post, url, headers);
        auto j = json::parse(res);

        listen_key = j["listenKey"];
        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){
        std_string url = config.endpoint + "/listenKey?listenKey=" + listen_key;
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
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

        std_string url = config.endpoint + "/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + config.api_key};

        auto res = http.request(http::verb::delete_, url, headers);
        return json::parse(res);
    }
};

class LiveExecution : public Execution {
public:
    MarketConfig& config;
    State& state;
    DatasetRecorder& recorder;
    BinanceBroker& broker;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    LiveExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, BinanceBroker& broker, BinanceClock& clock)
        : config(config), state(state), recorder(recorder), broker(broker), clock(clock) {}

    double get_last_bid() override {
        return last_bid;
    }

    double get_last_ask() override {
        return last_ask;
    }

    double get_current_bid_size() override {
        return current_bid_size;
    }

    double get_current_ask_size() override {
        return current_ask_size;
    }

    void place_quotes_latency(const Signal& signal) override {}

    bool process_latency_queue() override {
        return true; // Live execution processes immediately through exchange events
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick &&
            (order.status == "LIVE" || order.status == "PARTIALLY_FILLED")) return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {
        
        update_trade_flow(trade);

        // state.hawkes.update(depletion, entry.ts); // hawkes process

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;
    }

    void update_trade_flow(const Trade& trade){
        
        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;

        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){

        // Convert inventory to USDT notional for risk calculations.
        double inv_usdt = signal.inventory * signal.mid; // USDT per PEPE

        // Inventory scale should be in USDT, not PEPE quantity.
        double inv_scale_usdt = 1.0; // strongly reduce bid/ask size at 1usdt long/short to go to flat inv
        double inv_signal = tanh(inv_usdt / inv_scale_usdt);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        // Also preferably make this USDT-based.
        double vol_penalty = 1.0 / (1.0 + 50.0 * signal.volatility);
        double risk_penalty = exp(-0.2 * pow(inv_usdt / inv_scale_usdt, 2));
        double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

        // Size is USDT notional.
        double size_usdt = config.base_size * vol_penalty * risk_penalty * toxicity_penalty;

        // Exchange minimum is 1 USDT.
        size_usdt = max(config.base_size, min(size_usdt, 1.5 * config.base_size)); // max 1.5 base size usdt per order

        double bid_size_usdt = size_usdt * bid_multiplier;
        double ask_size_usdt = size_usdt * ask_multiplier;
        
        // Inventory limits in USDT.
        double max_buy_usdt = max(0.0, config.max_inv - inv_usdt); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell_usdt = max(0.0, config.max_inv + inv_usdt);

        bid_size_usdt = min(bid_size_usdt, max_buy_usdt);
        ask_size_usdt = min(ask_size_usdt, max_sell_usdt);

        // Convert USDT notional -> PEPE quantity.
        double bid_size = bid_size_usdt / signal.mid;
        double ask_size = ask_size_usdt / signal.mid;

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        // Re-check actual notional AFTER normalization.
        if(bid_size_usdt < config.base_size) bid_size = 0.0;
        if(ask_size_usdt < config.base_size) ask_size = 0.0;

        cout << "bid_size: " << bid_size << "\n";
        cout << "ask_size: " << ask_size << "\n";

        return {bid_size, ask_size};
    }

    // pair<double, double> compute_order_size(const Signal& signal){
    //     double inv = state.inventory;
    //     double vol = state.get_vol();

    //     double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

    //     double inv_scale = 5.0;
    //     double inv_signal = tanh(inv / inv_scale);

    //     double bid_multiplier = exp(-inv_signal);
    //     double ask_multiplier = exp(inv_signal);

    //     double risk_penalty = exp(-0.2 * inv * inv);

    //     double toxicity_penalty = exp(-signal.toxicity.k_order_size * signal.toxicity.tox); //negative markout translates into positive exp

    //     double base = config.base_size * vol_penalty * risk_penalty;
    //     double size = base * toxicity_penalty;

    //     size = max(0.05, min(size, 2.0));

    //     double bid_size = size * bid_multiplier;
    //     double ask_size = size * ask_multiplier;

    //     // inventory limits
    //     double max_buy = max(0.0, config.max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
    //     double max_sell = max(0.0, config.max_inv + inv);

    //     bid_size = min(bid_size, max_buy);
    //     ask_size = min(ask_size, max_sell);

    //     // exchange LOT_SIZE normalization
    //     bid_size = config.normalize_qty(bid_size);
    //     ask_size = config.normalize_qty(ask_size);

    //     return {bid_size, ask_size};
    // }

    Order* get_open_order(const std_string& side) override {     
        for(auto& [client_oid, order]: open_orders) if(order.side == side) return &order;
        return nullptr;
    }

    std_string uuid16(){
        auto u = boost::uuids::random_generator()();
        std_string s = boost::uuids::to_string(u);

        s.erase(remove(s.begin(), s.end(), '-'), s.end());
        return s.substr(0, 16);
    }

    void place_limit(const std_string& side, const double& price, const double& size, const Signal& signal){

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = config.to_tick(price);

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = size;
        order.remaining = size;
        order.status = "PENDING_NEW";
        order.ts = clock.now_ms();
        order.owner = "self";
        order.signal = signal;

        cout << "- PLACE LIMIT ORDER - client_oid: " << order.client_oid << 
        ", status: " << order.status << ", timestamp: " << order.ts << "\n";

        json resp = broker.place_limit(order, price, size);
        order.resp = resp;

        if(resp.contains("code")){
            int code = resp["code"];

            if(code == -4003){
                cout << "Limit order rejected: Quantity less than or equal to zero.\n";
            }

            else if(code == -4164){
                cout << "Order's notional must be no smaller than 50 (unless you choose reduce only).\n";
            }
            // other exchange errors
            else cout << "Limit order failed: " << resp.dump() << "\n";

            order.ts = clock.now_ms();
            order.status = "REJECTED";
            order.exchange_latency = state.exchange_latency;

            recorder.log_quote(order, (order.side == "BUY") ? "BID" : "ASK", "REJECTED");
            open_orders.erase(order.client_oid);

            return;
        }

        recorder.log_quote(order, (side == "BUY") ? "BID" : "ASK", "NEW_SUBMITTED");
    }

    void place_market() override {

        double pos = broker.get_position();
        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        std_string client_oid = "MM-" + uuid16();
        int64_t price_tick = (pos > 0) ? bid_tick : ask_tick;
        std_string side = (pos > 0) ? "SELL" : "BUY";

        Order& order = open_orders[client_oid]; // <-- insert into map

        order.client_oid = client_oid;
        order.side = side;
        order.price_tick = price_tick;
        order.qty = abs(pos);
        order.remaining = abs(pos);
        order.status = "PENDING_NEW";
        order.ts = clock.now_ms();
        order.owner = "self";
        order.signal = *state.last_signal;
        order.queue_ahead_at_join = 0.0;

        cout << "- PLACE MARKET ORDER - client_oid: " << order.client_oid << 
        ", status: " << order.status << ", timestamp: " << order.ts << "\n";

        json resp = broker.place_market(order);
        order.resp = resp;

        if(resp.contains("code")){
            int code = resp["code"];

            if(code == -4003){
                cout << "Market order rejected: Quantity less than or equal to zero.\n";
            }

            else if(code == -4164){
                cout << "Order's notional must be no smaller than 50 (unless you choose reduce only).\n";
            }
            // other exchange errors
            else cout << "Market order failed: " << resp.dump() << "\n";

            order.ts = clock.now_ms();
            order.status = "REJECTED";
            order.exchange_latency = state.exchange_latency;

            open_orders.erase(order.client_oid);

            return;
        }
    }

    void cancel_order(Order* order){
        
        order->ts = clock.now_ms();
        order->status = "PENDING_CANCEL";
        order->pending_cancel = true;
        
        cout << "- CANCEL LIMIT ORDER - client_oid: " << order->client_oid << 
        ", status: " << order->status << ", timestamp: " << order->ts << "\n";

        json resp = broker.cancel_order(*order);
        order->resp = resp;

        if(resp.contains("code")){ // TO BE DELETED
            int code = resp["code"];

            if(code == -2011){ // Order already filled/canceled
                cout << "Cancel rejected: order no longer open\n";
            }
            // other exchange errors
            else cout << "Cancel failed: " << resp.dump() << "\n";
            return;
        }

        recorder.log_quote(*order, (order->side == "BUY") ? "BID" : "ASK", "CANCEL_SUBMITTED");
    }

    void cancel_all_orders() override {
        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        if(bid_order && !bid_order->pending_cancel &&
        (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")) cancel_order(bid_order);

        if(ask_order && !ask_order->pending_cancel &&
        (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")) cancel_order(ask_order);
    }

    void place_quotes(const Signal& signal) override {

        double desired_bid = signal.my_bid;
        double desired_ask = signal.my_ask;

        auto [bid_size, ask_size] = compute_order_size(signal);
        double tick = config.tick_size;

        Order* bid_order = get_open_order("BUY");
        Order* ask_order = get_open_order("SELL");

        current_bid_size = bid_size;
        current_ask_size = ask_size;

        // -------------------------
        // BID ORDERS
        // -------------------------
        if(!bid_order){ // if no bid order
            if(bid_size > 0.0) place_limit("BUY", desired_bid, bid_size, signal);
        }

        else if(abs(desired_bid - config.from_tick(bid_order->price_tick)) >= tick && !bid_order->pending_cancel &&
            (bid_order->status == "LIVE" || bid_order->status == "PARTIALLY_FILLED")){
            cancel_order(bid_order);
        }

        // -------------------------
        // ASK ORDERS
        // -------------------------
        if(!ask_order){ // if no ask order
            if(ask_size > 0.0) place_limit("SELL", desired_ask, ask_size, signal);
        }

        else if(abs(desired_ask - config.from_tick(ask_order->price_tick)) >= tick && !ask_order->pending_cancel &&
            (ask_order->status == "LIVE" || ask_order->status == "PARTIALLY_FILLED")){
            cancel_order(ask_order);
        }
    }

    Order* get_order(const std_string& msg_client_oid){
        for(auto& [client_oid, order]: open_orders) if(client_oid == msg_client_oid) return &order;
        return nullptr;
    }

    void orderToString(const Order* order, const Stream& stream, const std_string& order_type){
        cout << "- " << order_type << ((stream.order_type != "MARKET") ? " LIMIT ORDER" : " MARKET ORDER")
        << " - client_oid: " << order->client_oid <<
        ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
    }

    // -------------------------
    // BINANCE USER STREAM SIGNAL
    // -------------------------
    void apply_stream_update(const Stream& stream) override {

        Order* order = get_order(stream.client_oid);

        if(!order) return;

        // -------------------------
        // EXCHANGE LATENCY
        // -------------------------
        order->exchange_latency = clock.compute_exchange_latency(stream.exchange_ts, order->ts);
        state.exchange_latency = order->exchange_latency;
 
        // -------------------------
        // NEW
        // -------------------------
        if(stream.exec_type == "NEW"){
            
            order->live_ts = stream.exchange_ts;
            order->status = "LIVE";
            order->queue_ahead_at_join = state.set_queue_position(stream.side, order->price_tick);

            (stream.side == "BUY") ? last_bid = stream.price : last_ask = stream.price;
            state.last_order_update = *order;

            orderToString(order, stream, "PLACE");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "NEW");
        }

        // -------------------------
        // CANCELED
        // -------------------------
        else if(stream.exec_type == "CANCELED"){
            
            order->status = "CANCELED";

            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            orderToString(order, stream, "CANCEL");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "CANCELED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // EXPIRED - FOR EXPIRED_IN_MATCH, IF EXCHANGE LAGS BEHIND NEW QUOTES
        // -------------------------
        else if(stream.exec_type == "EXPIRED"){
            
            order->status = "EXPIRED";

            state.reset_queue_position(stream.side);

            state.last_order_update = *order;

            orderToString(order, stream, "EXPIRED");
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "EXPIRED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // TRADE
        // -------------------------
        else if(stream.exec_type == "TRADE"){
            // cout << "stream fill_price: " << stream.fill_price << ", stream fill_qty: " << stream.fill_qty << "\n";
            // cout << "stream fees paid: " << stream.fees_paid << "\n"; // maker_fees = 0.0002, same as params

            // if(toxicity_model){
            //     ToxicityPrediction p;
                    // p.ts = stream.exchange_ts;
                // //p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
            //     p.horizon_ms = toxicity_model->horizon_ms;

            //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
            //     p.fill_price = fill_price;

            //     p.side = (side == "BUY") ? 1 : -1;

            //     state.market_feature_state.toxicity_predictions.push_back(move(p));
            // }

            state.on_fill(stream.fill_price, stream.fill_qty, stream.side, true);
            
            if(stream.status == "PARTIALLY_FILLED"){
                order->status = "PARTIALLY_FILLED";
                order->remaining = max(0.0, order->remaining - stream.fill_qty);

                (stream.side == "BUY") ? state.bid_queue_ahead.second = 0.0 : state.ask_queue_ahead.second = 0.0;

                orderToString(order, stream, "PARTIALLY_FILLED");
            }

            else if(stream.status == "FILLED"){
                order->status = "FILLED";
                order->remaining = 0.0;
                
                state.reset_queue_position(stream.side);

                orderToString(order, stream, "FILLED");
            }

            // if(stream.order_type == "MARKET") orderMarketToString(order, stream);

            recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, (stream.order_type != "MARKET"));

            state.last_order_update = *order;
       
            if(stream.status == "FILLED") open_orders.erase(order->client_oid);
        }
        else{
            cout << "UNKNOWN ORDER UPDATE " << stream.exec_type << "\n";
            throw runtime_error("unknown order update");            
        }
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
        const string hostname = "ws-api.binance.com";
        const string target = "/ws-api/v3";
        cout << "[USER STREAM] resolving " << hostname << "\n";

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
                if(running) cerr << "[USER STREAM] read error: " << ec.message() << "\n";
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

            cout << "[USER STREAM] subscription response: " << msg << "\n";

            auto resp = json::parse(msg);

            if(!resp.contains("status")) continue;

            int status = resp["status"].get<int>();

            if(status != 200){
                throw runtime_error("userDataStream subscription failed: " + msg);
            }

            if(resp.contains("result") && resp["result"].contains("subscriptionId")){
                cout << "[USER STREAM] subscribed, subscriptionId=" << resp["result"]["subscriptionId"] << "\n";
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
        cout << "msg1: " << msg << endl;
        if(std_string(doc["e"]) != "executionReport") return;
        cout << msg << endl;

        Stream stream;
        stream.client_oid = std_string(doc["c"]);
        stream.side = std_string(doc["S"]);
        stream.status = std_string(doc["X"]);
        stream.exec_type = std_string(doc["x"]);
        stream.order_type = std_string(doc["o"]);
        stream.price = double(doc["p"].get_double_in_string());
        stream.qty = double(doc["q"].get_double_in_string());
        stream.fill_price = double(doc["L"].get_double_in_string());
        stream.fill_qty = double(doc["l"].get_double_in_string());
        stream.fees_paid = double(doc["n"].get_double_in_string());
        stream.exchange_ts = int64_t(doc["T"]);
        stream.local_ts = clock.now_ms();

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
        // ws->handshake(config.hostname, "/ws/" + broker.listen_key);
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
        cout << msg << endl;

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

        ExecutionEvent ev;
        ev.type = ExecutionEventType::STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};