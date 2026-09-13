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

// #include <curl/curl.h>
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
#include "mmpy_dashboard.hpp" //dashboard classes
#include "mmpy_feed.hpp" // feeds
#include "mmpy_state.hpp" //state & market feature state
#include "mmpy_recorder.hpp" //dataset recorder
#include "mmpy_strategy.hpp" // models & strategy

#include "mmpy_clock.hpp" // clock -> to be shifted

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
    const json& params;
    State& state;
    DatasetRecorder& recorder;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    double base_size;
    double max_inv;

    mt19937 rng;
    uniform_real_distribution<double> dist;

    int64_t exchange_latency;
    priority_queue<LatencyEvent, vector<LatencyEvent>, Compare> latency_queue;

    PaperExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, BinanceClock& clock, const json& params)
        : config(config), state(state), recorder(recorder), clock(clock), params(params)
    {
        base_size = params["base_size"].get<double>();
        max_inv = params["max_inv"].get<double>();
        exchange_latency = params["exchange_latency"].get<int64_t>();

        state.exchange_latency = exchange_latency;
        
        rng.seed(random_device{}());
        dist = uniform_real_distribution<double>(0.0, 1.0);\
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
        event.execute_ts = clock.now_ms() + exchange_latency;
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
            if(order.side == side && order.price_tick == price_tick && order.status == "LIVE") return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {

        update_trade_flow_buckets(trade);
        update_trade_flow(trade);
        match_side(trade);
    }

    void update_trade_flow_buckets(const Trade& trade){

        int64_t price_tick = config.to_tick(trade.price);

        auto& bucket = state.trade_buckets[trade.side][price_tick];
        bucket.push_back(trade);

        while(!bucket.empty() && trade.ts - bucket.front().ts > state.trade_flow_window_ms){
            bucket.pop_front();
        }
    }

    void update_trade_flow(const Trade& trade){

        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;
        
        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){
        double inv = state.inventory;
        double vol = state.get_vol();

        double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

        double inv_scale = 5.0;
        double inv_signal = tanh(inv / inv_scale);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        double risk_penalty = exp(-0.2 * inv * inv);

        double toxicity_penalty = exp(-signal.toxicity.k2 * signal.toxicity.tox); //negative markout translates into positive exp

        double base = base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        double bid_size = size * bid_multiplier;
        double ask_size = size * ask_multiplier;

        // inventory limits
        double max_buy = max(0.0, max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell = max(0.0, max_inv + inv);

        bid_size = min(bid_size, max_buy);
        ask_size = min(ask_size, max_sell);

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        return {
            bid_size,
            ask_size
        };
    }

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
        order.exchange_latency = exchange_latency;
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
        order.exchange_latency = exchange_latency;
        order.owner = "self";
        order.signal = *state.last_signal;
        order.queue_ahead_at_join = 0.0;

        open_orders[client_oid] = order;   // <-- insert into map

        execute_market(&order);
    }

    void cancel_order(Order* order){

        order->ts = clock.now_ms();
        order->status = "CANCELED";
        order->exchange_latency = exchange_latency;

        state.last_order_update = *order;
        state.reset_queue_ahead(order->side, order->price_tick);

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
        if(!bid_order && bid_size > 0.0){ // if no bid order
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
        if(!ask_order && ask_size > 0.0){ // if no ask order
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

    double get_trade_rate(const Trade& trade){

        int64_t price_tick = config.to_tick(trade.price);

        auto& bucket = state.trade_buckets[trade.side][price_tick]; //guaranteed to be pruned at update_trade_flow_buckets
        double volume = 0.0;

        for(auto& t: bucket) volume += t.qty;

        return volume / (state.trade_flow_window_ms / 1000.0); // per second (1000ms)
    }

    void match_side(const Trade& trade){

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);
        double qty = trade.qty;

        // -------------------------
        // GET dt (time interval between trades) -> amount of time you allow those events to happen
        // -------------------------
        if(state.last_fill_match_ts != 0){
            state.dt = (trade.ts - state.last_fill_match_ts) / 1000.0; // dt (s) represents interval between last_fill_model_ts and trade ts
        }
        state.last_fill_match_ts = trade.ts;

        // -------------------------
        // FILL CANDIDATE
        // -------------------------
        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;

        // -------------------------
        // QUEUE + FLOW MODEL
        // -------------------------
        double queue_ahead = state.compute_queue_ahead(side, price_tick);
        double trade_rate = get_trade_rate(trade);

        double lambda_fill = trade_rate / (queue_ahead + 1e-9); // expected fill events per second
        double p_fill = 1.0 - exp(-lambda_fill * state.dt); // probability that at least one fill event happens during that time
        // double p_fill = 1.0 - exp(-lambda_fill * 0.1); // dt = 100ms

        // -------------------------
        // How to increase p_fill
        // -------------------------
        // 1. Higher trade_rate
        // More aggressive market taking liquidity
        // More volume consumes the queue ahead of you
        // Increases lambda_fill

        // 2. Lower queue_ahead
        // Less volume in front of your order
        // Easier for trades to reach you
        // Increases lambda_fill

        // 3. Longer dt
        // More time for the process to act
        // Increases the chance that the queue gets consumed

        // stochastic acceptance
        if(p_fill < dist(rng)) return;

        // if(toxicity_model){
        //     ToxicityPrediction p;
                // p.ts = stream.exchange_ts;
            // //p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
        //     p.horizon_ms = toxicity_model->horizon_ms;

        //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
        //     p.fill_price = fill_price;

        //     p.side = (side == "BUY") ? +1 : -1;

        //     state.market_feature_state.toxicity_predictions.push_back(move(p));
        // }

        // -------------------------
        // APPLY FILL
        // -------------------------
        double fill_qty = min(order->remaining, qty);
        order->remaining -= fill_qty;

        state.on_fill(trade.price, fill_qty, order->side, true);

        order->status = "PARTIALLY_FILLED";

        if(order->remaining == 0){
            order->status = "FILLED";
            state.reset_queue_ahead(side, order->price_tick);
        }

        state.last_order_update = *order;

        recorder.log_fill(*order, fill_qty, trade.ts + clock.offset_ms.load(), true);

        if(order->remaining == 0) open_orders.erase(order->client_oid);
    }

    void orderMarketToString(Order* order, const double& fill_qty){
        cout << format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), fill_qty, order->status) << "\n";
    }

    void execute_market(Order* order){

        auto& book = state.market_book;

        if(order->side == "BUY"){
            for(auto it = book.asks.begin(); it != book.asks.end() && order->remaining > 0;){
                
                order->price_tick = it->first; //set to current market order price

                double fill_qty = min(order->remaining, it->second);
                order->remaining -= fill_qty;
                it->second -= fill_qty;

                if(order->remaining == 0) order->status = "FILLED";
                else order->status = "PARTIALLY_FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "BUY", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;
                orderMarketToString(order, fill_qty);

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0) it = book.asks.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
        else if(order->side == "SELL"){
            for(auto it = book.bids.begin(); it != book.bids.end() && order->remaining > 0;){

                order->price_tick = it->first;
                
                double fill_qty = min(order->remaining, it->second);
                order->remaining -= fill_qty;
                it->second -= fill_qty;
                
                if(order->remaining == 0) order->status = "FILLED";
                else order->status = "PARTIALLY_FILLED";

                state.on_fill(config.from_tick(it->first), fill_qty, "SELL", false);

                state.last_fill_candidate = *order;
                state.last_order_update = *order;
                orderMarketToString(order, fill_qty);

                recorder.log_fill(*order, fill_qty, clock.now_ms(), false);

                if(it->second <= 0) it = book.bids.erase(it);   // erase returns the next iterator
                else ++it;
            }
        }
    }

    void apply_stream_update(const Stream& stream) override {}
};

class BinanceBroker {
public:
    MarketConfig& config;
    BinanceClock& clock;

    std_string api_key;
    std_string api_secret;
    std_string base_url;
    std_string instrument_upper;

    std_string listen_key;
    atomic<bool> keepalive_running{false};
    thread keepalive_thread;

    HttpClient http;

    BinanceBroker(MarketConfig& config, BinanceClock& clock, const json& params)
        : config(config), clock(clock)
    {
        api_key = params["api"]["api_key"].get<std_string>();
        api_secret = params["api"]["api_secret"].get<std_string>();
        base_url = params["api"]["base_url"].get<std_string>();
        instrument_upper = params["instrument"].get<std_string>();
        transform(instrument_upper.begin(), instrument_upper.end(), instrument_upper.begin(), [](unsigned char c){return toupper(c);});

        http.initialize(base_url);
    }
    // -------------------------
    // USER STREAM
    // -------------------------
    std_string open_user_stream(){
        std_string url = base_url + "/fapi/v1/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};

        auto res = http.request(
            http::verb::post,
            "/fapi/v1/listenKey",
            headers
        );
        auto j = json::parse(res);

        listen_key = j["listenKey"];

        start_keepalive_loop();
        return listen_key;
    }

    void keepalive_listen_key(){
        std_string url = base_url + "/fapi/v1/listenKey";
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};

        http.request(
            http::verb::put,
            "/fapi/v1/listenKey?listenKey=" + listen_key,
            headers
        );
    }

    void start_keepalive_loop(){
        keepalive_running = true;

        keepalive_thread = thread([this](){
            while(keepalive_running){
                this_thread::sleep_for(minutes(20));
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

    void stop_keepalive(){
        keepalive_running = false;

        if(keepalive_thread.joinable()) keepalive_thread.join();
    }

    // -------------------------
    // SIGNING
    // -------------------------
    std_string sign(const std_string& query){
        unsigned char* digest;
        digest = HMAC(EVP_sha256(),
                      api_secret.c_str(),
                      api_secret.size(),
                      (unsigned char*)query.c_str(),
                      query.size(),
                      NULL, NULL);

        char mdString[65];
        for(int i = 0; i < 32; i++)
            sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);

        return std_string(mdString);
    }

    // -------------------------
    // ORDER PLACEMENT
    // -------------------------
    double get_position(){

        int64_t ts = clock.now_ms();
        ostringstream q;
        q << "timestamp=" << ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v2/positionRisk?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res =
            http.request(
                http::verb::get,
                "/fapi/v2/positionRisk?"
                + query +
                "&signature="
                + signature,
                headers
            );
        
        auto arr = json::parse(res);
        for(auto& p: arr){
            if(p["symbol"] == instrument_upper) return stod(p["positionAmt"].get<std_string>());
        }
        return 0.0;
    }

    json place_limit(const Order& order, const double& price, const double& size){
        
        ostringstream q;        
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << instrument_upper
          << "&side=" << order.side
          << "&type=LIMIT"
          << "&timeInForce=GTC"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << size
          << "&price=" << fixed << setprecision(config.price_precision) << price
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        cout << "A\n";
        auto res =
            http.request(
                http::verb::post,
                "/fapi/v1/order?"
                + query
                + "&signature="
                + signature,
                headers
            );
            cout << "B\n";
        
        return json::parse(res);
    }

    json place_market(const Order& order){

        ostringstream q;
        q << "newClientOrderId=" << order.client_oid
          << "&symbol=" << instrument_upper
          << "&side=" << order.side
          << "&type=MARKET"
          << "&quantity=" << fixed << setprecision(config.qty_precision) << order.qty
          << "&reduceOnly=true"
          << "&timestamp=" << order.ts
          << "&recvWindow=5000";

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        auto res =
    http.request(
        http::verb::post,
        "/fapi/v1/order?"
        + query
        + "&signature="
        + signature,
        headers
    );
        
        return json::parse(res);
    }

    json cancel_order(const Order& order){
        
        ostringstream q;
        q << "origClientOrderId=" << order.client_oid
          << "&symbol=" << instrument_upper << "&timestamp=" << order.ts;

        std_string query = q.str();
        std_string signature = sign(query);

        std_string url = base_url + "/fapi/v1/order?" + query + "&signature=" + signature;
        vector<std_string> headers = {"X-MBX-APIKEY: " + api_key};
        cout << "A\n";
        auto res =
    http.request(
        http::verb::delete_,
        "/fapi/v1/order?"
        + query
        + "&signature="
        + signature,
        headers
    );  
    cout << "B\n";

        return json::parse(res);
    }
};

class LiveExecution : public Execution {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    DatasetRecorder& recorder;
    BinanceBroker& broker;
    BinanceClock& clock;

    unordered_map<std_string, Order> open_orders;

    double current_bid_size = 0.0;
    double current_ask_size = 0.0;

    double last_bid = 0.0;
    double last_ask = 0.0;

    double base_size;
    double max_inv;

    LiveExecution(MarketConfig& config, State& state, DatasetRecorder& recorder, 
                BinanceBroker& broker, BinanceClock& clock, const json& params)
        : config(config), state(state), recorder(recorder), broker(broker), clock(clock), params(params)
    {
        base_size = params["base_size"];
        max_inv = params["max_inv"];
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

    void place_quotes_latency(const Signal& signal) override {}
    // Live execution has no simulated latency queue

    bool process_latency_queue() override {
        return true; // Live execution processes immediately through exchange events
    }

    Order* get_fill_candidate_order(const std_string& side, const int64_t& price_tick){
        for(auto& [client_oid, order]: open_orders){
            if(order.side == side && order.price_tick == price_tick && order.status == "LIVE") return &order;
        }
        return nullptr;
    }

    void process_trade(const Trade& trade) override {
        update_trade_flow_buckets(trade);
        update_trade_flow(trade);

        std_string side = (trade.side == "BUY") ? "SELL" : "BUY";
        int64_t price_tick = config.to_tick(trade.price);

        Order* order = get_fill_candidate_order(side, price_tick);

        if(!order) return;

        state.last_fill_candidate = *order;
    }

    void update_trade_flow_buckets(const Trade& trade){

        int64_t price_tick = config.to_tick(trade.price);

        auto& bucket = state.trade_buckets[trade.side][price_tick];
        bucket.push_back(trade);

        while(!bucket.empty() && trade.ts - bucket.front().ts > state.trade_flow_window_ms){
            bucket.pop_front();
        }
    }

    void update_trade_flow(const Trade& trade){
        
        // double flow = (trade.side == "BUY") ? trade.qty : -trade.qty;
        // double normalized_flow = flow / state.avg_trade_size;

        double flow = (trade.side == "BUY") ? 1.0 : -1.0;
        double alpha = 0.2;

        state.trade_imbalance = alpha * flow + (1 - alpha) * state.trade_imbalance;
    }

    pair<double, double> compute_order_size(const Signal& signal){
        double inv = state.inventory;
        double vol = state.get_vol();

        double vol_penalty = 1.0 / (1.0 + 50.0 * vol);

        double inv_scale = 5.0;
        double inv_signal = tanh(inv / inv_scale);

        double bid_multiplier = exp(-inv_signal);
        double ask_multiplier = exp(inv_signal);

        double risk_penalty = exp(-0.2 * inv * inv);

        double toxicity_penalty = exp(-signal.toxicity.k2 * signal.toxicity.tox); //negative markout translates into positive exp

        double base = base_size * vol_penalty * risk_penalty;
        double size = base * toxicity_penalty;

        size = max(0.05, min(size, 2.0));

        double bid_size = size * bid_multiplier;
        double ask_size = size * ask_multiplier;

        // inventory limits
        double max_buy = max(0.0, max_inv - inv); // NEW RISK GUARD HERE, PREVENT EXCEEDING MARGIN
        double max_sell = max(0.0, max_inv + inv);

        bid_size = min(bid_size, max_buy);
        ask_size = min(ask_size, max_sell);

        // exchange LOT_SIZE normalization
        bid_size = config.normalize_qty(bid_size);
        ask_size = config.normalize_qty(ask_size);

        return {
            bid_size,
            ask_size
        };
    }

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

        if(resp.contains("code")){ // TO BE DELETED
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

        json resp = broker.place_market(order);
        order.resp = resp;
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

    void orderMarketToString(Order* order, const Stream& stream){
        cout << format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, stream.fill_price, stream.fill_qty, order->status) << "\n";
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

        // order->exchange_latency = stream.exchange_ts - (order->send_ts + binance_clock_offset_ms.load()); // Exchange latency:
        // How long it took from your order leaving your machine until Binance generated the order update event, for quote placements / fills

        // Example:
        // exchange_latency = 8ms
        // ack_latency      = 250ms

        // means:

        // Binance processed the order quickly.
        // Most time was network/user-stream delay.

        // Or:

        // exchange_latency = 150ms
        // ack_latency      = 170ms

        // means:

        // Binance itself was slow.

        state.exchange_latency = order->exchange_latency;
        
        // push_latency(order->exchange_latency);
 
        // -------------------------
        // NEW
        // -------------------------
        if(stream.exec_type == "NEW"){
            
            order->live_ts = stream.exchange_ts;
            order->status = "LIVE";
            order->queue_ahead_at_join = state.set_queue_position(stream.side, order->price_tick);
            
            (stream.side == "BUY") ? last_bid = stream.price : last_ask = stream.price;
            state.last_order_update = *order;

            cout << "- PLACE LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "NEW");
        }

        // -------------------------
        // CANCELED
        // -------------------------
        else if(stream.exec_type == "CANCELED"){
            
            order->status = "CANCELED";
            state.reset_queue_ahead(stream.side, order->price_tick);
            state.last_order_update = *order;

            cout << "- CANCEL LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";

            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "CANCELED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // REJECTED - PENDING NEW ORDER REJECTED
        // -------------------------
        else if(stream.exec_type == "REJECTED"){
            
            order->status = "REJECTED";
            state.last_order_update = *order;

            cout << "- REJECTED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            
            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "REJECTED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // EXPIRED - FOR EXPIRED_IN_MATCH, IF EXCHANGE LAGS BEHIND NEW QUOTES
        // -------------------------
        else if(stream.exec_type == "EXPIRED"){
            
            order->status = "EXPIRED";
            state.reset_queue_ahead(stream.side, order->price_tick);
            state.last_order_update = *order;

            cout << "- EXPIRED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";

            recorder.log_quote(*order, (stream.side == "BUY") ? "BID" : "ASK", "EXPIRED");

            open_orders.erase(order->client_oid);
        }

        // -------------------------
        // TRADE
        // -------------------------
        else if(stream.exec_type == "TRADE"){
            
            // if(toxicity_model){
            //     ToxicityPrediction p;
                    // p.ts = stream.exchange_ts;
                // //p.ts = state.last_depth_ts;   // or ts, but be consistent with your system clock
            //     p.horizon_ms = toxicity_model->horizon_ms;

            //     p.pred = order.last_signal.cached_toxicity_pred;  // IMPORTANT: computed at quote time
            //     p.fill_price = fill_price;

            //     p.side = (side == "BUY") ? +1 : -1;

            //     state.market_feature_state.toxicity_predictions.push_back(move(p));
            // }

            state.on_fill(stream.fill_price, stream.fill_qty, stream.side, true);
            
            if(stream.status == "PARTIALLY_FILLED"){
                order->status = "PARTIALLY_FILLED";
                order->remaining -= stream.fill_qty;

                cout << "- PARTIALLY FILLED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            }

            else if(stream.status == "FILLED"){
                order->status = "FILLED";
                order->remaining = 0.0;
                state.reset_queue_ahead(stream.side, order->price_tick);

                cout << "- FILLED LIMIT ORDER - client_oid: " << order->client_oid << 
            ", status: " << order->status << ", timestamp: " << stream.exchange_ts << "\n";
            }
     
            state.last_order_update = *order;

            if(stream.order_type == "MARKET"){
                orderMarketToString(order, stream);
                recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, false);
            }
            else recorder.log_fill(*order, stream.fill_qty, stream.exchange_ts, true);
       
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
    State& state;
    LiveExecution& execution;
    BinanceBroker& broker;
    ExecutionEventQueue& execution_event;
    DatasetRecorder& recorder;

    simdjson::ondemand::parser parser;
    
    atomic<bool> running{false};
    atomic<bool> connected{false};
    thread stream_thread;

    mutex connection_mtx;
    condition_variable connection_cv;

    BinanceUserStream(State& state, LiveExecution& execution, BinanceBroker& broker, ExecutionEventQueue& execution_event, DatasetRecorder& recorder)
        : state(state), execution(execution), broker(broker), execution_event(execution_event), recorder(recorder) {}

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
        cout << "USER STREAM START\n";
        broker.open_user_stream();
        cout << "LISTEN KEY OK\n";

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
        ws.handshake("stream.binancefuture.com", "/ws/" + broker.listen_key);

        {
            lock_guard<mutex> lock(connection_mtx);
            connected = true;
        }
        connection_cv.notify_one();

        beast::flat_buffer buffer;

        while(running){
            ws.read(buffer);
            std_string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size()); // NEW

            on_message(msg);
        }
    }

    void stop(){
        running = false;

        if(stream_thread.joinable()) stream_thread.join();
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
        stream.exchange_ts = int64_t(o["T"]);

        ExecutionEvent ev;
        ev.type = ExecutionEventType::STREAM_UPDATE;
        ev.stream = stream;
        execution_event.push(ev);
    }
};

class Engine {
public:
    MarketConfig& config;
    const json& params;
    State& state;
    MarketMakingStrategy& strategy;
    Execution& execution;
    ExecutionEventQueue& execution_event;
    EventNotifier& dashboard_event;
    SnapshotStore& snapshot_store;
    DatasetRecorder& recorder;

    std_string header;

    atomic<bool> running{false};
    
    Engine(MarketConfig& config, State& state, MarketMakingStrategy& strategy, Execution& execution, ExecutionEventQueue& execution_event,
            EventNotifier& dashboard_event, SnapshotStore& snapshot_store, DatasetRecorder& recorder, const json& params)
        : config(config), state(state), strategy(strategy), execution(execution), execution_event(execution_event),
        dashboard_event(dashboard_event), snapshot_store(snapshot_store), recorder(recorder), params(params) {build_header();}

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
        }
    }

    void on_trade_event(const Trade& trade){
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
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(depth);
            book.apply_delta(depth);
            book.last_update_id = depth.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        execution.place_quotes(signal);
    }

    void on_depth_event_futures(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
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
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(depth);
            book.apply_delta(depth);
            book.last_update_id = depth.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        execution.place_quotes(signal);
    }

    void on_depth_event_spot_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
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
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(depth);
            book.apply_delta(depth);
            book.last_update_id = depth.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        execution.place_quotes_latency(signal);
    }

    void on_depth_event_futures_latency(const Depth& depth){
        // -------------------------
        // LIVE PROCESSING
        // -------------------------
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
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(depth);
            book.apply_delta(depth);
            book.last_update_id = depth.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
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
        execution.place_quotes_latency(signal);
    }

    void on_stream_event(const Stream& stream){
        execution.apply_stream_update(stream);
        execution.place_quotes(*state.last_signal);
    }

    std_string tradeToString(const optional<Trade>& trade){
        return trade ? format("{:<5} | {:>10.4f} | {:>8.6f}", trade->side, trade->price, trade->qty) : "—";
    }

    std_string orderPointerToString(Order* order){
        return order ? format("{:<5} | {:>10.4f} | {:>8.6f} [{}]", 
            order->side, config.from_tick(order->price_tick), order->remaining, order->status) : "—";
    }

    std_string orderToString(const optional<Order>& order){
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

        double bid_queue = state.compute_queue_ahead("bids", config.to_tick(best_bid));
        double ask_queue = state.compute_queue_ahead("asks", config.to_tick(best_ask));

        snap.title.header = header;
        snap.title.regime = state.last_signal ? state.last_signal->regime : "";
        snap.title.pnl_pct = state.get_pnl(mid) / state.initial_cash * 100;

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

        add(params["models"]["struct_model"].get<std_string>());
        add(params["models"]["regime_model"].get<std_string>());
        add(params["models"]["micro_signal_model"].get<std_string>());
        add(params["models"]["residual_model"].get<std_string>());
        add(params["models"]["toxicity_model"].get<std_string>());
        add(params["mode"].get<std_string>());
        add(params["exchange_new"].get<std_string>() + "_" + params["market"].get<std_string>());
        add(params["instrument"].get<std_string>());

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

    atomic<bool> engine_running{false};
    atomic<bool> dashboard_running{false};

    vector<thread> threads;

    TradingSystem(const json& params) : 
        params(params), config(params), state(config, params), strategy(config, params), recorder(config, state, params),
        clock(config, params), dashboard_terminal(snapshot_store), dashboard_server(snapshot_store, params) {initialize();}

    void initialize(){
        std_string mode = params["mode"].get<std_string>();
        if(mode == "live"){
            broker = make_unique<BinanceBroker>(config, clock, params);
            execution = make_unique<LiveExecution>(config, state, recorder, *broker, clock, params);
            user_stream = make_unique<BinanceUserStream>(state, *dynamic_cast<LiveExecution*>(execution.get()), *broker, execution_event, recorder);
        }
        
        else if(mode != "live"){
            execution = make_unique<PaperExecution>(config, state, recorder, clock, params);
        }

        engine = make_unique<Engine>(config, state, strategy, *execution, execution_event, dashboard_event, snapshot_store, recorder, params);

        std_string exchange = params["exchange_new"].get<std_string>();
        std_string market = params["market"].get<std_string>();
        auto log_event = [this](const std_string& type, const int64_t& ts, const std_string& msg) {recorder.log_event(type, ts, msg);};
        auto log_orderbook_snapshot = [this](const json& snapshot) {recorder.log_orderbook_snapshot(snapshot);};
        
        if(exchange == "binance" && market == "spot" && mode != "replay"){
            feed = make_unique<BinanceSpotFeed>(config, state, execution_event, clock, log_event, log_orderbook_snapshot, params);
        }

        if(exchange == "binance" && market == "futures" && mode != "replay"){
            feed = make_unique<BinanceFuturesFeed>(config, state, execution_event, clock, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance" && market == "spot" && mode == "replay"){
            feed = make_unique<BinanceSpotReplayFeed>(config, state, execution_event, clock, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance" && market == "futures" && mode == "replay"){
            feed = make_unique<BinanceFuturesReplayFeed>(config, state, execution_event, clock, log_event, log_orderbook_snapshot, params);
        }
    }

    void start(){
        engine_running = true;
        dashboard_running = true;

        clock.start();

        // dashboard_terminal.start();
        dashboard_server.start();

        feed->start();

        if(user_stream) user_stream->start();

        start_dashboard_loop();

        if(params["exchange_latency"].get<int64_t>() == 0){
            cout << "Starting execution loop\n";
            start_execution_loop();
        }
        else{
            cout << "Starting execution latency loop\n";
            start_execution_latency_loop();
        }

        // Wait 5 seconds
        this_thread::sleep_for(seconds(5)); //FOR TESTING
        
        recorder.export_orderbook_snapshot();
        recorder.export_event_parquet();
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
                // cout << "dashboard signal received, unlocking... ts: " << snap.system.last_depth_ts << "\n";
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

    void run_forever(){
        while(engine_running) this_thread::sleep_for(seconds(1));
        shutdown();
    }

    void shutdown(){
        cout << "INTERRUPT RECEIVED - SHUTTING DOWN\n";

        engine_running = false;
        feed->stop();

        //--------------------------------------------------
        // cancel orders
        //--------------------------------------------------
        cout << "CLOSING OPEN POSITIONS\n";
        execution->cancel_all_orders();

        while(execution->get_open_order("BUY") || execution->get_open_order("SELL")){
            this_thread::sleep_for(milliseconds(50));
        }

        //--------------------------------------------------
        // flatten inventory
        //--------------------------------------------------
        execution->place_market();

        while(abs(state.inventory) > 1e-9){
            this_thread::sleep_for(milliseconds(50));
        }

        //--------------------------------------------------
        // dashboards
        //--------------------------------------------------
        dashboard_running = false;
        // dashboard_terminal.stop();
        dashboard_server.stop();

        clock.stop();

        // if(broker) broker->stop_keepalive();

        for(auto& t: threads){
            if(t.joinable()) t.join();
        }

        cout << "\n";
        recorder.shutdown();
    }
};

int main(){
    // std_string path = "D:\\OneDrive\\Trading\\manifest_live.json";
    std_string path = "D:\\OneDrive\\Trading\\manifest_replay.json";
    
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

    return 0;
}