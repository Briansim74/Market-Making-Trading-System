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

#include <curl/curl.h>
#include <cpr/cpr.h>
#include "simdjson.h"
#include <nlohmann/json.hpp>
#include <xgboost/c_api.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <parquet/arrow/reader.h>

#include <ftxui/dom/table.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>

#include "mmpy_structs.hpp" //structs
#include "mmpy_config.hpp" //market config & orderbook
#include "mmpy_dashboard.hpp" //dashboard classes

using std::cout;
using json = nlohmann::json;
using std_string = std::string;

using namespace std;
// using namespace arrow;
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
class State {
public:
    MarketConfig& config;
    const json& params;
    OrderBook market_book;
    double last_mid = 0.0;
    MarketFeatureState market_feature_state;

    optional<Signal> last_signal;

    double order_imbalance = 0.0;
    double trade_imbalance = 0.0;
    double ewma_var = 0.0;

    double inventory = 0.0;
    double cash = 100000.0;
    double initial_cash = 100000.0;
    double realized_pnl = 0.0;
    double avg_entry_price = 0.0;

    deque<double> equity_history;
    deque<double> return_history;
    double last_equity = 0.0;

    double maker_fee_rate;
    double taker_fee_rate;
    double fees_paid = 0.0;

    unordered_map<std_string, unordered_map<int64_t, double>> queue_flow;
    unordered_map<std_string, unordered_map<int64_t, double>> my_queue_position;
    unordered_map<double, deque<Trade>> trade_buckets;
    double window_ms = 1000;

    double last_trade_ts = 0;
    optional<Trade> last_trade;
    double last_depth_ts = 0;
    Order* last_fill_candidate = nullptr;
    Order* last_order_update = nullptr;

    bool initialized = false;

    State(MarketConfig& config, const json& params)
        : config(config), params(params), market_book(config, params),
        maker_fee_rate(params["fees"]["maker_fee_rate"].get<double>()),
        taker_fee_rate(params["fees"]["taker_fee_rate"].get<double>()) {}

    double get_vol(){
        return sqrt(ewma_var);
    }

    void update_vol(){
        double alpha = 0.90;
        double mid = market_book.mid();

        if(last_mid != 0.0){
            double r = log(mid / last_mid);
            ewma_var = alpha * ewma_var + (1 - alpha) * r * r;
        }
        last_mid = mid;
    }

    void compute_order_imbalance(){
        auto [bid_tick, bid_size] = market_book.best_bid();
        auto [ask_tick, ask_size] = market_book.best_ask();

        order_imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1e-9);
    }

    void on_fill(const double& price, double qty, const std_string& side, const std_string& liquidity){

        double old_inv = inventory;
        double old_avg = avg_entry_price;

        double fill_value = price * qty;
        double fee_rate = (liquidity == "maker") ? maker_fee_rate : taker_fee_rate;
        double fee = fill_value * fee_rate;
        fees_paid += fee;

        if(side == "BUY"){
            if(old_inv < 0){
                double close_qty = min(qty, abs(old_inv));
                realized_pnl += close_qty * (old_avg - price);

                old_inv += close_qty;
                qty -= close_qty;
            }

            inventory = old_inv;

            if(qty > 0){
                double new_inv = old_inv + qty;

                if(old_inv > 0){
                    avg_entry_price = (old_avg * old_inv + price * qty) / new_inv;
                }
                else avg_entry_price = price;

                inventory = new_inv;
            }
            cash -= (fill_value + fee);
        }

        else{ // SELL
            if(old_inv > 0){
                double close_qty = min(qty, old_inv);
                realized_pnl += close_qty * (price - old_avg);

                old_inv -= close_qty;
                qty -= close_qty;
            }

            inventory = old_inv;

            if(qty > 0){
                double new_inv = old_inv - qty;

                if(old_inv < 0){
                    avg_entry_price = (old_avg * abs(old_inv) + price * qty) / abs(new_inv);
                }
                else avg_entry_price = price;

                inventory = new_inv;
            }
            cash += (fill_value - fee);
        }

        if(inventory == 0.0) avg_entry_price = 0.0;
    }

    double get_unrealized_pnl(double mid){
        return inventory * (mid - avg_entry_price);
    }

    double get_pnl(){
        double mid = market_book.mid();
        return realized_pnl + inventory * (mid - avg_entry_price) - fees_paid;
    }

    double get_value(auto& m, const std_string& side, const int64_t& price){
        auto it1 = m.find(side);
        if(it1 == m.end()) return 0.0;

        auto it2 = it1->second.find(price);
        if(it2 == it1->second.end()) return 0.0;

        return it2->second;
    }

    double compute_queue_ahead(const std_string& side, const int64_t& price_tick){
        double my_pos = get_value(my_queue_position, side, price_tick);
        double flow   = get_value(queue_flow, side, price_tick);

        double ahead = my_pos - flow;
        return max(0.0, ahead);
    }

    void reset_queue_ahead(const std_string& side, const int64_t& price_tick){
        my_queue_position[side].erase(price_tick);
        queue_flow[side].erase(price_tick);
    }

    void update_queue_from_depth(const vector<pair<int64_t, double>>& bid_delta, 
                                    const vector<pair<int64_t, double>>& ask_delta){

        for(auto& [price_tick, q]: bid_delta){
            
            auto it = market_book.bids.find(price_tick);
            double old_qty = (it != market_book.bids.end()) ? it->second : 0.0;
            double new_qty = q;

            if(old_qty > 0.0){
                double depletion = max(0.0, old_qty - new_qty);
                queue_flow["bids"][price_tick] += depletion;
            }
        }

        for(auto& [price_tick, q]: ask_delta){
            
            auto it = market_book.asks.find(price_tick);
            double old_qty = (it != market_book.asks.end()) ? it->second : 0.0;
            double new_qty = static_cast<double>(q);

            if(old_qty > 0.0){
                double depletion = max(0.0, old_qty - new_qty);
                queue_flow["asks"][price_tick] += depletion;
            }
        }
    }

    template <typename T> void push_limited(deque<T>& dq, const T& value, size_t maxlen = 10){
        dq.push_back(value);
        if(dq.size() > maxlen) dq.pop_front();
    }

    void update_market_feature_state(){
        auto& mfs = market_feature_state;
        auto& book = market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double spread = best_ask - best_bid;
        double microprice = (best_ask * bid_size + best_bid * ask_size) /(bid_size + ask_size + 1e-9);

        last_mid = mid;
        double mid_return = (mid - last_mid) / last_mid;
        
        double quote_churn;
        if(mfs.prev_best_bid == 0.0 && mfs.prev_best_ask == 0.0){
            quote_churn = 0.0;
        }
        else{
            double bid_delta = abs(best_bid - mfs.prev_best_bid);
            double ask_delta = abs(best_ask - mfs.prev_best_ask);
            quote_churn = bid_delta + ask_delta;
        }

        mfs.prev_best_bid = best_bid;
        mfs.prev_best_ask = best_ask;

        push_limited(mfs.mid_returns, mid_return);
        push_limited(mfs.spread, spread);
        push_limited(mfs.order_imbalance, order_imbalance);
        push_limited(mfs.trade_imbalance, trade_imbalance);
        push_limited(mfs.quote_churn, quote_churn);
        push_limited(mfs.inventory, inventory);
        push_limited(mfs.microprice_error, mid - microprice);
    }

    Regime get_regime(){
        auto& mfs = market_feature_state;

        auto mean = [](const deque<double>& q){
            if(q.empty()) return 0.0;
            double s = 0;
            for(auto x: q) s += x;
            return s / q.size();
        };

        auto stddev = [](const deque<double>& q){
            if(q.size() < 2) return 0.0;
            double m = 0;
            for(auto x: q) m += x;
            m /= q.size();

            double var = 0;
            for(auto x: q) var += (x - m) * (x - m);
            return sqrt(var / q.size());
        };

        Regime regime;
        regime.volatility = stddev(mfs.mid_returns);
        regime.spread = mean(mfs.spread);
        regime.order_imbalance = mean(mfs.order_imbalance);
        regime.trade_imbalance = mean(mfs.trade_imbalance);
        regime.quote_churn = mean(mfs.quote_churn);
        regime.inventory = mean(mfs.inventory);
        regime.inventory_vol = stddev(mfs.inventory);
        regime.microprice_error = mean(mfs.microprice_error);

        return regime;
    }

    void update_ml_realization(){
        auto& mfs = market_feature_state;
        double now = config.now_ms();

        while(!mfs.ml_predictions.empty() && now - mfs.ml_predictions.front().ts >= mfs.ml_horizon_ms){
            auto& entry = mfs.ml_predictions.front();
            mfs.ml_predictions.pop_front();
            
            entry.realized = log(last_mid / entry.reservation);
            mfs.ml_signal_log.push_back(entry);
        }
    }

    void update_performance(){
        auto [bid_tick, bid_size] = market_book.best_bid();
        auto [ask_tick, ask_size] = market_book.best_ask();

        if(bid_size <= 0 || ask_size <= 0) return;

        double bid = config.from_tick(bid_tick);
        double ask = config.from_tick(ask_tick);

        double mid = (bid + ask) / 2.0;
        double equity = cash + inventory * mid;

        if(last_equity > 0.0 && equity > 0.0){
            double r = log(equity / last_equity);

            if(isfinite(r)) return_history.push_back(r);
        }

        last_equity = equity;
        equity_history.push_back(equity);
    }

    double compute_sharpe(){
        if(return_history.size() < 30) return 0.0;

        vector<double> returns;
        returns.reserve(return_history.size());

        for(double r: return_history){
            if(isfinite(r)) returns.push_back(r);
        }

        if(returns.size() < 30){
            cout << "SHARPE - len(returns) < 30:" << NAN << "\n";
            return NAN;
        }

        double mean = 0.0;
        for(double r: returns) mean += r;
        mean /= returns.size();

        double var = 0.0;
        for(double r: returns) var += (r - mean) * (r - mean);
        var /= returns.size();

        double std = sqrt(var);
        if(std == 0.0 || isnan(std)){
            cout << "SHARPE - std == 0 / isnan(std):" << NAN << "\n";
            return NAN;
        }

        double sharpe_ratio = mean / (std + 1e-9);
        cout << "SHARPE: " << sharpe_ratio << "\n";

        return sharpe_ratio;
    }
};
struct SnapshotRow {};
struct TradeRow {};
struct QuoteRow {};
struct FillRow {};
// struct EventsRow {
//     std_string type;
//     uint64_t ts;
//     std_string message;   // raw JSON string (same as Python)
// };
class DatasetRecorder {
public:
    DatasetRecorder(MarketConfig&, State&, const json&) {}
    void log_event(const std_string& type, const uint64_t& ts, const std_string& msg){}
    void log_orderbook_snapshot(const json& snapshot){}
};
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
    std_string instrument;
    std_string instrument_upper;

    function<void()> on_market_data;
    function<void(const Trade&)> on_trade_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    mutex cv_mtx; //condition variable lock
    mutex buffer_mtx;
    condition_variable cv;
    atomic<bool> running{false};
    atomic<bool> buffering{true};
    atomic<bool> first_depth_received{false};

    vector<Depth> depth_buffer;

    BinanceSpotFeed(MarketConfig& config, State& state, function<void()> on_market_data,
                    function<void(const Trade&)> on_trade_event, 
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_market_data(move(on_market_data)),
                    on_trade_event(move(on_trade_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot)), 
                    instrument(params["instrument"].get<std_string>()),
                    instrument_upper(instrument) {
                        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
                    }

    void parse_book(simdjson::ondemand::object obj,
                vector<pair<int64_t, double>>& bid_delta,
                vector<pair<int64_t, double>>& ask_delta){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            ask_delta.emplace_back(config.to_tick(p), q);
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
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.ts = doc["T"];

            log_event("trade", trade.ts, msg);

            state.last_trade = trade;
            state.last_trade_ts = trade.ts;

            on_trade_event(trade);
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

            Depth entry;
            entry.ts = uint64_t(doc["E"]);
            entry.U = uint64_t(doc["U"]);
            entry.u = uint64_t(doc["u"]);
            parse_book(doc.get_object(), entry.bid_delta, entry.ask_delta);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", entry.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(entry);
                    continue;
                }
            }
            
            // -------------------------
            // LIVE PROCESSING
            // -------------------------
            state.last_depth_ts = entry.ts;

            on_depth(entry);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceSpotFeed::trade_loop, this);
        depth_thread = thread(&BinanceSpotFeed::depth_loop, this);

        cout << "SOCKETS STARTED\n";

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

            state.market_book.apply_delta(it->bid_delta, it->ask_delta);
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

    void on_depth(const Depth& entry){

        auto& book = state.market_book;
        // cout << "entry.u: " << entry.u << " entry.ts: " << entry.ts << " entry.U: " << entry.U << '\n';
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(entry.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << entry.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry.bid_delta, entry.ask_delta);
            book.apply_delta(entry.bid_delta, entry.ask_delta);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_ml_realization();
        state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(state.initialized) on_market_data();
    }
};
class BinanceFuturesFeed : public Feed {
public:
    MarketConfig& config;
    State& state;
    std_string instrument;
    std_string instrument_upper;

    function<void()> on_market_data;
    function<void(const Trade&)> on_trade_event;

    function<void(const std_string&, const uint64_t&, const std_string&)> log_event;
    function<void(const json&)> log_orderbook_snapshot;

    thread depth_thread;
    thread trade_thread;

    atomic<bool> running{false};
    vector<Depth> depth_buffer;
    atomic<bool> buffering{true};

    mutex buffer_mtx;
    mutex cv_mtx; //condition variable lock
    atomic<bool> first_depth_received{false};
    condition_variable cv;

    BinanceFuturesFeed(MarketConfig& config, State& state, function<void()> on_market_data,
                    function<void(const Trade&)> on_trade_event, 
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_market_data(move(on_market_data)),
                    on_trade_event(move(on_trade_event)), log_event(move(log_event)),
                    log_orderbook_snapshot(move(log_orderbook_snapshot)), 
                    instrument(params["instrument"].get<std_string>()),
                    instrument_upper(instrument) {
                        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
                    }

    void parse_book(simdjson::ondemand::object obj,
                vector<pair<int64_t, double>>& bid_delta,
                vector<pair<int64_t, double>>& ask_delta){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            ask_delta.emplace_back(config.to_tick(p), q);
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
            trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
            trade.price = double(doc["p"].get_double_in_string());
            trade.qty   = double(doc["q"].get_double_in_string());
            trade.ts = doc["T"];

            log_event("trade", trade.ts, msg);

            state.last_trade = trade;
            state.last_trade_ts = trade.ts;

            on_trade_event(trade);
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

            Depth entry;
            entry.ts = uint64_t(doc["E"]);
            entry.pu = uint64_t(doc["pu"]);
            entry.U = uint64_t(doc["U"]);
            entry.u = uint64_t(doc["u"]);
            parse_book(doc.get_object(), entry.bid_delta, entry.ask_delta);

            first_depth_received = true;
            cv.notify_all();

            log_event("depth", entry.ts, msg);

            {
                lock_guard<mutex> lock(buffer_mtx);
                if(buffering){
                    depth_buffer.push_back(entry);
                    continue;
                }
            }
            
            // -------------------------
            // LIVE PROCESSING
            // -------------------------
            state.last_depth_ts = entry.ts;

            on_depth(entry);
        }
    }

    void start() override {
        running = true;
        buffering = true;

        depth_buffer.clear();
        first_depth_received = false;

        trade_thread = thread(&BinanceFuturesFeed::trade_loop, this);
        depth_thread = thread(&BinanceFuturesFeed::depth_loop, this);

        cout << "SOCKETS STARTED\n";

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

            state.market_book.apply_delta(it->bid_delta, it->ask_delta);
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

    void on_depth(const Depth& entry){
        
        auto& book = state.market_book;
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        if(entry.pu != book.last_update_id) {
            cout << "GAP DETECTED expected " << book.last_update_id << " got " << entry.pu << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry.bid_delta, entry.ask_delta);
            book.apply_delta(entry.bid_delta, entry.ask_delta);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_ml_realization();
        state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(state.initialized) on_market_data();
    }
};
struct EventsRow {
        std_string type;
        uint64_t ts;
        std_string msg;   // raw JSON string (same as Python)
    };
class ReplayFeed : public Feed {
public:
    State& state;
    MarketConfig& config;
    std_string events_path;
    std_string orderbook_snapshot_path;
    vector<EventsRow> events;
    json orderbook_snapshot;

    function<void()> on_market_data;
    function<void(const Trade&)> on_trade_event;

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
    
    ReplayFeed(MarketConfig& config, State& state, function<void()> on_market_data,
                    function<void(const Trade&)> on_trade_event, 
                    function<void(const std_string&, const uint64_t&, const std_string&)> log_event,
                    function<void(const json&)> log_orderbook_snapshot, const json& params):
                    config(config), state(state), on_market_data(move(on_market_data)),
                    on_trade_event(move(on_trade_event)), log_event(move(log_event)),
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

        auto type_col = static_pointer_cast<arrow::StringArray>(
            table->GetColumnByName("type")->chunk(0)
        );

        auto ts_col = static_pointer_cast<arrow::Int64Array>(
            table->GetColumnByName("ts")->chunk(0)
        );

        auto msg_col = static_pointer_cast<arrow::StringArray>(
            table->GetColumnByName("message")->chunk(0)
            // table->GetColumnByName("msg")->chunk(0) // TO BE CHANGED ONCE LOG EVENT IS SETTLED
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

    void parse_book(simdjson::ondemand::object obj,
                vector<pair<int64_t, double>>& bid_delta,
                vector<pair<int64_t, double>>& ask_delta){

        for(auto b: obj["b"]){
            auto arr = b.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            bid_delta.emplace_back(config.to_tick(p), q);
        }

        for(auto a: obj["a"]){
            auto arr = a.get_array();

            double p = double(arr.at(0).get_double_in_string());
            double q = double(arr.at(1).get_double_in_string());

            ask_delta.emplace_back(config.to_tick(p), q);
        }
    }

    void on_trade_message(const std_string& msg){

        simdjson::ondemand::parser parser;
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Trade trade;
        trade.side  = bool(doc["m"]) ? "SELL" : "BUY";
        trade.price = double(doc["p"].get_double_in_string());
        trade.qty   = double(doc["q"].get_double_in_string());
        trade.ts    = doc["T"];

        log_event("trade", trade.ts, msg);

        state.last_trade = trade;
        state.last_trade_ts = trade.ts;

        on_trade_event(trade);

    }

    void on_depth_message(const std_string& msg){

        simdjson::ondemand::parser parser;
        simdjson::padded_string json(msg);
        auto doc = parser.iterate(json);

        Depth entry;
        entry.ts = uint64_t(doc["E"]);
        entry.U  = uint64_t(doc["U"]);
        entry.u  = uint64_t(doc["u"]);
        parse_book(doc.get_object(), entry.bid_delta, entry.ask_delta);

        first_depth_received = true;
        cv.notify_all();
  
        log_event("depth", entry.ts, msg);
        
        if(!snapshot_aligned.load()){
            if(entry.U <= state.market_book.last_update_id + 1 && state.market_book.last_update_id + 1 <= entry.u){
                snapshot_aligned = true;

                state.market_book.apply_delta(entry.bid_delta, entry.ask_delta);
                state.market_book.last_update_id = entry.u;
                state.update_vol();

                cout << "BOOK SYNCHRONIZED\n";
            }
            return;
        }

        // -------------------------
        // LIVE PROCESSING
        // -------------------------
        state.last_depth_ts = entry.ts;

        on_depth(entry);
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

        cout << "SOCKETS STARTED\n";

        auto [snapshot_id, snapshot] = state.market_book.initialize_from_orderbook_snapshot(orderbook_snapshot);

        log_orderbook_snapshot(snapshot);

        replay_thread = thread(&ReplayFeed::run, this);

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

    void on_depth(const Depth& entry){

        auto& book = state.market_book;
        // cout << " entry.U: " << entry.U << " state.market_book.last_update_id: " << state.market_book.last_update_id << " entry.u: " << entry.u << '\n';
        // -----------------------------
        // DROP OLD EVENTS
        // -----------------------------
        if(entry.u <= book.last_update_id) return;

        // -----------------------------
        // GAP DETECTION
        // -----------------------------
        if(entry.U > book.last_update_id + 1){
            cout << "GAP DETECTED expected " << book.last_update_id + 1 << " got " << entry.U << "\n";
            state.initialized = false;
            return;
        }

        // -----------------------------
        // APPLY DELTA (FAST LOCK ONLY)
        // -----------------------------
        {
            lock_guard<recursive_mutex> lock(book.mtx);
            state.update_queue_from_depth(entry.bid_delta, entry.ask_delta);
            book.apply_delta(entry.bid_delta, entry.ask_delta);
            book.last_update_id = entry.u;
        }

        // -----------------------------
        // HEAVY FEATURES OUTSIDE LOCK (IMPORTANT)
        // -----------------------------
        state.update_vol();
        state.compute_order_imbalance();
        state.update_market_feature_state();
        state.update_ml_realization();
        state.update_performance();

        // -----------------------------
        // STRATEGY ONLY AFTER INIT
        // -----------------------------
        if(state.initialized) on_market_data();
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

    void on_market_data(){
        // if(!state.initialized) return;
        // Signal signal = strategy.on_market_update(state);

        // if(signal_queue.size() < 1000){
        //     signal_queue.push(signal);
        // }
        // recorder.log_snapshot(state.last_depth_ts, signal.value(), toupper(instrument));
    }

    void on_trade_event(const Trade& trade){
        // recorder.log_trade(trade, toupper(instrument));
        // execution.process_trade(trade);
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
        snap.risk.total_pnl = state.get_pnl();

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
        auto on_market = [this]() {engine->on_market_data();};
        auto on_trade = [this](const Trade& trade) {engine->on_trade_event(trade);};
        auto log_ev = [this](const std_string& type, const uint64_t& ts, const std_string& msg) {recorder.log_event(type, ts, msg);};
        auto log_ob_snapshot = [this](const json& snapshot) {recorder.log_orderbook_snapshot(snapshot);};

        if(mode == "replay"){
            feed = make_unique<ReplayFeed>(config, state, on_market, on_trade, log_ev, log_ob_snapshot, params);
        }
        
        else if(exchange == "binance_spot"){
            feed = make_unique<BinanceSpotFeed>(config, state, on_market, on_trade, log_ev, log_ob_snapshot, params);
        }

        else if(exchange == "binance_futures"){
            feed = make_unique<BinanceFuturesFeed>(config, state, on_market, on_trade, log_ev, log_ob_snapshot, params);
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
                // Signal signal;

                // // lock_guard<mutex>lock(signal_mtx);

                // if(signal_queue.empty()){
                //     this_thread::sleep_for(milliseconds(1));
                //     continue;
                // }

                // signal = signal_queue.front();

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

    curl_global_cleanup();

    return 0;
}