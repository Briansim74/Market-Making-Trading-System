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

// =========================
// MARKET MICROSTRUCTURE UTILS
// =========================
class MarketConfig {
public:
    double tick_size = 0.0;
    double step_size = 0.0;
    size_t price_precision = 0;
    size_t qty_precision = 0;

    std_string base_url;
    std_string endpoint;

    MarketConfig(const json& params){
        load_filters(params);
    }

    int get_precision(double step){
        int precision = 0;
        while(step < 1.0){
            step *= 10.0;
            precision++;
            if(precision > 18) break; // safety guard
        }
        return precision;
    }

    void load_filters(const json& params){
        if(params["exchange_new"].get<std_string>() == "binance" && params["market"].get<std_string>() == "spot"){
            tick_size = params["tick_size"].get<double>();
            step_size = params["step_size"].get<double>();
            price_precision = get_precision(tick_size);
            qty_precision   = get_precision(step_size);
            cout << "Exchange: " << params["exchange_new"].get<std_string>() + "_" + params["market"].get<std_string>() << "\n";
            cout << "tick_size: " << tick_size << ", price_precision: " << price_precision << "\n";
            cout << "step_size: " << step_size << ", qty_precision: " << qty_precision << "\n";
            return;
        }

        std_string instrument = params["instrument"].get<std_string>();
        transform(instrument.begin(), instrument.end(), instrument.begin(), [](unsigned char c){return toupper(c);});

        std_string url = "https://" + params["api"]["base_url"].get<std_string>() + "/fapi/v1/exchangeInfo?symbol=" + instrument;
        auto r = cpr::Get(cpr::Url{url});
        auto data = json::parse(r.text);

        cout << "Exchange: " << params["exchange_new"].get<std_string>() + "_" + params["market"].get<std_string>() << "\n";
        auto filters = data["symbols"][0]["filters"];
        
        for(auto& f: filters){
            if(f["filterType"] == "PRICE_FILTER"){
                tick_size = stod(f["tickSize"].get<std_string>());
                price_precision = get_precision(tick_size);
                cout << "tick_size: " << tick_size << ", price_precision: " << price_precision << "\n";
            }

            if(f["filterType"] == "LOT_SIZE"){
                step_size = stod(f["stepSize"].get<std_string>());
                qty_precision   = get_precision(step_size);
                cout << "step_size: " << step_size << ", qty_precision: " << qty_precision << "\n";
            }
        }
    }

    int64_t to_tick(double price) const{
        return static_cast<int64_t>(llround(price / tick_size));
    }

    double from_tick(int64_t tick) const{
        return tick * tick_size;
    }

    double round_price(double price) const{
        return llround(price / tick_size) * tick_size;
    }

    double normalize_qty(double qty) const {
        double rounded = floor(qty / step_size) * step_size;

        if(rounded < step_size) return 0.0;

        return rounded;
    }

    double normalize_bid(double price) const {
        return floor(price / tick_size) * tick_size;
    }

    double normalize_ask(double price) const {
        return ceil(price / tick_size) * tick_size;
    }

    uint64_t now_ms() const{
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    int64_t now_ms_signed() const{
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    uint64_t to_ms(double ts) const{
        return static_cast<uint64_t>(ts * 1000.0);
    }

    std_string format_ms(int64_t ts_ms) const{
        time_t t = ts_ms / 1000;
        tm tm = *localtime(&t);

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S");

        return oss.str();
    }

    std_string format_ms_precise(int64_t ts_ms) const{
        time_t t = ts_ms / 1000;
        tm tm = *localtime(&t);

        int ms = ts_ms % 1000;

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << setw(3) << setfill('0') << ms;

        return oss.str();
    }
};

class OrderBook {
public:
    MarketConfig& config; // pointer to avoid copies
    const json& params;
    std::map<int64_t, double, greater<>> bids;
    std::map<int64_t, double> asks;
    int64_t last_update_id = 0;
    recursive_mutex mtx; //recursive mtx
    
    OrderBook(MarketConfig& config, const json& params): config(config), params(params) {}

    pair<int64_t, double> best_bid(){
        if(bids.empty()) return {0, 0.0};
        return *bids.begin();
    }

    pair<int64_t, double> best_ask(){
        if(asks.empty()) return {0, 0.0};
        return *asks.begin();
    }

    double mid(){
        lock_guard<recursive_mutex> lock(mtx);

        if(bids.empty() || asks.empty())
            return 0.0;

        auto [bid_tick, bid_size] = best_bid();
        auto [ask_tick, ask_size] = best_ask();

        return config.from_tick((bid_tick + ask_tick) / 2);
    }

    pair<int64_t, json> initialize_from_binance(const std_string& symbol, int limit){
        
        std_string url;
        if(params["exchange"] == "binance_spot"){
            url = "https://api.binance.com/api/v3/depth?symbol=" + symbol + "&limit=" + to_string(limit);
        }
        else if(params["exchange"] == "binance_futures"){
            url = "https://testnet.binancefuture.com/fapi/v1/depth?symbol=" + symbol + "&limit=" + to_string(limit);
        }

        // {
        //     url = "https://" + params["api"]["base_url"].get<std_string>() + "/fapi/v1/exchangeInfo?symbol=" + instrument;

        // }

        auto r = cpr::Get(cpr::Url{url});
        auto snapshot = json::parse(r.text);

        last_update_id = snapshot["lastUpdateId"].get<int64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());
            
            //cout << "p: " << p << " q: " << q << "\n";

            bids[config.to_tick(p)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            //cout << "p: " << p << " q: " << q << "\n";

            asks[config.to_tick(p)] = q;
        }

        cout << "SNAPSHOT FETCHED: " << last_update_id << "\n";
        cout << "ORDER BOOK INITIALIZED\n";

        return {last_update_id, snapshot};
    }

    pair<int64_t, json> initialize_from_orderbook_snapshot(const json& snapshot){

        last_update_id = snapshot["lastUpdateId"].get<int64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            bids[config.to_tick(p)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            asks[config.to_tick(p)] = q;
        }

        cout << "SNAPSHOT FETCHED: " << last_update_id << "\n";
        cout << "ORDER BOOK INITIALIZED\n";

        return {last_update_id, snapshot};
    }

    void apply_delta(const Depth& entry){
        lock_guard<recursive_mutex> lock(mtx);

        for(auto& [price_tick, q]: entry.bid_delta){
            if(q == 0.0) bids.erase(price_tick);
            else bids[price_tick] = q;
        }

        for(auto& [price_tick, q]: entry.ask_delta){
            if(q == 0.0) asks.erase(price_tick);
            else asks[price_tick] = q;
        }
    }
};