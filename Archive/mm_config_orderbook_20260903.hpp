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

// =========================
// MARKET MICROSTRUCTURE UTILS
// =========================
class MarketConfig {
public:
    std_string exchange;
    std_string mode;
    std_string market;

    std_string struct_model;
    std_string regime_model;
    std_string micro_signal_model;
    std_string residual_model;
    std_string toxicity_model;

    std_string instrument;
    std_string instrument_upper;

    int64_t exchange_latency;
    double gamma;
    double base_size;
    double max_inv;

    double initial_cash;
    double maker_fee_rate;
    double taker_fee_rate;

    double tick_size = 0.0;
    double step_size = 0.0;
    size_t price_precision = 0;
    size_t qty_precision = 0;

    std_string folder_path;
    size_t EVENTS_CHUNK;
    size_t SNAPSHOTS_CHUNK;
    size_t TRADES_CHUNK;
    size_t QUOTES_CHUNK;
    size_t FILLS_CHUNK;
    double speed_multiplier;
    std_string host;
    int16_t port;

    std_string api_key;
    std_string api_secret;
    std_string base_url;
    std_string endpoint;
    std_string hostname;

    MarketConfig(const ordered_json& params) {initialize(params);}

    int get_precision(double step){
        int precision = 0;
        while(step < 1.0){
            step *= 10.0;
            precision++;
            if(precision > 18) break; // safety guard
        }
        return precision;
    }

    void initialize(const ordered_json& params){
        exchange = params["exchange"].get<std_string>();
        mode = params["mode"].get<std_string>();
        market = params["market"].get<std_string>();

        struct_model = params["models"]["struct_model"].get<std_string>();
        regime_model = params["models"]["regime_model"].get<std_string>();
        micro_signal_model = params["models"]["micro_signal_model"].get<std_string>();
        residual_model = params["models"]["residual_model"].get<std_string>();
        toxicity_model = params["models"]["toxicity_model"].get<std_string>();

        instrument = params["instrument"].get<std_string>();
        instrument_upper = instrument;
        transform(instrument_upper.begin(), instrument_upper.end(), instrument_upper.begin(), [](unsigned char c){return toupper(c);});

        exchange_latency = params["exchange_latency"].get<int64_t>();
        gamma = params["gamma"].get<double>();
        base_size = params["base_size"].get<double>();
        max_inv = params["max_inv"].get<double>();

        initial_cash = params["initial_cash"].get<double>();
        maker_fee_rate = params["fees"]["maker_fee_rate"].get<double>();
        taker_fee_rate = params["fees"]["taker_fee_rate"].get<double>();

        folder_path = params["folder_path"].get<std_string>();
        EVENTS_CHUNK = params["dataset"]["events_chunk"].get<size_t>();
        SNAPSHOTS_CHUNK = params["dataset"]["snapshots_chunk"].get<size_t>();
        TRADES_CHUNK = params["dataset"]["trades_chunk"].get<size_t>();
        QUOTES_CHUNK = params["dataset"]["quotes_chunk"].get<size_t>();
        FILLS_CHUNK = params["dataset"]["fills_chunk"].get<size_t>();
        speed_multiplier = params["speed_multiplier"].get<double>();
        host = params["server_config"]["host"].get<std_string>();
        port = params["server_config"]["port"].get<int16_t>();

        api_key = (mode == "live" && market == "spot" && instrument == "pepeusdt") ? params["api"]["api_key_live"].get<std_string>() : params["api"]["api_key"].get<std_string>();
        api_secret = (mode == "live" && market == "spot" && instrument == "pepeusdt") ? params["api"]["api_secret_live"].get<std_string>() : params["api"]["api_secret"].get<std_string>();
        hostname = params["api"]["hostname_" + market].get<std_string>();
        base_url = params["api"]["base_url_" + market].get<std_string>();
        endpoint = params["api"]["endpoint_" + market].get<std_string>();

        cout << "exchange: " << exchange + "_" + market << ", mode: " << mode << ", instrument: " << instrument << ", instrument_upper: " << instrument_upper << "\n";
        cout << "exchange_latency: " << exchange_latency << ", gamma: " << gamma << ", base_size: " << base_size << ", max_inv: " << max_inv << "\n";
        cout << "initial_cash: " << initial_cash << ", maker_fee_rate: " << maker_fee_rate << ", taker_fee_rate: " << taker_fee_rate << "\n";
        cout << "folder_path: " << folder_path << ", events_chunk: " << EVENTS_CHUNK << ", snapshots_chunk: " << SNAPSHOTS_CHUNK << "\n";
        cout << "trades_chunk: " << TRADES_CHUNK << ", quotes_chunk: " << QUOTES_CHUNK << ", fills_chunk: " << FILLS_CHUNK << "\n";
        cout << "host: " << host << ", port: " << port << "\n";
        cout << "hostname: " << hostname << ", base_url: " << base_url << ", endpoint: " << endpoint << "\n";

        std_string url = "https://" + base_url + endpoint + "/exchangeInfo?symbol=" + instrument_upper;

        auto r = cpr::Get(cpr::Url{url});
        auto data = json::parse(r.text);
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

    int64_t to_tick(const double& price) const {
        return static_cast<int64_t>(llround(price / tick_size));
    }

    double from_tick(const int64_t& tick) const {
        return tick * tick_size;
    }

    // double normalize_qty(const double& qty) const {
    //     double rounded = floor(qty / step_size) * step_size;

    //     if(rounded < step_size) return 0.0;

    //     return rounded;
    // }

    double normalize_qty(const double& qty) const {
        double rounded = ceil(qty / step_size) * step_size;

        if(rounded < step_size) return 0.0;

        return rounded;
    }

    double normalize_bid(const double& price) const {
        return floor(price / tick_size) * tick_size;
    }

    double normalize_ask(const double& price) const {
        return ceil(price / tick_size) * tick_size;
    }

    std_string format_ms_precise(const int64_t& ts) const{
        time_t t = ts / 1000;
        tm tm = *localtime(&t);

        int ms = ts % 1000;

        ostringstream oss;
        oss << put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << setw(3) << setfill('0') << ms;

        return oss.str();
    }
};

class OrderBook {
public:
    MarketConfig& config;

    int64_t last_update_id = 0;

    std::map<int64_t, double, greater<>> bids;
    std::map<int64_t, double> asks;
    
    OrderBook(MarketConfig& config): config(config) {}

    pair<int64_t, double> best_bid(){
        if(bids.empty()) return {0, 0.0};
        return *bids.begin();
    }

    pair<int64_t, double> best_ask(){
        if(asks.empty()) return {0, 0.0};
        return *asks.begin();
    }

    double mid(){
        if(bids.empty() || asks.empty()) return 0.0;

        auto [bid_tick, bid_size] = best_bid();
        auto [ask_tick, ask_size] = best_ask();

        return config.from_tick((bid_tick + ask_tick) / 2.0);
    }

    pair<int64_t, json> initialize_from_binance(int limit = 1000){

        std_string url = "https://" + config.base_url + config.endpoint + "/depth?symbol=" + config.instrument_upper + "&limit=" + to_string(limit);

        auto r = cpr::Get(cpr::Url{url});
        auto snapshot = json::parse(r.text);

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