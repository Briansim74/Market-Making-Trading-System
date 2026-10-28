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
    unordered_map<std_string, InstrumentConfig> markets;

    int64_t exchange_latency;
    double gamma;
    double initial_position;
    double base_spread;
    double base_size;
    double max_inv;
    double target_notional;
    
    double target_hedge_ratio; // NEW

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


        target_hedge_ratio = params["target_hedge_ratio"].get<double>();



        markets["spot"].instrument = params["spot_instrument"].get<std_string>();
        markets["spot"].instrument_upper = markets["spot"].instrument;
        transform(markets["spot"].instrument_upper.begin(), markets["spot"].instrument_upper.end(), markets["spot"].instrument_upper.begin(), [](unsigned char c){return toupper(c);});
        markets["spot"].base_spread = params["spot_base_spread"].get<double>();
        markets["spot"].base_size = params["spot_base_size"].get<double>();
        markets["spot"].initial_cash = params["spot_initial_cash"].get<double>();
        markets["spot"].maker_fee_rate = params["fees"]["spot_maker_fee_rate"].get<double>();
        markets["spot"].taker_fee_rate = params["fees"]["spot_taker_fee_rate"].get<double>();
        markets["spot"].hostname = params["api"]["spot"]["hostname"].get<std_string>();
        markets["spot"].base_url = params["api"]["spot"]["base_url"].get<std_string>();
        markets["spot"].endpoint = params["api"]["spot"]["endpoint"].get<std_string>();

        markets["perps"].instrument = params["perps_instrument"].get<std_string>();
        markets["perps"].instrument_upper = markets["perps"].instrument;
        transform(markets["perps"].instrument_upper.begin(), markets["perps"].instrument_upper.end(), markets["perps"].instrument_upper.begin(), [](unsigned char c){return toupper(c);});
        markets["perps"].base_spread = params["perps_base_spread"].get<double>();
        markets["perps"].base_size = params["perps_base_size"].get<double>();
        markets["perps"].initial_cash = params["perps_initial_cash"].get<double>();
        markets["perps"].maker_fee_rate = params["fees"]["perps_maker_fee_rate"].get<double>();
        markets["perps"].taker_fee_rate = params["fees"]["perps_taker_fee_rate"].get<double>();
        markets["perps"].hostname = params["api"]["perps"]["hostname"].get<std_string>();
        markets["perps"].base_url = params["api"]["perps"]["base_url"].get<std_string>();
        markets["perps"].endpoint = params["api"]["perps"]["endpoint"].get<std_string>();

        markets["testnet_perps"].instrument = params["testnet_perps_instrument"].get<std_string>();
        markets["testnet_perps"].instrument_upper = markets["testnet_perps"].instrument;
        transform(markets["testnet_perps"].instrument_upper.begin(), markets["testnet_perps"].instrument_upper.end(), markets["testnet_perps"].instrument_upper.begin(), [](unsigned char c){return toupper(c);});
        markets["testnet_perps"].base_spread = params["testnet_perps_base_spread"].get<double>();
        markets["testnet_perps"].base_size = params["testnet_perps_base_size"].get<double>();
        markets["testnet_perps"].initial_cash = params["testnet_perps_initial_cash"].get<double>();
        markets["testnet_perps"].maker_fee_rate = params["fees"]["testnet_perps_maker_fee_rate"].get<double>();
        markets["testnet_perps"].taker_fee_rate = params["fees"]["testnet_perps_taker_fee_rate"].get<double>();
        markets["testnet_perps"].hostname = params["api"]["testnet_perps"]["hostname"].get<std_string>();
        markets["testnet_perps"].base_url = params["api"]["testnet_perps"]["base_url"].get<std_string>();
        markets["testnet_perps"].endpoint = params["api"]["testnet_perps"]["endpoint"].get<std_string>();

        exchange_latency = params["exchange_latency"].get<int64_t>();
        gamma = params["gamma"].get<double>();
        initial_position = params["initial_position"].get<double>();
        max_inv = params["max_inv"].get<double>();
        target_notional = params["target_notional"].get<double>();

        folder_path = params["folder_path"].get<std_string>();
        EVENTS_CHUNK = params["dataset"]["events_chunk"].get<size_t>();
        SNAPSHOTS_CHUNK = params["dataset"]["snapshots_chunk"].get<size_t>();
        TRADES_CHUNK = params["dataset"]["trades_chunk"].get<size_t>();
        QUOTES_CHUNK = params["dataset"]["quotes_chunk"].get<size_t>();
        FILLS_CHUNK = params["dataset"]["fills_chunk"].get<size_t>();
        speed_multiplier = params["speed_multiplier"].get<double>();
        host = params["server_config"]["host"].get<std_string>();
        port = params["server_config"]["port"].get<int16_t>();

        api_key = (mode == "live") ? params["api"]["api_key_live"].get<std_string>() : params["api"]["api_key"].get<std_string>();
        api_secret = (mode == "live") ? params["api"]["api_secret_live"].get<std_string>() : params["api"]["api_secret"].get<std_string>();
        
        for(std_string market: vector<std_string>{"spot", "perps", "testnet_perps"}){
            std_string url = "https://" + markets[market].base_url + "/" + markets[market].endpoint + "/exchangeInfo?symbol=" + markets[market].instrument_upper;

            auto r = cpr::Get(cpr::Url{url});
            auto data = json::parse(r.text);
            json filters;
            
            if(market != "spot"){
                for(auto& symbol_entry: data["symbols"]){
                    if(symbol_entry["symbol"] == markets[market].instrument_upper){
                        filters = symbol_entry["filters"];
                        break;
                    }
                }
            }
            else filters = data["symbols"][0]["filters"];

            for(auto& f: filters){
                if(f["filterType"] == "PRICE_FILTER"){
                    markets[market].tick_size = stod(f["tickSize"].get<std_string>());
                    markets[market].price_precision = get_precision(markets[market].tick_size);
                }

                if(f["filterType"] == "LOT_SIZE"){
                    markets[market].step_size = stod(f["stepSize"].get<std_string>());
                    markets[market].qty_precision   = get_precision(markets[market].step_size);

                    markets[market].min_qty = stod(f["minQty"].get<std_string>());
                    markets[market].max_qty = stod(f["maxQty"].get<std_string>());
                }

                if(f["filterType"] == "NOTIONAL") { // spot
                    markets[market].min_notional = stod(f["minNotional"].get<std_string>());
                    markets[market].apply_min_market = f["applyMinToMarket"].get<bool>();
                }

                if(f["filterType"] == "MIN_NOTIONAL") { // perps
                    markets[market].min_notional = stod(f["notional"].get<std_string>());
                }
            }

            cout << "\n===== MARKET CONFIG =====\n";
            cout << "exchange:          " << exchange + "_" + market << "\n";
            cout << "mode:              " << mode << "\n";
            cout << "market:            " << market << "\n";
            cout << "instrument:        " << markets[market].instrument << "\n";
            cout << "instrument_upper:  " << markets[market].instrument_upper << "\n";

            cout << "\n===== MODEL CONFIG =====\n";
            cout << "struct_model:       " << struct_model << "\n";
            cout << "regime_model:       " << regime_model << "\n";
            cout << "micro_signal_model: " << micro_signal_model << "\n";
            cout << "residual_model:     " << residual_model << "\n";
            cout << "toxicity_model:     " << toxicity_model << "\n";

            cout << "\n===== STRATEGY CONFIG =====\n";
            cout << "exchange_latency:   " << exchange_latency << "\n";
            cout << "gamma:              " << gamma << "\n";
            cout << "initial_position:   " << initial_position << "\n";
            cout << "base_spread:        " << markets[market].base_spread << "\n";
            cout << "base_size:          " << markets[market].base_size << "\n";
            cout << "max_inv:            " << max_inv << "\n";
            cout << "target_notional:    " << target_notional << "\n";
            cout << "initial_cash:       " << markets[market].initial_cash << "\n";
            cout << "maker_fee_rate:     " << markets[market].maker_fee_rate << "\n";
            cout << "taker_fee_rate:     " << markets[market].taker_fee_rate << "\n";

            cout << "\n===== EXECUTION CONFIG =====\n";
            cout << "tick_size:          " << markets[market].tick_size << "\n";
            cout << "price_precision:    " << markets[market].price_precision << "\n";
            cout << "step_size:          " << markets[market].step_size << "\n";
            cout << "qty_precision:      " << markets[market].qty_precision << "\n";
            cout << "min_qty:            " << markets[market].min_qty << "\n";
            cout << "max_qty:            " << markets[market].max_qty << "\n";
            cout << "min_notional_USDT:  " << markets[market].min_notional << "\n";
            cout << "apply_min_market:   " << boolalpha << markets[market].apply_min_market << "\n";

            cout << "\n===== STORAGE CONFIG =====\n";
            cout << "folder_path:        " << folder_path << "\n";
            cout << "events_chunk:       " << EVENTS_CHUNK << "\n";
            cout << "snapshots_chunk:    " << SNAPSHOTS_CHUNK << "\n";
            cout << "trades_chunk:       " << TRADES_CHUNK << "\n";
            cout << "quotes_chunk:       " << QUOTES_CHUNK << "\n";
            cout << "fills_chunk:        " << FILLS_CHUNK << "\n";

            cout << "\n===== HTTP CONFIG =====\n";
            cout << "host:               " << host << "\n";
            cout << "port:               " << port << "\n";
            cout << "hostname:           " << markets[market].hostname << "\n";
            cout << "base_url:           " << markets[market].base_url << "\n";
            cout << "endpoint:           " << markets[market].endpoint << "\n";

            cout << "================================\n\n";
            }
    }

    int64_t to_tick(const double& price, const std_string& market) const {
        return static_cast<int64_t>(llround(price / markets.at(market).tick_size));
    }

    double from_tick(const int64_t& tick, const std_string& market) const {
        return tick * markets.at(market).tick_size;
    }

    double normalize_qty(const double& qty, const std_string& market) const {
        double rounded = ceil(qty / markets.at(market).step_size) * markets.at(market).step_size;

        if(rounded < markets.at(market).step_size) return 0.0;

        return rounded;
    }

    double normalize_bid(const double& price, const std_string& market) const {
        return floor(price / markets.at(market).tick_size) * markets.at(market).tick_size;
    }

    double normalize_ask(const double& price, const std_string& market) const {
        return ceil(price / markets.at(market).tick_size) * markets.at(market).tick_size;
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
    std_string market;

    int64_t last_update_id = 0;

    std::map<int64_t, double, greater<>> bids;
    std::map<int64_t, double> asks;
    
    OrderBook(MarketConfig& config, std_string market): config(config), market(market) {}

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

        return config.from_tick((bid_tick + ask_tick) / 2.0, market);
    }

    pair<int64_t, json> initialize_from_binance(int limit = 1000){

        std_string url = "https://" + config.markets[market].base_url + "/" + config.markets[market].endpoint + "/depth?symbol=" + config.markets[market].instrument_upper + "&limit=" + to_string(limit);
        cout << "url: " << url << "\n";

        auto r = cpr::Get(cpr::Url{url});
        auto snapshot = json::parse(r.text);

        last_update_id = snapshot["lastUpdateId"].get<int64_t>();

        for(auto& entry: snapshot["bids"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            bids[config.to_tick(p, market)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            asks[config.to_tick(p, market)] = q;
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

            bids[config.to_tick(p, market)] = q;
        }

        for(auto& entry: snapshot["asks"]){
            double p = stod(entry[0].get_ref<const std_string&>());
            double q = stod(entry[1].get_ref<const std_string&>());

            asks[config.to_tick(p, market)] = q;
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

    void replace_book(const Depth& entry){

        bids.clear();
        asks.clear();

        for(auto& [price_tick, q]: entry.bid_delta){
            if(q > 0) bids[price_tick] = q;
        }

        for(auto& [price_tick, q]: entry.ask_delta){
            if(q > 0) asks[price_tick] = q;
        }

        last_update_id = 0;
    }
};