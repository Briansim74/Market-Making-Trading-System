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
#include "mm_state.hpp" //state & market_feature_state

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

class DatasetRecorder {
public:
    MarketConfig& config;
    State& state;
    const ordered_json& params;

    vector<EventRow> events;
    vector<SnapshotRow> snapshots;
    vector<TradeRow> trades;
    vector<QuoteRow> quotes;
    vector<FillRow> fills;

    std_string run_id;
    std_string folder_path;
    std_string orderbook_snapshot_path;
    std_string events_path;
    std_string snapshots_path;
    std_string trades_path;
    std_string quotes_path;
    std_string fills_path;

    int events_id = 0;
    int snapshots_id = 0;
    int trades_id = 0;
    int quotes_id = 0;
    int fills_id = 0;

    DatasetRecorder(MarketConfig& config, State& state, const ordered_json& params)
        : config(config), state(state), params(params) {initialize();}

    std_string build_file_path(const std_string& file, const int& file_id){
        ostringstream ss;
        ss << folder_path << '/' << file << '_' << setw(6) << setfill('0') << file_id << ".parquet";
        
        cout << file << "_file_path: " << ss.str() << "\n";
        return ss.str();
    }

    void initialize(){
        auto now = system_clock::now();
        time_t now_t = system_clock::to_time_t(now);

        tm tm{};
        gmtime_s(&tm, &now_t);

        ostringstream ss;
        ss << put_time(&tm, "%Y%m%d_%H%M%S");

        run_id = ss.str();
        folder_path = "data/runs/run_" + run_id;
        filesystem::create_directories(folder_path);

        orderbook_snapshot_path = folder_path + "/orderbook_snapshot.json";
        events_path = build_file_path("events", events_id++);
        snapshots_path = build_file_path("snapshots", snapshots_id++);
        trades_path = build_file_path("trades", trades_id++);
        quotes_path = build_file_path("quotes", quotes_id++);
        fills_path = build_file_path("fills", fills_id++);
    }

    void export_orderbook_snapshot(const json& snapshot){
        ofstream(orderbook_snapshot_path) << snapshot.dump(4);
    }

    void log_event(const std_string& type, const int64_t& ts, const int64_t& local_ts, const int64_t& latency, const std_string& msg){

        EventRow row;
        row.type = type;
        row.ts = ts;
        row.local_ts = local_ts;
        row.latency = latency;
        row.msg = msg;

        events.push_back(move(row));

        if(events.size() >= config.EVENTS_CHUNK){
            events_path = build_file_path("events", events_id++);
            export_event_parquet(events);
            events.clear();
        }
    }

    void export_event_parquet(const vector<EventRow>& events){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        StringBuilder type_b(pool);
        Int64Builder ts_b(pool);
        Int64Builder local_ts_b(pool);
        Int64Builder latency_b(pool);
        StringBuilder msg_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& e: events){
            type_b.Append(e.type);
            ts_b.Append(e.ts);
            local_ts_b.Append(e.local_ts);
            latency_b.Append(e.latency);
            msg_b.Append(e.msg);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> type_arr, ts_arr, local_ts_arr, latency_arr, msg_arr;

        type_b.Finish(&type_arr);
        ts_b.Finish(&ts_arr);
        local_ts_b.Finish(&local_ts_arr);
        latency_b.Finish(&latency_arr);
        msg_b.Finish(&msg_arr);
        
        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("type", utf8()),
            field("ts", int64()),
            field("local_ts", int64()),
            field("latency", int64()),
            field("msg", utf8())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {type_arr, ts_arr, local_ts_arr, latency_arr, msg_arr});

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(events_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void stop(){
        state.update_performance();

        // flush events
        if(!events.empty()){
            events_path = build_file_path("events", events_id++);
            export_event_parquet(events);
        }

        // flush snapshots
        if(!snapshots.empty()){
            snapshots_path = build_file_path("snapshots", snapshots_id++);
            export_snapshot_parquet(snapshots);
        }

        // flush trades
        if(!trades.empty()){
            trades_path = build_file_path("trades", trades_id++);
            export_trade_parquet(trades);
        }

        // flush quotes
        if(!quotes.empty()){
            quotes_path = build_file_path("quotes", quotes_id++);
            export_quote_parquet(quotes);
        }

        // flush fills
        if(!fills.empty()){
            fills_path = build_file_path("fills", fills_id++);
            export_fill_parquet(fills);
        }

        // manifest
        ordered_json manifest = params;
        std_string manifest_path = folder_path + "/manifest.json";
        PerformanceMetrics performance = state.compute_performance();

        manifest["run_id"] = run_id;
        manifest["folder_path"] = folder_path;
        manifest["performance"] = {
            // {"pnl", round(state.get_pnl(state.market_book.mid()) * 10000.0) / 10000.0},
            // {"sharpe", round(performance.sharpe * 10000.0) / 10000.0},
            // {"annualized_sharpe", round(performance.annualized_sharpe * 10000.0) / 10000.0},
            // {"sortino", round(performance.sortino * 10000.0) / 10000.0},
            // {"annualized_sortino", round(performance.annualized_sortino * 10000.0) / 10000.0},
            // {"fees_paid", round(state.fees_paid * 10000.0) / 10000.0},
            // {"notional_traded", round(state.notional_traded) / 10000.0},
            {"pnl", state.get_pnl(state.market_book.mid())},
            {"sharpe", performance.sharpe},
            {"annualized_sharpe", performance.annualized_sharpe},
            {"sortino", performance.sortino},
            {"annualized_sortino", performance.annualized_sortino},
            {"fees_paid", state.fees_paid},
            {"notional_traded", state.notional_traded},
            {"fees_per_fill", state.fees_paid / (fills.size() + 1e-9)},
            {"pnl_per_fill", state.get_pnl(state.market_book.mid()) / (fills.size() + 1e-9)}
        };

        ofstream(manifest_path) << manifest.dump(2);
        cout << "DATASETS SAVED SUCCESSFULLY\n";
        cout << "Saved run → " << folder_path << "\n";
    }

    void log_snapshot(const Signal& signal){
        SnapshotRow row;
  
        row.ts = signal.ts;
        row.trade_latency = signal.trade_latency;
        row.depth_latency = signal.depth_latency;
        row.exchange_latency = signal.exchange_latency;
        row.symbol = config.instrument_upper;

        row.mid = signal.mid;
        row.mid_tick = config.to_tick(signal.mid);
        row.microprice = signal.microprice;
        row.microprice_dev = signal.microprice_dev;
        row.microprice_error = signal.microprice_error;
        row.spread = signal.spread;

        row.best_bid = signal.best_bid;
        row.best_ask = signal.best_ask;
        row.best_bid_tick = config.to_tick(signal.best_bid);
        row.best_ask_tick = config.to_tick(signal.best_ask);

        row.order_imbalance = signal.order_imbalance;
        row.trade_imbalance = signal.trade_imbalance;
        row.volatility = signal.volatility;
        row.queue_ahead_bid = signal.queue_ahead_bid;
        row.queue_ahead_ask = signal.queue_ahead_ask;

        row.inventory = signal.inventory;
        row.realized_pnl = signal.realized_pnl;
        row.unrealized_pnl = signal.unrealized_pnl;
        row.total_pnl = signal.total_pnl;
        row.fees_paid = signal.fees_paid;
        row.equity = signal.equity;

        row.fair = signal.fair;
        row.skew = signal.skew;
        row.struct_delta = signal.struct_delta;
        row.micro_signal_delta = signal.micro_signal_delta;
        row.residual_delta = signal.residual_delta;
        row.reservation = signal.reservation;

        row.regime = signal.regime;
        row.regime_id = signal.regime_id;
        row.regime_prob = signal.regime_prob;
        row.alpha_trade_imb = signal.alpha_trade_imb;
        row.alpha_struct = signal.alpha_struct;
        row.alpha_residual = signal.alpha_residual;
        row.spread_multiplier = signal.spread_multiplier;
        row.inventory_target = signal.inventory_target;
        row.residual_signal_quality = signal.residual_signal_quality;
        row.tox = signal.toxicity.tox;
        row.k_spread = signal.toxicity.k_spread;
        row.k_order_size = signal.toxicity.k_order_size;

        row.my_bid = signal.my_bid;
        row.my_ask = signal.my_ask;
        row.my_bid_tick = config.to_tick(signal.my_bid);
        row.my_ask_tick = config.to_tick(signal.my_ask);

        row.bid_distance_touch = signal.my_bid - signal.best_bid;
        row.ask_distance_touch = signal.my_ask - signal.best_ask;
        row.bid_distance_spread = signal.my_bid - signal.best_ask;
        row.ask_distance_spread = signal.my_ask - signal.best_bid;

        row.bid_delta = signal.bid_delta;
        row.ask_delta = signal.ask_delta;
        row.quote_churn = signal.quote_churn;
    
        snapshots.push_back(move(row));

        if(snapshots.size() >= config.SNAPSHOTS_CHUNK){
            snapshots_path = build_file_path("snapshots", snapshots_id++);
            export_snapshot_parquet(snapshots);
            snapshots.clear();
        }
    }

    void export_snapshot_parquet(const vector<SnapshotRow>& snapshots){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder trade_latency_b(pool);
        Int64Builder depth_latency_b(pool);
        Int64Builder exchange_latency_b(pool);
        StringBuilder symbol_b(pool);

        DoubleBuilder mid_b(pool);
        Int64Builder mid_tick_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);

        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);

        DoubleBuilder inventory_b(pool);
        DoubleBuilder realized_pnl_b(pool);
        DoubleBuilder unrealized_pnl_b(pool);
        DoubleBuilder total_pnl_b(pool);
        DoubleBuilder fees_paid_b(pool);
        DoubleBuilder equity_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);
        
        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder alpha_residual_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k_spread_b(pool);
        DoubleBuilder k_order_size_b(pool);

        DoubleBuilder my_bid_b(pool);
        DoubleBuilder my_ask_b(pool);
        Int64Builder my_bid_tick_b(pool);
        Int64Builder my_ask_tick_b(pool);

        DoubleBuilder bid_distance_touch_b(pool);
        DoubleBuilder ask_distance_touch_b(pool);
        DoubleBuilder bid_distance_spread_b(pool);
        DoubleBuilder ask_distance_spread_b(pool);

        DoubleBuilder bid_delta_b(pool);
        DoubleBuilder ask_delta_b(pool);
        DoubleBuilder quote_churn_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: snapshots){
            ts_b.Append(r.ts);
            trade_latency_b.Append(r.trade_latency);
            depth_latency_b.Append(r.depth_latency);
            exchange_latency_b.Append(r.exchange_latency);
            symbol_b.Append(r.symbol);

            mid_b.Append(r.mid);
            mid_tick_b.Append(r.mid_tick);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);

            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);

            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);

            inventory_b.Append(r.inventory);
            realized_pnl_b.Append(r.realized_pnl);
            unrealized_pnl_b.Append(r.unrealized_pnl);
            total_pnl_b.Append(r.total_pnl);
            fees_paid_b.Append(r.fees_paid);
            equity_b.Append(r.equity);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            alpha_residual_b.Append(r.alpha_residual);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k_spread_b.Append(r.k_spread);
            k_order_size_b.Append(r.k_order_size);

            my_bid_b.Append(r.my_bid);
            my_ask_b.Append(r.my_ask);
            my_bid_tick_b.Append(r.my_bid_tick);
            my_ask_tick_b.Append(r.my_ask_tick);

            bid_distance_touch_b.Append(r.bid_distance_touch);
            ask_distance_touch_b.Append(r.ask_distance_touch);
            bid_distance_spread_b.Append(r.bid_distance_spread);
            ask_distance_spread_b.Append(r.ask_distance_spread);

            bid_delta_b.Append(r.bid_delta);
            ask_delta_b.Append(r.ask_delta);
            quote_churn_b.Append(r.quote_churn);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, trade_latency_arr, depth_latency_arr, exchange_latency_arr, symbol_arr, mid_arr, mid_tick_arr;
        shared_ptr<Array> microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;
        shared_ptr<Array> inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, fees_paid_arr, equity_arr;
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr;
        shared_ptr<Array> spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr;
        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_distance_touch_arr, ask_distance_touch_arr;
        shared_ptr<Array> bid_distance_spread_arr, ask_distance_spread_arr;
        shared_ptr<Array> bid_delta_arr, ask_delta_arr, quote_churn_arr;

        ts_b.Finish(&ts_arr);
        trade_latency_b.Finish(&trade_latency_arr);
        depth_latency_b.Finish(&depth_latency_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        symbol_b.Finish(&symbol_arr);

        mid_b.Finish(&mid_arr);
        mid_tick_b.Finish(&mid_tick_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);

        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);

        inventory_b.Finish(&inventory_arr);
        realized_pnl_b.Finish(&realized_pnl_arr);
        unrealized_pnl_b.Finish(&unrealized_pnl_arr);
        total_pnl_b.Finish(&total_pnl_arr);
        fees_paid_b.Finish(&fees_paid_arr);
        equity_b.Finish(&equity_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        alpha_residual_b.Finish(&alpha_residual_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k_spread_b.Finish(&k_spread_arr);
        k_order_size_b.Finish(&k_order_size_arr);

        my_bid_b.Finish(&my_bid_arr);
        my_ask_b.Finish(&my_ask_arr);
        my_bid_tick_b.Finish(&my_bid_tick_arr);
        my_ask_tick_b.Finish(&my_ask_tick_arr);

        bid_distance_touch_b.Finish(&bid_distance_touch_arr);
        ask_distance_touch_b.Finish(&ask_distance_touch_arr);
        bid_distance_spread_b.Finish(&bid_distance_spread_arr);
        ask_distance_spread_b.Finish(&ask_distance_spread_arr);

        bid_delta_b.Finish(&bid_delta_arr);
        ask_delta_b.Finish(&ask_delta_arr);
        quote_churn_b.Finish(&quote_churn_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("trade_latency", int64()),
            field("depth_latency", int64()),
            field("exchange_latency", int64()),
            field("symbol", utf8()),

            field("mid", float64()),
            field("mid_tick", int64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),

            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),
            
            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),

            field("inventory", float64()),
            field("realized_pnl", float64()),
            field("unrealized_pnl", float64()),
            field("total_pnl", float64()),
            field("fees_paid", float64()),
            field("equity", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("alpha_residual", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k_spread", float64()),
            field("k_order_size", float64()),

            field("my_bid", float64()),
            field("my_ask", float64()),
            field("my_bid_tick", int64()),
            field("my_ask_tick", int64()),

            field("bid_distance_touch", float64()),
            field("ask_distance_touch", float64()),
            field("bid_distance_spread", float64()),
            field("ask_distance_spread", float64()),

            field("bid_delta", float64()),
            field("ask_delta", float64()),
            field("quote_churn", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, trade_latency_arr, depth_latency_arr, exchange_latency_arr, symbol_arr, mid_arr, mid_tick_arr,
            microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, fees_paid_arr, equity_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr,
            spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr,
            my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr,
            bid_distance_touch_arr, ask_distance_touch_arr,
            bid_distance_spread_arr, ask_distance_spread_arr,
            bid_delta_arr, ask_delta_arr, quote_churn_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(snapshots_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_trade(const Trade& trade){
        TradeRow row;

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9);

        double notional = trade.price * trade.qty;

        row.ts = trade.ts;
        row.symbol = config.instrument_upper;

        row.price = trade.price;
        row.price_tick = config.to_tick(trade.price);
        row.qty = trade.qty;
        row.side = trade.side;
        row.is_buyer_maker = (trade.side == "SELL") ? true : false;

        row.mid = mid;
        row.microprice = microprice;
        row.microprice_dev = microprice - mid;
        row.microprice_error = mid - microprice;
        row.spread = best_ask - best_bid;
        row.best_bid = best_bid;
        row.best_ask = best_ask;
        row.best_bid_tick = bid_tick;
        row.best_ask_tick = ask_tick;
        
        row.bid_size = bid_size;
        row.ask_size = ask_size;

        row.trade_to_mid = trade.price - mid;
        row.trade_to_microprice = trade.price - microprice;
        row.price_to_best_bid = trade.price - best_bid;
        row.price_to_best_ask = trade.price - best_ask;

        row.trade_side = trade.side == "SELL" ? "SELL_AGGRESSOR" : "BUY_AGGRESSOR";
        row.trade_sign = trade.side == "SELL" ? -1 : 1;

        row.notional = notional;
        row.log_notional = log1p(notional);
        row.intensity = trade.qty / (bid_size + ask_size + 1e-9);

        trades.push_back(move(row));

        if(trades.size() >= config.TRADES_CHUNK){
            trades_path = build_file_path("trades", trades_id++);
            export_trade_parquet(trades);
            trades.clear();
        }
    }

    void export_trade_parquet(const vector<TradeRow>& trades){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        StringBuilder symbol_b(pool);

        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);
        StringBuilder side_b(pool);
        BooleanBuilder is_buyer_maker_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder bid_size_b(pool);
        DoubleBuilder ask_size_b(pool);

        DoubleBuilder trade_to_mid_b(pool);
        DoubleBuilder trade_to_microprice_b(pool);
        DoubleBuilder price_to_best_bid_b(pool);
        DoubleBuilder price_to_best_ask_b(pool);

        StringBuilder trade_side_b(pool);
        Int32Builder trade_sign_b(pool);

        DoubleBuilder notional_b(pool);
        DoubleBuilder log_notional_b(pool);
        DoubleBuilder intensity_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: trades){
            ts_b.Append(r.ts);
            symbol_b.Append(r.symbol);

            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);
            side_b.Append(r.side);
            is_buyer_maker_b.Append(r.is_buyer_maker);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);

            bid_size_b.Append(r.bid_size);
            ask_size_b.Append(r.ask_size);

            trade_to_mid_b.Append(r.trade_to_mid);
            trade_to_microprice_b.Append(r.trade_to_microprice);
            price_to_best_bid_b.Append(r.price_to_best_bid);
            price_to_best_ask_b.Append(r.price_to_best_ask);

            trade_side_b.Append(r.trade_side);
            trade_sign_b.Append(r.trade_sign);

            notional_b.Append(r.notional);
            log_notional_b.Append(r.log_notional);
            intensity_b.Append(r.intensity);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, symbol_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;
        shared_ptr<Array> side_arr, is_buyer_maker_arr;

        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        shared_ptr<Array> bid_size_arr, ask_size_arr;

        shared_ptr<Array> trade_to_mid_arr, trade_to_microprice_arr;
        shared_ptr<Array> price_to_best_bid_arr, price_to_best_ask_arr;

        shared_ptr<Array> trade_side_arr, trade_sign_arr;
        shared_ptr<Array> notional_arr, log_notional_arr, intensity_arr;

        ts_b.Finish(&ts_arr);
        symbol_b.Finish(&symbol_arr);

        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);
        side_b.Finish(&side_arr);
        is_buyer_maker_b.Finish(&is_buyer_maker_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        bid_size_b.Finish(&bid_size_arr);
        ask_size_b.Finish(&ask_size_arr);

        trade_to_mid_b.Finish(&trade_to_mid_arr);
        trade_to_microprice_b.Finish(&trade_to_microprice_arr);
        price_to_best_bid_b.Finish(&price_to_best_bid_arr);
        price_to_best_ask_b.Finish(&price_to_best_ask_arr);

        trade_side_b.Finish(&trade_side_arr);
        trade_sign_b.Finish(&trade_sign_arr);

        notional_b.Finish(&notional_arr);
        log_notional_b.Finish(&log_notional_arr);
        intensity_b.Finish(&intensity_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("symbol", utf8()),

            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
            field("side", utf8()),
            field("is_buyer_maker", boolean()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("bid_size", float64()),
            field("ask_size", float64()),

            field("trade_to_mid", float64()),
            field("trade_to_microprice", float64()),
            field("price_to_best_bid", float64()),
            field("price_to_best_ask", float64()),

            field("trade_side", utf8()),
            field("trade_sign", int32()),

            field("notional", float64()),
            field("log_notional", float64()),
            field("intensity", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, symbol_arr,
            price_arr, price_tick_arr, qty_arr,
            side_arr, is_buyer_maker_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            bid_size_arr, ask_size_arr,
            trade_to_mid_arr, trade_to_microprice_arr,
            price_to_best_bid_arr, price_to_best_ask_arr,
            trade_side_arr, trade_sign_arr,
            notional_arr, log_notional_arr, intensity_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(trades_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_quote(const Order& order, const std_string& side, const std_string& event_type){
        QuoteRow row;

        double price = config.from_tick(order.price_tick);

        row.ts = order.ts;
        row.exchange_latency = order.exchange_latency;
        row.symbol = config.instrument_upper;
        row.client_oid = order.client_oid;
        row.side = side;
        row.event_type = event_type;

        row.price = price;
        row.price_tick = order.price_tick;
        row.qty = order.qty;

        row.mid = order.signal.mid;
        row.microprice = order.signal.microprice;
        row.microprice_dev = order.signal.microprice_dev;
        row.microprice_error = order.signal.microprice_error;
        row.spread = order.signal.spread;
        row.best_bid = order.signal.best_bid;
        row.best_ask = order.signal.best_ask;
        row.best_bid_tick = config.to_tick(order.signal.best_bid);
        row.best_ask_tick = config.to_tick(order.signal.best_ask);
        
        row.order_imbalance = order.signal.order_imbalance;
        row.trade_imbalance = order.signal.trade_imbalance;
        row.volatility = order.signal.volatility;
        row.queue_ahead_bid = order.signal.queue_ahead_bid;
        row.queue_ahead_ask = order.signal.queue_ahead_ask;

        row.distance_to_mid = price - order.signal.mid;
        row.distance_to_touch = (order.side == "BUY") ? price - order.signal.best_bid : price - order.signal.best_ask;
        row.inventory = order.signal.inventory;

        row.fair = order.signal.fair;
        row.skew = order.signal.skew;
        row.struct_delta = order.signal.struct_delta;
        row.micro_signal_delta = order.signal.micro_signal_delta;
        row.residual_delta = order.signal.residual_delta;
        row.reservation = order.signal.reservation;
        
        row.regime = order.signal.regime;
        row.regime_id = order.signal.regime_id;
        row.regime_prob = order.signal.regime_prob;
        row.alpha_trade_imb = order.signal.alpha_trade_imb;
        row.alpha_struct = order.signal.alpha_struct;
        row.alpha_residual = order.signal.alpha_residual;
        row.spread_multiplier = order.signal.spread_multiplier;
        row.inventory_target = order.signal.inventory_target;
        row.residual_signal_quality = order.signal.residual_signal_quality;
        row.tox = order.signal.toxicity.tox;
        row.k_spread = order.signal.toxicity.k_spread;
        row.k_order_size = order.signal.toxicity.k_order_size;
        
        quotes.push_back(move(row));

        if(quotes.size() >= config.QUOTES_CHUNK){
            quotes_path = build_file_path("quotes", quotes_id++);
            export_quote_parquet(quotes);
            quotes.clear();
        }
    }

    void export_quote_parquet(const vector<QuoteRow>& quotes){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder exchange_latency_b(pool);
        StringBuilder symbol_b(pool);
        StringBuilder client_oid_b(pool);
        StringBuilder side_b(pool);
        StringBuilder event_type_b(pool);

        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);

        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);
        
        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);

        DoubleBuilder distance_to_mid_b(pool);
        DoubleBuilder distance_to_touch_b(pool);
        DoubleBuilder inventory_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder alpha_residual_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k_spread_b(pool);
        DoubleBuilder k_order_size_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: quotes){
            ts_b.Append(r.ts);
            exchange_latency_b.Append(r.exchange_latency);
            symbol_b.Append(r.symbol);
            client_oid_b.Append(r.client_oid);
            side_b.Append(r.side);
            event_type_b.Append(r.event_type);

            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);
            
            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);

            distance_to_mid_b.Append(r.distance_to_mid);
            distance_to_touch_b.Append(r.distance_to_touch);
            inventory_b.Append(r.inventory);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            alpha_residual_b.Append(r.alpha_residual);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k_spread_b.Append(r.k_spread);
            k_order_size_b.Append(r.k_order_size);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, exchange_latency_arr, symbol_arr, client_oid_arr;
        shared_ptr<Array> side_arr, event_type_arr;

        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;

        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;

        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;

        shared_ptr<Array> distance_to_mid_arr, distance_to_touch_arr, inventory_arr;

        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr;
        shared_ptr<Array> micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr;
        shared_ptr<Array> spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr;

        ts_b.Finish(&ts_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        symbol_b.Finish(&symbol_arr);
        client_oid_b.Finish(&client_oid_arr);
        side_b.Finish(&side_arr);
        event_type_b.Finish(&event_type_arr);

        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);
        
        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);

        distance_to_mid_b.Finish(&distance_to_mid_arr);
        distance_to_touch_b.Finish(&distance_to_touch_arr);
        inventory_b.Finish(&inventory_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);

        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        alpha_residual_b.Finish(&alpha_residual_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k_spread_b.Finish(&k_spread_arr);
        k_order_size_b.Finish(&k_order_size_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("exchange_latency", int64()),
            field("symbol", utf8()),
            field("client_oid", utf8()),
            field("side", utf8()),
            field("event_type", utf8()),

            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),

            field("distance_to_mid", float64()),
            field("distance_to_touch", float64()),
            field("inventory", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),

            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("alpha_residual", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k_spread", float64()),
            field("k_order_size", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, exchange_latency_arr, symbol_arr, client_oid_arr,
            side_arr, event_type_arr,
            price_arr, price_tick_arr, qty_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            distance_to_mid_arr, distance_to_touch_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr,
            micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr,
            spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(quotes_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_fill(const Order& order, const double& fill_qty, const int64_t& fill_ts, bool is_maker){
        FillRow row;

        auto& book = state.market_book;

        auto [bid_tick, bid_size] = book.best_bid();
        auto [ask_tick, ask_size] = book.best_ask();

        double best_bid = config.from_tick(bid_tick);
        double best_ask = config.from_tick(ask_tick);

        double mid = (best_bid + best_ask) / 2.0;
        double microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size + 1e-9);

        double volatility_at_fill = state.get_vol();

        row.ts = state.last_trade_ts;
        row.exchange_latency = order.exchange_latency;
        row.time_to_fill = fill_ts - order.live_ts;
        row.symbol = config.instrument_upper;
        row.side = order.side;
        row.price = config.from_tick(order.price_tick);
        row.price_tick = order.price_tick;
        row.qty = fill_qty;
        row.cum_qty = order.qty - order.remaining;

        row.is_maker = is_maker;
        row.fill_sign = (order.side == "BUY") ? 1 : -1;
        row.fill_type = (order.side == "BUY") ? "BID_HIT" : "ASK_LIFT";
        row.fill_status = (order.remaining > 0.0) ? "PARTIAL" : "FULL";
        row.queue_ahead_at_join = order.queue_ahead_at_join;

        row.mid_at_fill = mid;
        row.microprice_at_fill = microprice;
        row.microprice_dev_at_fill = microprice - mid;
        row.microprice_error_at_fill = mid - microprice;
        row.spread_at_fill = best_ask - best_bid;
        row.best_bid_at_fill = best_bid;
        row.best_ask_at_fill = best_ask;
        row.volatility_at_fill = volatility_at_fill;
        row.volatility_at_fill_bps = volatility_at_fill * 10000.0;

        row.mid = order.signal.mid;
        row.microprice = order.signal.microprice;
        row.microprice_dev = order.signal.microprice_dev;
        row.microprice_error = order.signal.microprice_error;
        row.spread = order.signal.spread;
        row.best_bid = order.signal.best_bid;
        row.best_ask = order.signal.best_ask;
        row.best_bid_tick = config.to_tick(order.signal.best_bid);
        row.best_ask_tick = config.to_tick(order.signal.best_ask);

        row.order_imbalance = order.signal.order_imbalance;
        row.trade_imbalance = order.signal.trade_imbalance;
        row.volatility = order.signal.volatility;
        row.volatility_bps = order.signal.volatility * 10000.0;
        row.queue_ahead_bid = order.signal.queue_ahead_bid;
        row.queue_ahead_ask = order.signal.queue_ahead_ask;
        row.inventory = state.inventory;

        row.fair = order.signal.fair;
        row.skew = order.signal.skew;
        row.struct_delta = order.signal.struct_delta;
        row.micro_signal_delta = order.signal.micro_signal_delta;
        row.residual_delta = order.signal.residual_delta;
        row.reservation = order.signal.reservation;

        row.regime = order.signal.regime;
        row.regime_id = order.signal.regime_id;
        row.regime_prob = order.signal.regime_prob;
        row.alpha_trade_imb = order.signal.alpha_trade_imb;
        row.alpha_struct = order.signal.alpha_struct;
        row.alpha_residual = order.signal.alpha_residual;
        row.spread_multiplier = order.signal.spread_multiplier;
        row.inventory_target = order.signal.inventory_target;
        row.residual_signal_quality = order.signal.residual_signal_quality;
        row.tox = order.signal.toxicity.tox;
        row.k_spread = order.signal.toxicity.k_spread;
        row.k_order_size = order.signal.toxicity.k_order_size;

        row.my_bid = order.signal.my_bid;
        row.my_ask = order.signal.my_ask;
        row.my_bid_tick = config.to_tick(order.signal.my_bid);
        row.my_ask_tick = config.to_tick(order.signal.my_ask);
        
        row.bid_distance_touch = order.signal.my_bid - best_bid;
        row.ask_distance_touch = order.signal.my_ask - best_ask;
        row.bid_distance_spread = order.signal.my_bid - best_ask;
        row.ask_distance_spread = order.signal.my_ask - best_bid;

        fills.push_back(move(row));

        if(fills.size() >= config.FILLS_CHUNK){
            fills_path = build_file_path("fills", fills_id++);
            export_fill_parquet(fills);
            fills.clear();
        }
    }

    void export_fill_parquet(const vector<FillRow>& fills){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
        Int64Builder exchange_latency_b(pool);
        Int64Builder time_to_fill_b(pool);
        StringBuilder symbol_b(pool);
        StringBuilder side_b(pool);
        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);
        DoubleBuilder cum_qty_b(pool);

        BooleanBuilder is_maker_b(pool);
        Int32Builder fill_sign_b(pool);
        StringBuilder fill_type_b(pool);
        StringBuilder fill_status_b(pool);
        DoubleBuilder queue_ahead_at_join_b(pool);

        DoubleBuilder mid_at_fill_b(pool);
        DoubleBuilder microprice_at_fill_b(pool);
        DoubleBuilder microprice_dev_at_fill_b(pool);
        DoubleBuilder microprice_error_at_fill_b(pool);
        DoubleBuilder spread_at_fill_b(pool);
        DoubleBuilder best_bid_at_fill_b(pool);
        DoubleBuilder best_ask_at_fill_b(pool);
        DoubleBuilder volatility_at_fill_b(pool);
        DoubleBuilder volatility_at_fill_bps_b(pool);

        DoubleBuilder mid_b(pool);
        DoubleBuilder microprice_b(pool);
        DoubleBuilder microprice_dev_b(pool);
        DoubleBuilder microprice_error_b(pool);
        DoubleBuilder spread_b(pool);
        DoubleBuilder best_bid_b(pool);
        DoubleBuilder best_ask_b(pool);
        Int64Builder best_bid_tick_b(pool);
        Int64Builder best_ask_tick_b(pool);

        DoubleBuilder order_imbalance_b(pool);
        DoubleBuilder trade_imbalance_b(pool);
        DoubleBuilder volatility_b(pool);
        DoubleBuilder volatility_bps_b(pool);
        DoubleBuilder queue_ahead_bid_b(pool);
        DoubleBuilder queue_ahead_ask_b(pool);
        DoubleBuilder inventory_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder residual_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        Int32Builder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder alpha_residual_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder residual_signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k_spread_b(pool);
        DoubleBuilder k_order_size_b(pool);

        DoubleBuilder my_bid_b(pool);
        DoubleBuilder my_ask_b(pool);
        Int64Builder my_bid_tick_b(pool);
        Int64Builder my_ask_tick_b(pool);

        DoubleBuilder bid_dist_touch_b(pool);
        DoubleBuilder ask_dist_touch_b(pool);
        DoubleBuilder bid_dist_spread_b(pool);
        DoubleBuilder ask_dist_spread_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: fills){
            ts_b.Append(r.ts);
            exchange_latency_b.Append(r.exchange_latency);
            time_to_fill_b.Append(r.time_to_fill);
            symbol_b.Append(r.symbol);
            side_b.Append(r.side);
            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);
            cum_qty_b.Append(r.cum_qty);

            is_maker_b.Append(r.is_maker);
            fill_sign_b.Append(r.fill_sign);
            fill_type_b.Append(r.fill_type);
            fill_status_b.Append(r.fill_status);
            queue_ahead_at_join_b.Append(r.queue_ahead_at_join);

            mid_at_fill_b.Append(r.mid_at_fill);
            microprice_at_fill_b.Append(r.microprice_at_fill);
            microprice_dev_at_fill_b.Append(r.microprice_dev_at_fill);
            microprice_error_at_fill_b.Append(r.microprice_error_at_fill);
            spread_at_fill_b.Append(r.spread_at_fill);
            best_bid_at_fill_b.Append(r.best_bid_at_fill);
            best_ask_at_fill_b.Append(r.best_ask_at_fill);
            volatility_at_fill_b.Append(r.volatility_at_fill);
            volatility_at_fill_bps_b.Append(r.volatility_at_fill_bps);

            mid_b.Append(r.mid);
            microprice_b.Append(r.microprice);
            microprice_dev_b.Append(r.microprice_dev);
            microprice_error_b.Append(r.microprice_error);
            spread_b.Append(r.spread);
            best_bid_b.Append(r.best_bid);
            best_ask_b.Append(r.best_ask);
            best_bid_tick_b.Append(r.best_bid_tick);
            best_ask_tick_b.Append(r.best_ask_tick);
            
            order_imbalance_b.Append(r.order_imbalance);
            trade_imbalance_b.Append(r.trade_imbalance);
            volatility_b.Append(r.volatility);
            volatility_bps_b.Append(r.volatility_bps);
            queue_ahead_bid_b.Append(r.queue_ahead_bid);
            queue_ahead_ask_b.Append(r.queue_ahead_ask);
            inventory_b.Append(r.inventory);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            residual_delta_b.Append(r.residual_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            alpha_residual_b.Append(r.alpha_residual);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            residual_signal_quality_b.Append(r.residual_signal_quality);
            tox_b.Append(r.tox);
            k_spread_b.Append(r.k_spread);
            k_order_size_b.Append(r.k_order_size);

            my_bid_b.Append(r.my_bid);
            my_ask_b.Append(r.my_ask);
            my_bid_tick_b.Append(r.my_bid_tick);
            my_ask_tick_b.Append(r.my_ask_tick);

            bid_dist_touch_b.Append(r.bid_distance_touch);
            ask_dist_touch_b.Append(r.ask_distance_touch);
            bid_dist_spread_b.Append(r.bid_distance_spread);
            ask_dist_spread_b.Append(r.ask_distance_spread);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, exchange_latency_arr, time_to_fill_arr, symbol_arr, side_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr, cum_qty_arr;
        shared_ptr<Array> is_maker_arr, fill_sign_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr;
        
        shared_ptr<Array> mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr;
        shared_ptr<Array> spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr;
        shared_ptr<Array> volatility_at_fill_arr, volatility_at_fill_bps_arr;

        
        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr;
        shared_ptr<Array> spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr;
        
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr;
        shared_ptr<Array> spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr;

        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_dist_touch_arr, ask_dist_touch_arr;
        shared_ptr<Array> bid_dist_spread_arr, ask_dist_spread_arr;

        ts_b.Finish(&ts_arr);
        exchange_latency_b.Finish(&exchange_latency_arr);
        time_to_fill_b.Finish(&time_to_fill_arr);
        symbol_b.Finish(&symbol_arr);
        side_b.Finish(&side_arr);
        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);
        cum_qty_b.Finish(&cum_qty_arr);

        is_maker_b.Finish(&is_maker_arr);
        fill_sign_b.Finish(&fill_sign_arr);
        fill_type_b.Finish(&fill_type_arr);
        fill_status_b.Finish(&fill_status_arr);
        queue_ahead_at_join_b.Finish(&queue_ahead_at_join_arr);

        mid_at_fill_b.Finish(&mid_at_fill_arr);
        microprice_at_fill_b.Finish(&microprice_at_fill_arr);
        microprice_dev_at_fill_b.Finish(&microprice_dev_at_fill_arr);
        microprice_error_at_fill_b.Finish(&microprice_error_at_fill_arr);
        spread_at_fill_b.Finish(&spread_at_fill_arr);
        best_bid_at_fill_b.Finish(&best_bid_at_fill_arr);
        best_ask_at_fill_b.Finish(&best_ask_at_fill_arr);
        volatility_at_fill_b.Finish(&volatility_at_fill_arr);
        volatility_at_fill_bps_b.Finish(&volatility_at_fill_bps_arr);

        mid_b.Finish(&mid_arr);
        microprice_b.Finish(&microprice_arr);
        microprice_dev_b.Finish(&microprice_dev_arr);
        microprice_error_b.Finish(&microprice_error_arr);
        spread_b.Finish(&spread_arr);
        best_bid_b.Finish(&best_bid_arr);
        best_ask_b.Finish(&best_ask_arr);
        best_bid_tick_b.Finish(&best_bid_tick_arr);
        best_ask_tick_b.Finish(&best_ask_tick_arr);

        order_imbalance_b.Finish(&order_imbalance_arr);
        trade_imbalance_b.Finish(&trade_imbalance_arr);
        volatility_b.Finish(&volatility_arr);
        volatility_bps_b.Finish(&volatility_bps_arr);
        queue_ahead_bid_b.Finish(&queue_ahead_bid_arr);
        queue_ahead_ask_b.Finish(&queue_ahead_ask_arr);
        inventory_b.Finish(&inventory_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        residual_delta_b.Finish(&residual_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        alpha_residual_b.Finish(&alpha_residual_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        residual_signal_quality_b.Finish(&residual_signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k_spread_b.Finish(&k_spread_arr);
        k_order_size_b.Finish(&k_order_size_arr);

        my_bid_b.Finish(&my_bid_arr);
        my_ask_b.Finish(&my_ask_arr);
        my_bid_tick_b.Finish(&my_bid_tick_arr);
        my_ask_tick_b.Finish(&my_ask_tick_arr);

        bid_dist_touch_b.Finish(&bid_dist_touch_arr);
        ask_dist_touch_b.Finish(&ask_dist_touch_arr);
        bid_dist_spread_b.Finish(&bid_dist_spread_arr);
        ask_dist_spread_b.Finish(&ask_dist_spread_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
            field("exchange_latency", int64()),
            field("time_to_fill", int64()),
            field("symbol", utf8()),
            field("side", utf8()),
            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
            field("cum_qty", float64()),
            field("is_maker", boolean()),
            field("fill_sign", int32()),
            field("fill_type", utf8()),
            field("fill_status", utf8()),
            field("queue_ahead_at_join", float64()),

            field("mid_at_fill", float64()),
            field("microprice_at_fill", float64()),
            field("microprice_dev_at_fill", float64()),
            field("microprice_error_at_fill", float64()),
            field("spread_at_fill", float64()),
            field("best_bid_at_fill", float64()),
            field("best_ask_at_fill", float64()),
            field("volatility_at_fill", float64()),
            field("volatility_at_fill_bps", float64()),

            field("mid", float64()),
            field("microprice", float64()),
            field("microprice_dev", float64()),
            field("microprice_error", float64()),
            field("spread", float64()),
            field("best_bid", float64()),
            field("best_ask", float64()),
            field("best_bid_tick", int64()),
            field("best_ask_tick", int64()),

            field("order_imbalance", float64()),
            field("trade_imbalance", float64()),
            field("volatility", float64()),
            field("volatility_bps", float64()),
            field("queue_ahead_bid", float64()),
            field("queue_ahead_ask", float64()),
            field("inventory", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("residual_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", int32()),
            field("regime_prob", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("alpha_residual", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("residual_signal_quality", float64()),
            field("tox", float64()),
            field("k_spread", float64()),
            field("k_order_size", float64()),
            
            field("my_bid", float64()),
            field("my_ask", float64()),
            field("my_bid_tick", int64()),
            field("my_ask_tick", int64()),

            field("bid_distance_touch", float64()),
            field("ask_distance_touch", float64()),
            field("bid_distance_spread", float64()),
            field("ask_distance_spread", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, exchange_latency_arr, time_to_fill_arr, symbol_arr, side_arr,
            price_arr, price_tick_arr, qty_arr, cum_qty_arr,
            is_maker_arr, fill_sign_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr,
            mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr,
            spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr,
            volatility_at_fill_arr, volatility_at_fill_bps_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr,
            spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, residual_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_trade_imb_arr, alpha_struct_arr, alpha_residual_arr,
            spread_multiplier_arr, inventory_target_arr,
            residual_signal_quality_arr, tox_arr, k_spread_arr, k_order_size_arr,
            my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr,
            bid_dist_touch_arr, ask_dist_touch_arr,
            bid_dist_spread_arr, ask_dist_spread_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(fills_path).Value(&out);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }
};