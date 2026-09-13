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

#include <curl/curl.h>
#include <cpr/cpr.h>
#include "simdjson.h"
#include <nlohmann/json.hpp>
#include <xgboost/c_api.h>

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
#include "mmpy_config.hpp" //market config & orderbook
#include "mmpy_dashboard.hpp" //dashboard classes
#include "mmpy_feed.hpp" // feeds
#include "mmpy_state.hpp" //state & market_feature_state

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
struct FillRow {};
class DatasetRecorder {
public:
    MarketConfig& config;
    State& state;
    const json& params;
    std_string instrument_upper;

    vector<SnapshotRow> snapshots;
    vector<TradeRow> trades;
    vector<QuoteRow> quotes;
    vector<FillRow> fills;
    
    vector<EventsRow> events;
    json orderbook_snapshot;

    std_string folder_path;
    std_string events_path;
    std_string orderbook_snapshot_path;

    std_string snapshots_path;
    std_string trades_path;
    std_string quotes_path;
    std_string fills_path;

    static constexpr size_t SNAPSHOT_CHUNK = 10000;
    static constexpr size_t TRADE_CHUNK = 10000;
    static constexpr size_t QUOTE_CHUNK = 10000;
    static constexpr size_t FILL_CHUNK  = 5000;   // usually smaller

    int snapshots_id = 0;
    int trades_id = 0;
    int quotes_id = 0;
    int fills_id = 0;

    DatasetRecorder(MarketConfig& config, State& state, const json& params)
        : config(config), state(state), params(params)
    {
        initialize();
    }

    std_string build_file_path(const std_string& file, int file_id){
        ostringstream ss;
        ss << folder_path << '/' << file << '_' << setw(6) << setfill('0') << file_id << ".parquet";
        cout << "file_path: " << ss.str() << "\n";
        return ss.str();
    }

    void initialize(){
        instrument_upper = params["instrument"].get<std_string>(),
        ranges::transform(instrument_upper, instrument_upper.begin(), [](unsigned char c){ return toupper(c);});
        cout << "instrument_upper: " << instrument_upper << "\n";

        std_string current_run_path = params["folder_path"].get<std_string>();

        auto now = system_clock::now();
        time_t now_t = system_clock::to_time_t(now);

        tm tm{};
        gmtime_s(&tm, &now_t);

        ostringstream ss;
        ss << put_time(&tm, "%Y%m%d_%H%M%S");

        folder_path = current_run_path + "/runs/run_" + ss.str();
        filesystem::create_directories(folder_path);

        events_path = folder_path + "/" + params["files"]["replay_events"]["events"].get<std_string>();
        orderbook_snapshot_path = folder_path + "/" + params["files"]["replay_events"]["orderbook_snapshot"].get<std_string>();
        snapshots_path = build_file_path("snapshots", snapshots_id++);
        trades_path = build_file_path("trades", trades_id++);
        quotes_path = build_file_path("quotes", quotes_id++);
        fills_path = build_file_path("fills", fills_id++);
    }

    void log_event(const std_string& type, const uint64_t& ts, const std_string& msg){
        events.push_back(EventsRow{type, ts, msg});
    }

    void log_orderbook_snapshot(const json& snapshot){
        orderbook_snapshot = snapshot;
    }

    void export_event_parquet(){
        std_string filename = folder_path + "/" + params["files"]["replay_events"]["events"].get<std_string>();
        cout << "Exported events: " << filename << "\n";
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        StringBuilder type_b(pool);
        Int64Builder ts_b(pool);
        StringBuilder msg_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& e: events){
            type_b.Append(e.type);
            ts_b.Append(e.ts);
            msg_b.Append(e.msg);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> type_arr, ts_arr, msg_arr;

        type_b.Finish(&type_arr);
        ts_b.Finish(&ts_arr);
        msg_b.Finish(&msg_arr);
        
        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("type", utf8()),
            field("ts", int64()),
            field("msg", utf8())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {type_arr, ts_arr, msg_arr});

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> out;
        arrow::io::FileOutputStream::Open(filename).Value(&out);
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1024);
    }

    void export_orderbook_snapshot(){
        ofstream(orderbook_snapshot_path) << orderbook_snapshot.dump(4);
    }

    void log_snapshot(const uint64_t& ts, const Signal& signal){
        SnapshotRow row;

        row.ts = ts;
        row.symbol = instrument_upper;

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
        row.equity = signal.equity;

        row.fair = signal.fair;
        row.skew = signal.skew;
        row.struct_delta = signal.struct_delta;
        row.micro_signal_delta = signal.micro_signal_delta;
        row.ml_delta = signal.ml_delta;
        row.reservation = signal.reservation;

        row.regime = signal.regime;
        row.regime_id = signal.regime_id;
        row.regime_prob = signal.regime_prob;
        row.alpha_order_imb = signal.alpha_order_imb;
        row.alpha_trade_imb = signal.alpha_trade_imb;
        row.alpha_struct = signal.alpha_struct;
        row.k0 = signal.k0;
        row.spread_multiplier = signal.spread_multiplier;
        row.inventory_target = signal.inventory_target;
        row.signal_quality = signal.signal_quality;
        row.tox = signal.toxicity.tox;
        row.k1 = signal.toxicity.k1;
        row.k2 = signal.toxicity.k2;

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

        if(snapshots.size() >= SNAPSHOT_CHUNK){
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
        DoubleBuilder equity_b(pool);

        DoubleBuilder fair_b(pool);
        DoubleBuilder skew_b(pool);
        DoubleBuilder struct_delta_b(pool);
        DoubleBuilder micro_signal_delta_b(pool);
        DoubleBuilder ml_delta_b(pool);
        DoubleBuilder reservation_b(pool);
        
        StringBuilder regime_b(pool);
        DoubleBuilder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

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
            equity_b.Append(r.equity);

            fair_b.Append(r.fair);
            skew_b.Append(r.skew);
            struct_delta_b.Append(r.struct_delta);
            micro_signal_delta_b.Append(r.micro_signal_delta);
            ml_delta_b.Append(r.ml_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            signal_quality_b.Append(r.signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);

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
        shared_ptr<Array> ts_arr, symbol_arr, mid_arr, mid_tick_arr;
        shared_ptr<Array> microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;
        shared_ptr<Array> inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, equity_arr;
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, ml_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> signal_quality_arr, tox_arr, k1_arr, k2_arr;
        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_distance_touch_arr, ask_distance_touch_arr;
        shared_ptr<Array> bid_distance_spread_arr, ask_distance_spread_arr;
        shared_ptr<Array> bid_delta_arr, ask_delta_arr, quote_churn_arr;

        ts_b.Finish(&ts_arr);
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
        equity_b.Finish(&equity_arr);

        fair_b.Finish(&fair_arr);
        skew_b.Finish(&skew_arr);
        struct_delta_b.Finish(&struct_delta_arr);
        micro_signal_delta_b.Finish(&micro_signal_delta_arr);
        ml_delta_b.Finish(&ml_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        signal_quality_b.Finish(&signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

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
            field("equity", float64()),

            field("fair", float64()),
            field("skew", float64()),
            field("struct_delta", float64()),
            field("micro_signal_delta", float64()),
            field("ml_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", float64()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64()),

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
            ts_arr, symbol_arr, mid_arr, mid_tick_arr,
            microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            inventory_arr, realized_pnl_arr, unrealized_pnl_arr, total_pnl_arr, equity_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, ml_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            signal_quality_arr, tox_arr, k1_arr, k2_arr,
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
        row.symbol = instrument_upper;

        row.price = trade.price;
        row.price_tick = config.to_tick(trade.price);
        row.qty = trade.qty;
        row.side = trade.side;
        row.is_buyer_marker = (trade.side == "SELL") ? true : false;

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

        row.trade_to_mid = price - mid;
        row.trade_to_microprice = price - microprice;
        row.price_to_best_bid = price - best_bid;
        row.price_to_best_ask = price - best_ask;

        row.trade_side = trade.side == "SELL" ? "SELL_AGGRESSOR" : "BUY_AGGRESSOR";
        row.trade_sign = trade.side == "SELL" ? -1 : 1;

        row.notional = notional;
        row.log_notional = log1p(notional);
        row.intensity = trade.qty / (bid_size + ask_size + 1e-9);

        trades.push_back(move(row));

        if(trades.size() >= TRADE_CHUNK){
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
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_best_tick_arr, best_ask_tick_arr;
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
            best_bid_arr, best_ask_arr, best_best_tick_arr, best_ask_tick_arr,
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

        row.ts = order.ts;
        row.symbol = instrument_upper;
        row.client_oid = order.client_oid;
        row.side = side;
        row.event_type = event_type;

        row.price = config.from_tick(order.price_tick);
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
        row.ml_delta = order.signal.ml_delta;
        row.reservation = order.signal.reservation;
        
        row.regime = order.signal.regime;
        row.regime_id = order.signal.regime_id;
        row.regime_prob = order.signal.regime_prob;
        row.alpha_order_imb = order.signal.alpha_order_imb;
        row.alpha_trade_imb = order.signal.alpha_trade_imb;
        row.alpha_struct = order.signal.alpha_struct;
        row.k0 = order.signal.k0;
        row.spread_multiplier = order.signal.spread_multiplier;
        row.inventory_target = order.signal.inventory_target;
        row.signal_quality = order.signal.signal_quality;
        row.tox = order.signal.toxicity.tox;
        row.k1 = order.signal.toxicity.k1;
        row.k2 = order.signal.toxicity.k2;
        
        quotes.push_back(move(row));

        if(quotes.size() >= QUOTE_CHUNK){
            quotes_path = build_file_path("quotes", quotes_id++);
            export_snapshot_parquet(quotes);
            quotes.clear();
        }
    }

    void export_quote_parquet(const vector<QuoteRow>& quotes){
        MemoryPool* pool = default_memory_pool();

        // -------------------------
        // BUILDERS
        // -------------------------
        Int64Builder ts_b(pool);
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
        DoubleBuilder ml_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        DoubleBuilder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

        // -------------------------
        // FILL
        // -------------------------
        for(const auto& r: quotes){
            ts_b.Append(r.ts);
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
            ml_delta_b.Append(r.ml_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            signal_quality_b.Append(r.signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);
        }

        // -------------------------
        // FINISH ARRAYS
        // -------------------------
        shared_ptr<Array> ts_arr, symbol_arr, client_oid_arr;
        shared_ptr<Array> side_arr, event_type_arr;

        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;

        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr;
        shared_ptr<Array> best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;

        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr;

        shared_ptr<Array> distance_to_mid_arr, distance_to_touch_arr, inventory_arr;

        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr;
        shared_ptr<Array> micro_signal_delta_arr, ml_delta_arr, reservation_arr;
        
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> signal_quality_arr, tox_arr, k1_arr, k2_arr;

        ts_b.Finish(&ts_arr);
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
        ml_delta_b.Finish(&ml_delta_arr);
        reservation_b.Finish(&reservation_arr);

        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        signal_quality_b.Finish(&signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

        // -------------------------
        // SCHEMA
        // -------------------------
        auto schema = arrow::schema({
            field("ts", int64()),
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
            field("ml_delta", float64()),
            field("reservation", float64()),

            field("regime", utf8()),
            field("regime_id", float64()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64())
        });

        // -------------------------
        // TABLE
        // -------------------------
        auto table = arrow::Table::Make(schema, {
            ts_arr, symbol_arr, client_oid_arr,
            side_arr, event_type_arr,
            price_arr, price_tick_arr, qty_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr, spread_arr,
            best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr,
            distance_to_mid_arr, distance_to_touch_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr,
            micro_signal_delta_arr, ml_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            signal_quality_arr, tox_arr, k1_arr, k2_arr
        });

        // -------------------------
        // WRITE PARQUET
        // -------------------------
        shared_ptr<arrow::io::FileOutputStream> outfile;
        arrow::io::FileOutputStream::Open(quotes_path).Value(&outfile);
        parquet::arrow::WriteTable(*table, pool, out, 1024);
    }

    void log_fill(const double& qty, const Order& order, const bool& is_maker){
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
        row.symbol = instrument_upper;
        row.side = order.side;
        row.price = config.from_tick(order.price_tick);
        row.price_tick = order.price_tick;
        row.qty = qty;

        row.is_maker = is_maker;
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

        row.fair = signal.fair;
        row.skew = signal.skew;
        row.struct_delta = signal.struct_delta;
        row.micro_signal_delta = signal.micro_signal_delta;
        row.ml_delta = signal.ml_delta;
        row.reservation = signal.reservation;

        row.regime = signal.regime;
        row.regime_id = signal.regime_id;
        row.regime_prob = signal.regime_prob;
        row.alpha_order_imb = signal.alpha_order_imb;
        row.alpha_trade_imb = signal.alpha_trade_imb;
        row.alpha_struct = signal.alpha_struct;
        row.k0 = signal.k0;
        row.spread_multiplier = signal.spread_multiplier;
        row.inventory_target = signal.inventory_target;
        row.signal_quality = signal.signal_quality;
        row.tox = signal.toxicity.tox;
        row.k1 = signal.toxicity.k1;
        row.k2 = signal.toxicity.k2;

        row.my_bid = order.signal.my_bid;
        row.my_ask = order.signal.my_ask;
        row.my_bid_tick = config.to_tick(order.signal.my_bid);
        row.my_ask_tick = config.to_tick(order.signal.my_ask);
        
        row.bid_distance_touch = order.signal.my_bid - best_bid;
        row.ask_distance_touch = order.signal.my_ask - best_ask;
        row.bid_distance_spread = order.signal.my_bid - best_ask;
        row.ask_distance_spread = order.signal.my_ask - best_bid;

        fills.push_back(move(row));

        if(fills.size() >= FILL_CHUNK){
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
        StringBuilder symbol_b(pool);
        StringBuilder side_b(pool);
        DoubleBuilder price_b(pool);
        Int64Builder price_tick_b(pool);
        DoubleBuilder qty_b(pool);

        BooleanBuilder is_maker_b(pool);
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
        DoubleBuilder ml_delta_b(pool);
        DoubleBuilder reservation_b(pool);

        StringBuilder regime_b(pool);
        DoubleBuilder regime_id_b(pool);
        DoubleBuilder regime_prob_b(pool);
        DoubleBuilder alpha_order_imb_b(pool);
        DoubleBuilder alpha_trade_imb_b(pool);
        DoubleBuilder alpha_struct_b(pool);
        DoubleBuilder k0_b(pool);
        DoubleBuilder spread_multiplier_b(pool);
        DoubleBuilder inventory_target_b(pool);
        DoubleBuilder signal_quality_b(pool);
        DoubleBuilder tox_b(pool);
        DoubleBuilder k1_b(pool);
        DoubleBuilder k2_b(pool);

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
            symbol_b.Append(r.symbol);
            side_b.Append(r.side);
            price_b.Append(r.price);
            price_tick_b.Append(r.price_tick);
            qty_b.Append(r.qty);

            is_maker_b.Append(r.is_maker);
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
            ml_delta_b.Append(r.ml_delta);
            reservation_b.Append(r.reservation);

            regime_b.Append(r.regime);
            regime_id_b.Append(r.regime_id);
            regime_prob_b.Append(r.regime_prob);
            alpha_order_imb_b.Append(r.alpha_order_imb);
            alpha_trade_imb_b.Append(r.alpha_trade_imb);
            alpha_struct_b.Append(r.alpha_struct);
            k0_b.Append(r.k0);
            spread_multiplier_b.Append(r.spread_multiplier);
            inventory_target_b.Append(r.inventory_target);
            signal_quality_b.Append(r.signal_quality);
            tox_b.Append(r.tox);
            k1_b.Append(r.k1);
            k2_b.Append(r.k2);

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
        shared_ptr<Array> ts_arr, symbol_arr, side_arr;
        shared_ptr<Array> price_arr, price_tick_arr, qty_arr;
        shared_ptr<Array> is_maker_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr;
        
        shared_ptr<Array> mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr;
        shared_ptr<Array> spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr;
        shared_ptr<Array> volatility_at_fill_arr, volatility_at_fill_bps_arr;

        
        shared_ptr<Array> mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr;
        shared_ptr<Array> spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr;
        
        shared_ptr<Array> order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr;
        shared_ptr<Array> queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr;
        
        shared_ptr<Array> fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, ml_delta_arr, reservation_arr;
        shared_ptr<Array> regime_arr, regime_id_arr, regime_prob_arr;
        shared_ptr<Array> alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr;
        shared_ptr<Array> k0_arr, spread_multiplier_arr, inventory_target_arr;
        shared_ptr<Array> signal_quality_arr, tox_arr, k1_arr, k2_arr;

        shared_ptr<Array> my_bid_arr, my_ask_arr, my_bid_tick_arr, my_ask_tick_arr;
        shared_ptr<Array> bid_dist_touch_arr, ask_dist_touch_arr;
        shared_ptr<Array> bid_dist_spread_arr, ask_dist_spread_arr;

        ts_b.Finish(&ts_arr);
        symbol_b.Finish(&symbol_arr);
        side_b.Finish(&side_arr);
        price_b.Finish(&price_arr);
        price_tick_b.Finish(&price_tick_arr);
        qty_b.Finish(&qty_arr);

        is_maker_b.Finish(&is_maker_arr);
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
        ml_delta_b.Finish(&ml_delta_arr);
        reservation_b.Finish(&reservation_arr);
        
        regime_b.Finish(&regime_arr);
        regime_id_b.Finish(&regime_id_arr);
        regime_prob_b.Finish(&regime_prob_arr);
        alpha_order_imb_b.Finish(&alpha_order_imb_arr);
        alpha_trade_imb_b.Finish(&alpha_trade_imb_arr);
        alpha_struct_b.Finish(&alpha_struct_arr);
        k0_b.Finish(&k0_arr);
        spread_multiplier_b.Finish(&spread_multiplier_arr);
        inventory_target_b.Finish(&inventory_target_arr);
        signal_quality_b.Finish(&signal_quality_arr);
        tox_b.Finish(&tox_arr);
        k1_b.Finish(&k1_arr);
        k2_b.Finish(&k2_arr);

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
            field("symbol", utf8()),
            field("side", utf8()),
            field("price", float64()),
            field("price_tick", int64()),
            field("qty", float64()),
            field("is_maker", boolean()),
            field("fill_type", utf8()),
            field("fill_status", utf8()),
            field("queue_ahead_at_join", float64()),

            field("mid_at_fill", float64()),
            field("microprice_at_fill", float64()),
            field("microprice_dev_at_fill", float64()),
            field("microprice_error_at_fill", float64()),
            field("best_bid_at_fill", float64()),
            field("best_ask_at_fill", float64()),
            field("mid_at_fill", float64()),
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
            field("ml_delta", float64()),
            field("reservation", float64()),
            
            field("regime", utf8()),
            field("regime_id", float64()),
            field("regime_prob", float64()),
            field("alpha_order_imb", float64()),
            field("alpha_trade_imb", float64()),
            field("alpha_struct", float64()),
            field("k0", float64()),
            field("spread_multiplier", float64()),
            field("inventory_target", float64()),
            field("signal_quality", float64()),
            field("tox", float64()),
            field("k1", float64()),
            field("k2", float64()),
            
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
            ts_arr, symbol_arr, side_arr,
            price_arr, price_tick_arr, qty_arr,
            is_maker_arr, fill_type_arr, fill_status_arr, queue_ahead_at_join_arr,
            mid_at_fill_arr, microprice_at_fill_arr, microprice_dev_at_fill_arr, microprice_error_at_fill_arr,
            spread_at_fill_arr, best_bid_at_fill_arr, best_ask_at_fill_arr,
            volatility_at_fill_arr, volatility_at_fill_bps_arr,
            mid_arr, microprice_arr, microprice_dev_arr, microprice_error_arr,
            spread_arr, best_bid_arr, best_ask_arr, best_bid_tick_arr, best_ask_tick_arr,
            order_imbalance_arr, trade_imbalance_arr, volatility_arr, volatility_bps_arr,
            queue_ahead_bid_arr, queue_ahead_ask_arr, inventory_arr,
            fair_arr, skew_arr, struct_delta_arr, micro_signal_delta_arr, ml_delta_arr, reservation_arr,
            regime_arr, regime_id_arr, regime_prob_arr,
            alpha_order_imb_arr, alpha_trade_imb_arr, alpha_struct_arr,
            k0_arr, spread_multiplier_arr, inventory_target_arr,
            signal_quality_arr, tox_arr, k1_arr, k2_arr,
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

    void shutdown(){
        state.update_performance();

        // flush snapshots
        if(!snapshots.empty()){
            snapshots_path = build_file_path("snapshots", snapshots_id++);
            export_snapshot_parquet(snapshots);
            snapshots.clear(); // REMOVE THIS, UNNECCESSARY
        }

        // flush trades
        if(!trades.empty()){
            write_trade_chunk(vector<TradeRow>(make_move_iterator(trades.begin()),
                                            make_move_iterator(trades.end())));
            trades.clear();
        }

        // flush quotes
        if(!quotes.empty()){
            write_quote_chunk(vector<QuoteRow>(make_move_iterator(quotes.begin()),
                                            make_move_iterator(quotes.end())));
            quotes.clear();
        }

        // flush fills
        if(!fills.empty()){
            write_fill_chunk(vector<FillRow>(make_move_iterator(fills.begin()),
                                            make_move_iterator(fills.end())));
            fills.clear();
        }

        std_string events_path   = run_path + "/events.json";
        std_string ob_path       = run_path + "/orderbook_snapshot.json";
        std_string manifest_path = run_path + "/manifest.json";

        // -------------------------
        // EVENTS JSON
        // -------------------------
        {
            ofstream f(events_path);
            f << events_to_json().dump(2);
        }

        // -------------------------
        // ORDERBOOK SNAPSHOT JSON
        // -------------------------
        {
            ofstream f(ob_path);
            f << orderbook_snapshot.dump(2);
        }

        // -------------------------
        // MANIFEST
        // -------------------------
        json manifest = params;

        manifest["run_id"] = run_id;
        manifest["folder_path"] = run_path;

        manifest["performance"] = {
            {"pnl", round(state.get_pnl() * 10000.0) / 10000.0},
            {"sharpe", round(state.compute_sharpe() * 10000.0) / 10000.0},
            {"fees_paid", round(state.fees_paid * 10000.0) / 10000.0},
            {"fees_per_fill", state.fees_paid / (fills.size() + 1e-9)},
            {"pnl_per_fill", state.get_pnl() / (fills.size() + 1e-9)}
        };

        {
            ofstream f(manifest_path);
            f << manifest.dump(2);
        }

        cout << "DATASETS SAVED SUCCESSFULLY\n";
        cout << "Saved run → " << run_path << "\n";
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

    void on_trade_event(const Trade& trade){
        // recorder.log_trade(trade, toupper(instrument));
        // execution.process_trade(trade);
    }

    void on_depth_event(){
        // if(!state.initialized) return;
        // Signal signal = strategy.on_market_update(state);

        // if(signal_queue.size() < 1000){
        //     signal_queue.push(signal);
        // }
        // recorder.log_snapshot(state.last_depth_ts, signal.value(), toupper(instrument));
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
        snap.market.best_bid = best_bid;
        snap.market.best_ask = best_ask;
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
        snap.risk.total_pnl = state.get_pnl(mid);

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
        auto on_trade_event = [this](const Trade& trade) {engine->on_trade_event(trade);};
        auto on_depth_event = [this]() {engine->on_depth_event();};
        auto log_event = [this](const std_string& type, const uint64_t& ts, const std_string& msg) {recorder.log_event(type, ts, msg);};
        auto log_orderbook_snapshot = [this](const json& snapshot) {recorder.log_orderbook_snapshot(snapshot);};
        
        if(exchange == "binance_spot" && mode != "replay"){
            feed = make_unique<BinanceSpotFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_futures" && mode != "replay"){
            feed = make_unique<BinanceFuturesFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_spot" && mode == "replay"){
            feed = make_unique<BinanceSpotReplayFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
        }

        else if(exchange == "binance_futures" && mode == "replay"){
            feed = make_unique<BinanceFuturesReplayFeed>(config, state, on_trade_event, on_depth_event, log_event, log_orderbook_snapshot, params);
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

        // Wait 5 seconds
        this_thread::sleep_for(seconds(5)); //FOR TESTING
        recorder.export_orderbook_snapshot();
        recorder.export_event_parquet();
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
                // // lock_guard<mutex>lock(signal_mtx);

                // if(signal_queue.empty()){
                //     this_thread::sleep_for(milliseconds(1));
                //     continue;
                // }

                // Signal signal = signal_queue.front();

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

    std_string path = "D:\\OneDrive\\Trading\\manifest_live.json";
    // std_string path = "D:\\OneDrive\\Trading\\manifest_replay.json";
    
    
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