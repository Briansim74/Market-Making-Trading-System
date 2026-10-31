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

class SnapshotStore {
public:
    mutex mtx;
    Snapshot latest;

    void set(Snapshot snap){
        lock_guard<mutex> lock(mtx);
        latest = move(snap);
    }

    Snapshot get(){
        lock_guard<mutex> lock(mtx);
        return latest;
    }
};

class DashboardTerminal {
public:
    SnapshotStore& snapshot_store;
    ScreenInteractive screen = ScreenInteractive::Fullscreen();
    thread terminal_thread;

    DashboardTerminal(SnapshotStore& snapshot_store) : snapshot_store(snapshot_store) {}

    void start(){
        terminal_thread = thread([this](){
            Component ui = Dashboard();
            screen.Loop(ui);
        });
    }

    void refresh(){
        screen.PostEvent(Event::Custom);
    }

    void stop(){
        screen.ExitLoopClosure()();

        if(terminal_thread.joinable()) terminal_thread.join();
    }

    Element color_pnl(const double& value){
        if(value > 0) return text("▲ " + format("{:.10f}", value)) | color(Color::Green);
        else if(value < 0) return text("▼ " + format("{:.10f}", abs(value))) | color(Color::Red);
        else return text(format("{:.10f}", value));
    }

    Element color_risk(const double& inventory, const double& limit = 10.0) {
        double intensity = min(abs(inventory) / limit, 1.0);
        auto value = text(format("{:.4f}", inventory));

        if(intensity < 0.3) return value | color(Color::Green);
        else if(intensity < 0.7) return hbox({text("▲ "),  value}) | color(Color::Yellow);
        else return hbox({text("▲ "), value}) | color(Color::Red);
    }

    Element centered_inventory_bar(const double& inv, const double& max_inv = 10.0, const int& width = 21){
        int half = width / 2;
        int scaled = static_cast<int>((inv / max_inv) * half);
        scaled = clamp(scaled, -half, half);

        std_string left, right;

        for(int i = 0; i < half; i++){
            left += ((half - i - 1) < -scaled ? "█" : " ");
            right += (i < scaled ? "█" : " ");
        }

        return hbox({text(left) | color(Color::Red), text("|"), text(right) | color(Color::Green)});
    }

    Component Dashboard(){

        auto row_title = [](const std_string& title, const std_string& value){
            return hbox({
                text(" "), text(move(title)) | bold | color(Color::Yellow) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text(move(value)) | color(Color::GrayLight)
            });
        };
        
        auto row_text = [](const std_string& title, const std_string& value){
            return hbox({
                text(" "), text(move(title)) | color(Color::CyanLight) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text(move(value)) | color(Color::GrayLight)
            });
        };

        auto row_elem = [](const std_string& title, const ftxui::Element& value){
            return hbox({
                text(" "), text(move(title)) | color(Color::CyanLight) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), move(value) | color(Color::GrayLight)
            });
        };

        auto make_title = [](const Snapshot& snap){
            vector<std_string> parts;

            auto add = [&](const std_string& s){
                if(!s.empty()) parts.push_back(s);
            };

            stringstream ss(snap.title.header);
            std_string item;

            while(getline(ss, item, '|')){
                // trim spaces
                item.erase(0, item.find_first_not_of(" "));
                item.erase(item.find_last_not_of(" ") + 1);
                add(item);
            }

            ftxui::Elements rendered;

            for(size_t i = 0; i < parts.size(); i++){
                if(i > 0) rendered.push_back(text(" | ") | dim);
                rendered.push_back(text(parts[i]) | italic);
            }

            rendered.push_back(text(" | ") | dim);
            rendered.push_back(text(snap.title.regime) | italic);
            rendered.push_back(text(" | ") | dim);
            rendered.push_back(text("pnl=") | italic);
            rendered.push_back(text((snap.title.pnl_pct > 0 ? "+" : "") + format("{:.4f}", snap.title.pnl_pct) + "%") | italic);

            return hbox(move(rendered)) | color(Color::GrayLight) | center;
        };

        return ftxui::Renderer([&]{
            Snapshot snap = snapshot_store.get();

            auto title = make_title(snap);

            auto header = hbox({
                text(" "), text("Metric") | bold | color(Color::White) | size(WIDTH, EQUAL, 30),
                separator(), text(" "), text("Value") | bold | color(Color::White)
            });

            auto market = vbox({
                row_title("MARKET", ""),
                row_text("Mid", format("{:<15.10f}", snap.maker.market.mid)),
                row_text("Microprice", format("{:<15.10f}", snap.maker.market.microprice)),
                row_text("Spread", format("{:<15.10f}", snap.maker.market.spread)),
                row_text("Best Bid / Size", format("{:<10.10f}", snap.maker.market.best_bid) + " (" + format("{:<6.4f}", snap.maker.market.bid_size) + ")"),
                row_text("Best Ask / Size", format("{:<10.10f}", snap.maker.market.best_ask) + " (" + format("{:<6.4f}", snap.maker.market.bid_size) + ")"),
                row_text("EWMA Vol", format("{:.2e}", snap.maker.market.ewma_vol)),
                row_text("Order Imbalance", format("{:<15.4f}", snap.maker.market.order_imbalance)),
                row_text("Trade Imbalance", format("{:<15.4f}", snap.maker.market.trade_imbalance)),
                row_text("Last Trade", snap.maker.market.trade),
                row_text("", "")
            });

            auto regime = vbox({
                row_title("REGIME", ""),
                row_text("Regime", snap.maker.regime.regime),
                row_text("Confidence", format("{:<15.2f}", snap.maker.regime.confidence)),
                row_text("", "")
            });

            auto signals = vbox({
                row_title("SIGNALS", ""),
                row_text("Spread Multiplier", format("{:<15.4f}", snap.maker.signals.spread_multiplier)),
                row_text("Inventory Target", format("{:<15.2f}", snap.maker.signals.inventory_target)),
                row_text("Alpha Trade Imb", format("{:<15.2f}", snap.maker.signals.alpha_trade_imb)),
                row_text("Alpha Struct", format("{:<15.2f}", snap.maker.signals.alpha_struct)),
                row_text("Alpha Residual", format("{:<15.2f}", snap.maker.signals.alpha_residual)),
                row_text("Fair Value", format("{:<15.10f}", snap.maker.signals.fair)),
                row_text("Inventory Skew", format("{:<15.10f}", snap.maker.signals.skew)),
                row_text("Toxicity", format("{:<15.10f}", snap.maker.signals.tox)),
                row_text("Reservation", format("{:<15.4f}", snap.maker.signals.reservation)),
                row_text("Residual Signal Quality", format("{:<15.2f}", snap.maker.signals.residual_signal_quality)),
                row_text("Toxicity Signal Quality", format("{:<15.2f}", snap.maker.signals.toxicity_signal_quality)),
                row_text("", "")
            });
            
            auto quotes = vbox({
                row_title("QUOTES", ""),
                row_text("My Spread", format("{:<10.10f}", snap.maker.quotes.my_spread)),
                row_text("My Bid / Size", format("{:<10.10f}", snap.maker.quotes.my_bid) + " (" + format("{:<6.4f}", snap.maker.quotes.current_bid_size) + ")"),
                row_text("My Ask / Size", format("{:<10.10f}", snap.maker.quotes.my_ask) + " (" + format("{:<6.4f}", snap.maker.quotes.current_ask_size) + ")"),
                row_text("", "")
            });

            auto execution = vbox({
                row_title("EXECUTION", ""),
                row_text("Queue Ahead / Bid", format("{:<10.10f}", snap.maker.market.best_bid) + " (" + format("{:<6.4f}", snap.maker.execution.bid_queue) + ")"),
                row_text("Queue Ahead / Ask", format("{:<10.10f}", snap.maker.market.best_ask) + " (" + format("{:<6.4f}", snap.maker.execution.ask_queue) + ")"),
                row_text("Queue Pressure / Bid", format("{:<10.10f}", snap.maker.market.best_bid) + " (" + format("{:<6.4f}", snap.maker.execution.bid_pressure) + ")"),
                row_text("Queue Pressure / Ask", format("{:<10.10f}", snap.maker.market.best_ask) + " (" + format("{:<6.4f}", snap.maker.execution.ask_pressure) + ")"),
                row_text("Open Orders", snap.maker.execution.buy_order),
                row_text("", snap.maker.execution.sell_order),
                row_text("Last Fill Candidate", snap.maker.execution.last_fill_candidate),
                row_text("Last Order Update", snap.maker.execution.last_order_update),
                row_text("", "")
            });

            auto risk = vbox({
                row_title("RISK", ""),
                row_elem("Inventory", color_risk(snap.maker.risk.inventory)),
                row_elem("Realized PnL", color_pnl(snap.maker.risk.realized_pnl)),
                row_elem("Unrealized PnL", color_pnl(snap.maker.risk.unrealized_pnl)),
                row_elem("Fees Paid", color_pnl(-snap.maker.risk.fees_paid)),
                row_elem("Total PnL", color_pnl(snap.maker.risk.total_pnl)),
                row_elem("Risk", centered_inventory_bar(snap.maker.risk.inventory)),
                row_text("", "")
            });

            auto system = vbox({
                row_title("SYSTEM", ""),
                row_text("Time", snap.maker.system.time),
                row_text("Last Trade ts", snap.maker.system.last_trade_ts),
                row_text("Last Depth ts", snap.maker.system.last_depth_ts),
                row_text("Trade Latency", format("{:<10}", snap.maker.system.trade_latency)),
                row_text("Depth Latency", format("{:<10}", snap.maker.system.depth_latency)),
                row_text("Exchange Latency", format("{:<10}", snap.maker.system.exchange_latency)),
                row_text("", "")
            });

            auto content = vbox({title, separator(), header, separator(), market, regime, signals, 
                    quotes, execution, risk, system, separator()});
        
            return content | border | color(Color::GrayLight);
        });
    };
};

class WsSession : public enable_shared_from_this<WsSession> {
public:
    websocket::stream<tcp::socket> ws; 
    boost::beast::flat_buffer buffer; 
    mutex write_mtx;
    
    explicit WsSession(tcp::socket socket) : ws(move(socket)) {}
        
    void run(){
        auto self = shared_from_this();
        ws.set_option(websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

        ws.async_accept([self](boost::system::error_code ec){
            if(ec) return;

            self->ws.text(true);
            cout << "DASHBOARD CLIENT CONNECTED\n";
        });
    }
    
    bool send(const std_string& msg){
        lock_guard<mutex> lock(write_mtx);
        boost::system::error_code ec;
        ws.write(boost::asio::buffer(msg), ec);

        if(ec) return false;  // IMPORTANT: signal failure instead of throwing
        return true;
    }
    
    void close(){ 
        boost::system::error_code ec; 
        ws.close(websocket::close_code::normal, ec); 
    }
};

class DashboardServer {
public:
    MarketConfig& config;
    SnapshotStore& snapshot_store; 

    boost::asio::io_context ioc; 
    tcp::acceptor acceptor; 

    set<shared_ptr<WsSession>> clients;
    mutex dash_mtx;
    thread server_thread;

    atomic<bool> running{false}; // keeps io_context alive 
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard; 
    
    DashboardServer(MarketConfig& config, SnapshotStore& snapshot_store)
        : config(config), snapshot_store(snapshot_store), acceptor(ioc), guard(boost::asio::make_work_guard(ioc)) {}

    void start(){
        running = true;
        tcp::endpoint endpoint(boost::asio::ip::make_address(config.host), config.port); 
        
        acceptor.open(endpoint.protocol());
        acceptor.set_option(tcp::acceptor::reuse_address(true));
        acceptor.bind(endpoint);
        acceptor.listen();
        
        server_thread = thread([this](){
            cout << "WS RUNNING ON http://localhost:" << config.port << "\n";
            do_accept();
            ioc.run();
        });
    }

    void do_accept(){ 
        acceptor.async_accept([this](boost::system::error_code ec, tcp::socket socket){
            if(!ec){
                auto session = make_shared<WsSession>(move(socket));
                {
                    lock_guard<mutex> lock(dash_mtx);
                    clients.insert(session);
                } // IMPORTANT: must run in same strand context
                session->run();
            }
            else if(running){
                // only print errors when server is actually running
                cout << "ACCEPT ERROR: " << ec.message() << "\n";
            }
            if(running) do_accept();
        });
    }

    void publish(){
        json event = {
            {"type","snapshot"},
            {"data", snapshot_to_json(snapshot_store.get())}
        };
        std_string msg = event.dump();

        vector<shared_ptr<WsSession>> snapshot_clients;
        {
            lock_guard<mutex> lock(dash_mtx);
            snapshot_clients.assign(clients.begin(), clients.end());
        }

        for(auto& client: snapshot_clients){
            bool ok = client->send(msg);

            if(!ok){
                client->close();
                lock_guard<mutex> lock(dash_mtx);
                clients.erase(client);
            }
        }
    }

    void stop(){ 
        running = false; 
        boost::system::error_code ec; 
        acceptor.close(ec);
        ioc.stop();
        if(server_thread.joinable()) server_thread.join();
        cout << "DASH STOPPED\n";
    }

    json snapshot_to_json(const Snapshot& snap){
        return {
            {"title", {
                {"header", snap.title.header},
                {"regime", snap.title.regime},
                {"pnl_pct", snap.title.pnl_pct}
            }},
            {"maker", {
                {"market", {
                    {"instrument", snap.maker.market.instrument},
                    {"mid", snap.maker.market.mid},
                    {"microprice", snap.maker.market.microprice},
                    {"spread", snap.maker.market.spread},
                    {"best_bid", snap.maker.market.best_bid},
                    {"best_ask", snap.maker.market.best_ask},
                    {"bid_size", snap.maker.market.bid_size},
                    {"ask_size", snap.maker.market.ask_size},
                    {"ewma_vol", snap.maker.market.ewma_vol},
                    {"order_imbalance", snap.maker.market.order_imbalance},
                    {"trade_imbalance", snap.maker.market.trade_imbalance},
                    {"trade", snap.maker.market.trade},
                }},
                {"regime", {
                    {"regime", snap.maker.regime.regime},
                    {"confidence", snap.maker.regime.confidence},
                }},
                {"signals", {
                    {"fair", snap.maker.signals.fair},
                    {"skew", snap.maker.signals.skew},
                    {"reservation", snap.maker.signals.reservation},
                    {"alpha_trade_imb", snap.maker.signals.alpha_trade_imb},
                    {"alpha_struct", snap.maker.signals.alpha_struct},
                    {"alpha_residual", snap.maker.signals.alpha_residual},
                    {"spread_multiplier", snap.maker.signals.spread_multiplier},
                    {"inventory_target", snap.maker.signals.inventory_target},
                    {"tox", snap.maker.signals.tox},
                    {"k_spread", snap.maker.signals.k_spread},
                    {"k_order_size", snap.maker.signals.k_order_size},
                    {"residual_signal_quality", snap.maker.signals.residual_signal_quality},
                    {"toxicity_signal_quality", snap.maker.signals.toxicity_signal_quality},
                }},
                {"quotes", {
                    {"my_spread", snap.maker.quotes.my_spread},
                    {"my_bid", snap.maker.quotes.my_bid},
                    {"my_ask", snap.maker.quotes.my_ask},
                    {"current_bid_size", snap.maker.quotes.current_bid_size},
                    {"current_ask_size", snap.maker.quotes.current_ask_size},
                }},
                {"execution", {
                    {"bid_queue", snap.maker.execution.bid_queue},
                    {"ask_queue", snap.maker.execution.ask_queue},
                    {"bid_pressure", snap.maker.execution.bid_pressure},
                    {"ask_pressure", snap.maker.execution.ask_pressure},
                    {"buy_order", snap.maker.execution.buy_order},
                    {"sell_order", snap.maker.execution.sell_order},
                    {"last_fill_candidate", snap.maker.execution.last_fill_candidate},
                    {"last_order_update", snap.maker.execution.last_order_update},
                }},
                {"risk", {
                    {"inventory", snap.maker.risk.inventory},
                    {"realized_pnl", snap.maker.risk.realized_pnl},
                    {"unrealized_pnl", snap.maker.risk.unrealized_pnl},
                    {"fees_paid", snap.maker.risk.fees_paid},
                    {"total_pnl", snap.maker.risk.total_pnl},
                }},
                {"system", {
                    {"time", snap.maker.system.time},
                    {"last_trade_ts", snap.maker.system.last_trade_ts},
                    {"last_depth_ts", snap.maker.system.last_depth_ts},
                    {"trade_latency", snap.maker.system.trade_latency},
                    {"depth_latency", snap.maker.system.depth_latency},
                    {"exchange_latency", snap.maker.system.exchange_latency},
                }}
            }},
            {"hedge", {
                {"signals1", {
                    {"maker_delta", snap.hedge.signals1.maker_delta},
                    {"hedge_delta", snap.hedge.signals1.hedge_delta},
                    {"net_delta", snap.hedge.signals1.net_delta},
                    {"target_hedge_ratio", snap.hedge.signals1.target_hedge_ratio},
                    {"target_hedge", snap.hedge.signals1.target_hedge},
                    {"hedge_decision", snap.hedge.signals1.hedge_decision},
                    {"target_hedge", snap.hedge.signals1.target_hedge},
                }},
                {"market", {
                    {"instrument", snap.hedge.market.instrument},
                    {"mid", snap.hedge.market.mid},
                    {"microprice", snap.hedge.market.microprice},
                    {"spread", snap.hedge.market.spread},
                    {"best_bid", snap.hedge.market.best_bid},
                    {"best_ask", snap.hedge.market.best_ask},
                    {"bid_size", snap.hedge.market.bid_size},
                    {"ask_size", snap.hedge.market.ask_size},
                    {"ewma_vol", snap.hedge.market.ewma_vol},
                    {"order_imbalance", snap.hedge.market.order_imbalance},
                    {"trade_imbalance", snap.hedge.market.trade_imbalance},
                    {"trade", snap.hedge.market.trade},

                    {"basis", snap.hedge.market.basis},
                    {"funding_rate", snap.hedge.market.funding_rate},
                    {"next_funding_ts", snap.hedge.market.next_funding_ts},
                }},
                {"regime", {
                    {"regime", snap.hedge.regime.regime},
                    {"confidence", snap.hedge.regime.confidence},
                }},
                {"signals", {
                    {"fair", snap.hedge.signals.fair},
                    {"skew", snap.hedge.signals.skew},
                    {"reservation", snap.hedge.signals.reservation},
                    {"alpha_trade_imb", snap.hedge.signals.alpha_trade_imb},
                    {"alpha_struct", snap.hedge.signals.alpha_struct},
                    {"alpha_residual", snap.hedge.signals.alpha_residual},
                    {"spread_multiplier", snap.hedge.signals.spread_multiplier},
                    {"inventory_target", snap.hedge.signals.inventory_target},
                    {"tox", snap.hedge.signals.tox},
                    {"k_spread", snap.hedge.signals.k_spread},
                    {"k_order_size", snap.hedge.signals.k_order_size},
                    {"residual_signal_quality", snap.hedge.signals.residual_signal_quality},
                    {"toxicity_signal_quality", snap.hedge.signals.toxicity_signal_quality},
                }},
                {"quotes", {
                    {"my_spread", snap.hedge.quotes.my_spread},
                    {"my_bid", snap.hedge.quotes.my_bid},
                    {"my_ask", snap.hedge.quotes.my_ask},
                    {"current_bid_size", snap.hedge.quotes.current_bid_size},
                    {"current_ask_size", snap.hedge.quotes.current_ask_size},
                }},
                {"execution", {
                    {"bid_queue", snap.hedge.execution.bid_queue},
                    {"ask_queue", snap.hedge.execution.ask_queue},
                    {"bid_pressure", snap.hedge.execution.bid_pressure},
                    {"ask_pressure", snap.hedge.execution.ask_pressure},
                    {"buy_order", snap.hedge.execution.buy_order},
                    {"sell_order", snap.hedge.execution.sell_order},
                    {"last_fill_candidate", snap.hedge.execution.last_fill_candidate},
                    {"last_order_update", snap.hedge.execution.last_order_update},
                }},
                {"risk", {
                    {"inventory", snap.hedge.risk.inventory},
                    {"realized_pnl", snap.hedge.risk.realized_pnl},
                    {"unrealized_pnl", snap.hedge.risk.unrealized_pnl},
                    {"fees_paid", snap.hedge.risk.fees_paid},
                    {"total_pnl", snap.hedge.risk.total_pnl},
                }},
                {"system", {
                    {"time", snap.hedge.system.time},
                    {"last_trade_ts", snap.hedge.system.last_trade_ts},
                    {"last_depth_ts", snap.hedge.system.last_depth_ts},
                    {"trade_latency", snap.hedge.system.trade_latency},
                    {"depth_latency", snap.hedge.system.depth_latency},
                    {"exchange_latency", snap.hedge.system.exchange_latency},
                }}
            }}
        };
    }
};