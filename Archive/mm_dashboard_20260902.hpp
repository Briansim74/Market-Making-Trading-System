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
                row_text("Mid", format("{:<15.10f}", snap.market.mid)),
                row_text("Microprice", format("{:<15.10f}", snap.market.microprice)),
                row_text("Spread", format("{:<15.10f}", snap.market.spread)),
                row_text("Best Bid / Size", format("{:<10.10f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.market.bid_size) + ")"),
                row_text("Best Ask / Size", format("{:<10.10f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.market.bid_size) + ")"),
                row_text("EWMA Vol", format("{:.2e}", snap.market.ewma_vol)),
                row_text("Order Imbalance", format("{:<15.4f}", snap.market.order_imbalance)),
                row_text("Trade Imbalance", format("{:<15.4f}", snap.market.trade_imbalance)),
                row_text("Last Trade", snap.market.trade),
                row_text("", "")
            });

            auto regime = vbox({
                row_title("REGIME", ""),
                row_text("Regime", snap.regime.regime),
                row_text("Confidence", format("{:<15.2f}", snap.regime.confidence)),
                row_text("", "")
            });

            auto signals = vbox({
                row_title("SIGNALS", ""),
                row_text("Spread Multiplier", format("{:<15.4f}", snap.signals.spread_multiplier)),
                row_text("Inventory Target", format("{:<15.2f}", snap.signals.inventory_target)),
                row_text("Alpha Trade Imb", format("{:<15.2f}", snap.signals.alpha_trade_imb)),
                row_text("Alpha Struct", format("{:<15.2f}", snap.signals.alpha_struct)),
                row_text("Alpha Residual", format("{:<15.2f}", snap.signals.alpha_residual)),
                row_text("Fair Value", format("{:<15.10f}", snap.signals.fair)),
                row_text("Inventory Skew", format("{:<15.10f}", snap.signals.skew)),
                row_text("Residual Signal Quality", format("{:<15.2f}", snap.signals.residual_signal_quality)),
                row_text("Toxicity", format("{:<15.2f}", snap.signals.tox)),
                row_text("Reservation", format("{:<15.4f}", snap.signals.reservation)),
                
                row_text("", "")
            });
            
            auto quotes = vbox({
                row_title("QUOTES", ""),
                row_text("My Bid / Size", format("{:<10.10f}", snap.quotes.my_bid) + " (" + format("{:<6.4f}", snap.quotes.current_bid_size) + ")"),
                row_text("My Ask / Size", format("{:<10.10f}", snap.quotes.my_ask) + " (" + format("{:<6.4f}", snap.quotes.current_ask_size) + ")"),
                row_text("", "")
            });

            auto execution = vbox({
                row_title("EXECUTION", ""),
                row_text("Queue Ahead / Bid", format("{:<10.10f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.execution.bid_queue) + ")"),
                row_text("Queue Ahead / Ask", format("{:<10.10f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.execution.ask_queue) + ")"),
                row_text("Queue Pressure / Bid", format("{:<10.10f}", snap.market.best_bid) + " (" + format("{:<6.4f}", snap.execution.bid_pressure) + ")"),
                row_text("Queue Pressure / Ask", format("{:<10.10f}", snap.market.best_ask) + " (" + format("{:<6.4f}", snap.execution.ask_pressure) + ")"),
                row_text("Open Orders", snap.execution.buy_order),
                row_text("", snap.execution.sell_order),
                row_text("Last Fill Candidate", snap.execution.last_fill_candidate),
                row_text("Last Order Update", snap.execution.last_order_update),
                row_text("", "")
            });

            auto risk = vbox({
                row_title("RISK", ""),
                row_elem("Inventory", color_risk(snap.risk.inventory)),
                row_elem("Realized PnL", color_pnl(snap.risk.realized_pnl)),
                row_elem("Unrealized PnL", color_pnl(snap.risk.unrealized_pnl)),
                row_elem("Fees Paid", color_pnl(-snap.risk.fees_paid)),
                row_elem("Total PnL", color_pnl(snap.risk.total_pnl)),
                row_elem("Risk", centered_inventory_bar(snap.risk.inventory)),
                row_text("", "")
            });

            auto system = vbox({
                row_title("SYSTEM", ""),
                row_text("Time", snap.system.time),
                row_text("Last Trade ts", snap.system.last_trade_ts),
                row_text("Last Depth ts", snap.system.last_depth_ts),
                row_text("Trade Latency", format("{:<10}", snap.system.trade_latency)),
                row_text("Depth Latency", format("{:<10}", snap.system.depth_latency)),
                row_text("Exchange Latency", format("{:<10}", snap.system.exchange_latency)),
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
    }

    json snapshot_to_json(const Snapshot& snap){
        return {
            {"title", {
                {"header", snap.title.header},
                {"regime", snap.title.regime},
                {"pnl_pct", snap.title.pnl_pct}
            }},
            {"market", {
                {"mid", snap.market.mid},
                {"microprice", snap.market.microprice},
                {"spread", snap.market.spread},
                {"best_bid", snap.market.best_bid},
                {"best_ask", snap.market.best_ask},
                {"bid_size", snap.market.bid_size},
                {"ask_size", snap.market.ask_size},
                {"ewma_vol", snap.market.ewma_vol},
                {"order_imbalance", snap.market.order_imbalance},
                {"trade_imbalance", snap.market.trade_imbalance},
                {"trade", snap.market.trade},
            }},
            {"regime", {
                {"regime", snap.regime.regime},
                {"confidence", snap.regime.confidence},
            }},
            {"signals", {
                {"fair", snap.signals.fair},
                {"skew", snap.signals.skew},
                {"reservation", snap.signals.reservation},
                {"alpha_trade_imb", snap.signals.alpha_trade_imb},
                {"alpha_struct", snap.signals.alpha_struct},
                {"alpha_residual", snap.signals.alpha_residual},
                {"spread_multiplier", snap.signals.spread_multiplier},
                {"inventory_target", snap.signals.inventory_target},
                {"residual_signal_quality", snap.signals.residual_signal_quality},
                {"tox", snap.signals.tox},
                {"k_spread", snap.signals.k_spread},
                {"k_order_size", snap.signals.k_order_size},
            }},
            {"quotes", {
                {"my_bid", snap.quotes.my_bid},
                {"my_ask", snap.quotes.my_ask},
                {"current_bid_size", snap.quotes.current_bid_size},
                {"current_ask_size", snap.quotes.current_ask_size},
            }},
            {"execution", {
                {"bid_queue", snap.execution.bid_queue},
                {"ask_queue", snap.execution.ask_queue},
                {"bid_pressure", snap.execution.bid_pressure},
                {"ask_pressure", snap.execution.ask_pressure},
                {"buy_order", snap.execution.buy_order},
                {"sell_order", snap.execution.sell_order},
                {"last_fill_candidate", snap.execution.last_fill_candidate},
                {"last_order_update", snap.execution.last_order_update},
            }},
            {"risk", {
                {"inventory", snap.risk.inventory},
                {"realized_pnl", snap.risk.realized_pnl},
                {"unrealized_pnl", snap.risk.unrealized_pnl},
                {"fees_paid", snap.risk.fees_paid},
                {"total_pnl", snap.risk.total_pnl},
            }},
            {"system", {
                {"time", snap.system.time},
                {"last_trade_ts", snap.system.last_trade_ts},
                {"last_depth_ts", snap.system.last_depth_ts},
                {"trade_latency", snap.system.trade_latency},
                {"depth_latency", snap.system.depth_latency},
                {"exchange_latency", snap.system.exchange_latency},
            }}
        };
    }
};