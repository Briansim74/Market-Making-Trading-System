#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/component/component.hpp>

using namespace ftxui;

int main() {
    auto screen = ScreenInteractive::TerminalOutput();

    auto renderer = Renderer([&] {
        return text("Hello FTXUI!") | border;
    });

    screen.Loop(renderer);
}

g+ +mmpy_mini.cpp \
-I "D:/OneDrive/Trading/Market Making/FTXUI/include" \
-I "D:/OneDrive/Trading/Market Making/xgboost/include" \
-L "D:/OneDrive/Trading/Market Making/FTXUI/build" \
-L "D:/OneDrive/Trading/Market Making/xgboost/lib" \
-lftxui-component -lftxui-dom -lftxui-screen \
-lxgboost \
-lcurl \
-lsimdjson \
-lssl -lcrypto \
-lws2_32 -lmswsock \
-o mmpy_mini.exe