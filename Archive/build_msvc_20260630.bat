call "D:\VSS Code\Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"

cl ^
 /nologo ^
 /std:c++latest ^
 /EHsc ^
 /Zi ^
 /MD ^
 "%~1" ^
 "D:\OneDrive\Trading\Market Making\external\simdjson\src\simdjson.cpp" ^
 /I"D:\OneDrive\Trading\Market Making\external\simdjson\include" ^
 /I"D:\OneDrive\Trading\Market Making\external\simdjson\src" ^
 /I"D:\OneDrive\Trading\Market Making\external\xgboost\include" ^
 /I"D:\vcpkg\installed\x64-windows\include" ^
 /DNOMINMAX ^
 /DWIN32_LEAN_AND_MEAN ^
 /D_WIN32_WINNT=0x0A00 ^
 /link ^
 /LIBPATH:"D:\OneDrive\Trading\Market Making\external\xgboost\lib" ^
 /LIBPATH:"D:\vcpkg\installed\x64-windows\lib" ^
 xgboost.lib ^
 libcurl.lib ^
 cpr.lib ^
 ftxui-component.lib ^
 ftxui-dom.lib ^
 ftxui-screen.lib ^
 ws2_32.lib ^
 mswsock.lib ^
 libssl.lib ^
 libcrypto.lib ^
 /OUT:"%~dpn1.exe"