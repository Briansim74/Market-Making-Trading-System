set BUILD_DIR=C:\Users\brian\OneDrive\Trading\Market Making\build

call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"

cl ^
 /nologo ^
 /bigobj ^
 /std:c++latest ^
 /EHsc ^
 /Zi ^
 /MD ^
 /Fd"%BUILD_DIR%\vc140.pdb" ^
 /Fo"%BUILD_DIR%\\" ^
 "%~1" ^
 "C:\Users\brian\OneDrive\Trading\Market Making\external\simdjson\src\simdjson.cpp" ^
 /I"C:\Users\brian\OneDrive\Trading\Market Making\external\simdjson\include" ^
 /I"C:\Users\brian\OneDrive\Trading\Market Making\external\simdjson\src" ^
 /I"C:\Users\brian\OneDrive\Trading\Market Making\external\xgboost\include" ^
 /I"C:\Users\brian\vcpkg\installed\x64-windows\include" ^
 /DNOMINMAX ^
 /DWIN32_LEAN_AND_MEAN ^
 /D_WIN32_WINNT=0x0A00 ^
 /link ^
 /INCREMENTAL:NO ^
 /PDB:"%BUILD_DIR%\%~n1.pdb" ^
 /LIBPATH:"C:\Users\brian\OneDrive\Trading\Market Making\external\xgboost\lib" ^
 /LIBPATH:"C:\Users\brian\vcpkg\installed\x64-windows\lib" ^

 arrow.lib ^
 parquet.lib ^

 utf8proc.lib ^
 lz4.lib ^
 zstd.lib ^
 snappy.lib ^
 brotlicommon.lib ^
 brotlidec.lib ^
 brotlienc.lib ^

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