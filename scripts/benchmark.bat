@echo off
setlocal enabledelayedexpansion

if "%~1" equ ":main" (
    shift /1
    goto main
)

cmd /d /c "%~f0" :main %*
set ZIG_RESULT=%ERRORLEVEL%
taskkill /F /IM tigerbeetle.exe >nul

if !ZIG_RESULT! equ 0 (
    del /f benchmark.log
) else (
    echo.
    echo Error running benchmark, here are more details
    type benchmark.log
)

echo.
exit /b

:main
zig\zig.exe build -Drelease-safe
move zig-out\bin\tigerbeetle.exe . >nul

for /l %%i in (0, 1, 0) do (
    echo Initializing replica %%i
    set ZIG_FILE=.\cluster_0000000000_replica_00%%i.tigerbeetle
    if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
    .\tigerbeetle.exe init --directory=. --cluster=0 --replica=%%i > benchmark.log 2>&1
)

for /l %%i in (0, 1, 0) do (
    echo Starting replica %%i
    start /B "tigerbeetle_%%i" .\tigerbeetle.exe start --directory=. --cluster=0 --addresses=3001 --replica=%%i > benchmark.log 2>&1
)

rem Wait for replicas to start, listen and connect:
timeout /t 2

echo.
echo Benchmarking...
zig\zig.exe run -OReleaseSafe src\benchmark.zig
exit /b %errorlevel%
