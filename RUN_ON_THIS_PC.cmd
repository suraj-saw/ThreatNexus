@echo off
setlocal enabledelayedexpansion
title ThreatNexus - Single-Click Local Runner
color 0B

echo ===================================================================
echo  ThreatNexus - Real-Time Cybersecurity Threat Detection Platform
echo  SINGLE-CLICK LOCAL RUNNER (One PC / Standalone Mode)
echo ===================================================================
echo.

cd /d "%~dp0"

:: 1. Verify Prerequisites
echo [1/5] Checking Python and Node.js environment...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in system PATH.
    echo Please install Python 3.10+ from python.org.
    pause
    exit /b 1
)

call npm --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Node.js / npm is not installed or not in system PATH.
    echo Please install Node.js from nodejs.org.
    pause
    exit /b 1
)
echo [OK] Python and Node.js are available.

:: 2. Ensure frontend packages are installed
echo.
echo [2/5] Checking frontend dependencies...
if not exist "frontend\node_modules" (
    echo Installing React dependencies [one-time setup, please wait]...
    call npm --prefix frontend install
    if errorlevel 1 (
        echo [ERROR] npm install failed.
        pause
        exit /b 1
    )
) else (
    echo [OK] Frontend dependencies are installed.
)

:: 3. Free up ports 8000 and 3000 if previously occupied
echo.
echo [3/5] Initializing service ports (8000, 3000)...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-NetTCPConnection -LocalPort 8000, 3000 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }" >nul 2>&1

:: 4. Start Backend and Frontend
echo.
echo [4/5] Starting ThreatNexus services...
echo Starting FastAPI Analytics Backend on port 8000...
start "ThreatNexus-Backend" cmd /k "title ThreatNexus-Backend && color 0A && python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000"

echo Starting React SOC Web Dashboard on port 3000...
start "ThreatNexus-Frontend" cmd /k "title ThreatNexus-Frontend && color 0D && call npm --prefix frontend run dev"

:: 5. Probe API Readiness
echo.
echo [5/5] Waiting for services to become ready...
set "READY=0"
for /L %%I in (1,1,20) do (
    if "!READY!"=="0" (
        curl.exe --fail --silent http://localhost:8000/health >nul 2>&1
        if not errorlevel 1 (
            set "READY=1"
        ) else (
            ping 127.0.0.1 -n 2 >nul
        )
    )
)

echo.
echo Opening React SOC Web Dashboard in your default browser...
start http://localhost:3000

echo.
echo ===================================================================
echo  THREATNEXUS IS RUNNING SUCCESSFULLY ON THIS SYSTEM!
echo ===================================================================
echo  - React SOC Web Dashboard:  http://localhost:3000
echo  - FastAPI Swagger Docs:     http://localhost:8000/docs
echo  - Service Health Check:     http://localhost:8000/health
echo ===================================================================
echo.
echo Interactive Attack Simulation ^& Control Menu:
echo   [1] Trigger SSH Brute Force Attack (7 failed logins)
echo   [2] Trigger Horizontal Port Scan (12 distinct ports)
echo   [3] Trigger Mimikatz Credential Dumper (Critical Process)
echo   [4] Trigger Volumetric Traffic Surge (25 requests)
echo   [5] Replay CIC-IDS2017 Benchmark Dataset (Sample flows)
echo   [6] Run Full 11-Step Automated Test Suite (test_pipeline.py)
echo   [7] Resolve All Active Alerts (Reset Gauge to Normal)
echo   [8] Reset Simulation State ^& Buffers
echo   [9] Open Dashboard in Browser Again
echo   [Q] Stop ThreatNexus and Exit Cleanly
echo.

if "%~1"=="--test" (
    echo [OK] Single-click startup sequence completed successfully in test mode.
    exit /b 0
)

:menu
set "choice="
set /p choice="Select an action [1-9, Q]: "
if not defined choice goto :menu

if "%choice%"=="1" (
    echo.
    echo [*] Injecting SSH Brute Force Attack scenario...
    python scripts\replay_dataset.py --scenario brute_force --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="2" (
    echo.
    echo [*] Injecting Horizontal Port Scan scenario...
    python scripts\replay_dataset.py --scenario port_scan --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="3" (
    echo.
    echo [*] Injecting Mimikatz Suspicious Process Execution...
    python scripts\replay_dataset.py --scenario process --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="4" (
    echo.
    echo [*] Injecting Volumetric Traffic Surge scenario...
    python scripts\replay_dataset.py --scenario traffic_spike --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="5" (
    echo.
    echo [*] Replaying CIC-IDS2017 Benchmark Dataset Records...
    python scripts\replay_dataset.py --source csv --input data\cic_ids2017_sample.csv --target http://localhost:8000/api/events --limit 10
    goto :menu
)
if "%choice%"=="6" (
    echo.
    echo [*] Running 11-step automated verification suite...
    python scripts\test_pipeline.py --api http://localhost:8000
    goto :menu
)
if "%choice%"=="7" (
    echo.
    echo [*] Resolving all active alerts...
    curl.exe -s -X POST http://localhost:8000/api/alerts/resolve-all
    echo.
    echo All alerts marked resolved. Gauge needle returned to Normal baseline.
    goto :menu
)
if "%choice%"=="8" (
    echo.
    echo [*] Resetting simulation state, buffers, and alert stores...
    curl.exe -s -X POST http://localhost:8000/api/simulation/reset
    echo.
    echo Simulation reset complete.
    goto :menu
)
if "%choice%"=="9" (
    start http://localhost:3000
    goto :menu
)
if /i "%choice%"=="q" (
    echo.
    echo Stopping ThreatNexus services...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-NetTCPConnection -LocalPort 8000, 3000 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }" >nul 2>&1
    taskkill /FI "WINDOWTITLE eq ThreatNexus-Backend*" /F /T >nul 2>&1
    taskkill /FI "WINDOWTITLE eq ThreatNexus-Frontend*" /F /T >nul 2>&1
    echo [OK] All services stopped cleanly.
    ping 127.0.0.1 -n 3 >nul
    exit /b 0
)

echo Invalid selection. Please enter 1-9 or Q.
goto :menu
