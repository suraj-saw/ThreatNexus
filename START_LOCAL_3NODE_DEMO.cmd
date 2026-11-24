@echo off
setlocal
title ThreatNexus - [3-NODE CLUSTER DEMONSTRATION LAUNCHER]
color 0B

echo ===================================================================
echo  ThreatNexus - Distributed Big Data & Threat Detection Platform
echo  SINGLE-PC MULTI-TERMINAL CLUSTER SIMULATION LAUNCHER
echo  (1 Master Node + 2 Distributed Worker Nodes)
echo ===================================================================
echo.

cd /d "%~dp0"

echo [1/4] Spawning Window 1: MASTER NODE (FastAPI + React SOC Dashboard)...
start "ThreatNexus - [MASTER NODE]" cmd /k "title ThreatNexus - [MASTER NODE] && color 0B && python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000"

echo [2/4] Spawning Window 2: WORKER NODE 1 (HDFS DataNode 1 + Spark Worker 1)...
start "ThreatNexus - [WORKER NODE 1]" cmd /k "title ThreatNexus - [WORKER NODE 1] && color 0A && python scripts\simulate_worker.py --worker worker1 --cores 2 --memory 2G"

echo [3/4] Spawning Window 3: WORKER NODE 2 (HDFS DataNode 2 + Spark Worker 2)...
start "ThreatNexus - [WORKER NODE 2]" cmd /k "title ThreatNexus - [WORKER NODE 2] && color 0E && python scripts\simulate_worker.py --worker worker2 --cores 2 --memory 2G"

echo [4/4] Starting React SOC Frontend Dashboard (Port 3000)...
start "ThreatNexus - [VITE FRONTEND]" cmd /k "title ThreatNexus - [REACT SOC DASHBOARD] && color 0D && npm --prefix frontend run dev"

echo.
echo Waiting 5 seconds for services to initialize...
timeout /t 5 /nobreak >nul

echo Opening React SOC Web Dashboard in browser...
start http://localhost:3000

echo.
echo ===================================================================
echo  ALL 3 CLUSTER NODES ARE RUNNING IN SEPARATE TERMINAL WINDOWS!
echo ===================================================================
echo  - Window 1 (Cyan):   Master Node (FastAPI REST API + Kafka / HBase)
echo  - Window 2 (Green):  Worker Node 1 (HDFS DataNode 1 + Spark Worker 1)
echo  - Window 3 (Yellow): Worker Node 2 (HDFS DataNode 2 + Spark Worker 2)
echo  - Browser (SOC):     http://localhost:3000
echo ===================================================================
echo.
echo Interactive Command Menu:
echo   [1] Trigger SSH Brute Force Attack (7 failed logins)
echo   [2] Trigger Horizontal Port Scan (12 distinct ports)
echo   [3] Trigger Mimikatz Credential Dumper (Critical Process)
echo   [4] Trigger Traffic Rate Surge (25 requests)
echo   [5] Replay CIC-IDS2017 Benchmark Dataset Flow Records
echo   [6] Run Full Automated 11-Test Verification Suite
echo   [7] Resolve All Active Alerts (Reset Gauge to Normal)
echo   [Q] Quit / Close Launcher
echo.

:menu
set /p choice="Select an action [1-7, Q]: "

if "%choice%"=="1" (
    echo.
    echo [*] Injecting SSH Brute Force Attack into Master Node...
    python scripts\replay_dataset.py --scenario brute_force --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="2" (
    echo.
    echo [*] Injecting Horizontal Port Scan into Master Node...
    python scripts\replay_dataset.py --scenario port_scan --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="3" (
    echo.
    echo [*] Injecting Mimikatz Suspicious Process into Master Node...
    python scripts\replay_dataset.py --scenario process --target http://localhost:8000/api/events
    goto :menu
)
if "%choice%"=="4" (
    echo.
    echo [*] Injecting Traffic Surge into Master Node...
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
    echo [*] Executing Full 11-Step Automated Test Suite...
    python scripts\test_pipeline.py --api http://localhost:8000
    goto :menu
)
if "%choice%"=="7" (
    echo.
    echo [*] Resolving all active alerts...
    curl.exe -s -X POST http://localhost:8000/api/alerts/resolve-all
    echo.
    echo Alerts resolved. Gauge needle returned to Normal baseline.
    goto :menu
)
if /i "%choice%"=="q" (
    echo Exiting launcher...
    exit /b 0
)

echo Invalid choice. Please enter 1-7 or Q.
goto :menu
