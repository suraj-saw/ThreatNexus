@echo off
setlocal enabledelayedexpansion
title ThreatNexus - [MASTER NODE]
color 0B

echo ===================================================================
echo  ThreatNexus - Distributed Big Data Platform
echo  ROLE: [SYSTEM 1: MASTER NODE]
echo ===================================================================
echo.

cd /d "%~dp0"

:: 1. Detect Local IPv4 Address
for /f "tokens=*" %%a in ('powershell -Command "Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias 'Wi-Fi*','Ethernet*' | Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' } | Select-Object -ExpandProperty IPAddress -First 1"') do set DETECTED_IP=%%a

if "%DETECTED_IP%"=="" set DETECTED_IP=192.168.1.10

echo [Step 1/5] Network Configuration
echo Detected LAN IPv4 Address: %DETECTED_IP%
set /p MASTER_IP="Enter/Confirm Master Node LAN IP [%DETECTED_IP%]: "
if "%MASTER_IP%"=="" set MASTER_IP=%DETECTED_IP%

echo.
echo Configuring .env for Master Node with MASTER_IP=%MASTER_IP%...
(
    echo MASTER_IP=%MASTER_IP%
    echo KAFKA_BOOTSTRAP_SERVERS=%MASTER_IP%:9092
    echo KAFKA_TOPIC=security-events
    echo HBASE_HOST=hbase
    echo HBASE_THRIFT_PORT=9090
    echo HBASE_TABLE=threatnexus:alerts
    echo HDFS_OUTPUT_PATH=hdfs://%MASTER_IP%:9000/threatnexus/events
    echo SPARK_CHECKPOINT_BASE=hdfs://%MASTER_IP%:9000/threatnexus/checkpoints
    echo CORS_ORIGINS=*
    echo VITE_API_URL=http://%MASTER_IP%:8000
) > .env

echo.
echo [Step 2/5] Creating Docker Network (threatnexus-net)...
docker network create threatnexus-net 2>nul
echo Network ready.

echo.
echo [Step 3/5] Starting Master Stack Containers...
echo (ZooKeeper, Kafka, HDFS NameNode, Spark Master, HBase Master, API, UI)
docker compose -p threatnexus-master -f docker-compose.master.yml up -d --build
if errorlevel 1 (
    echo.
    echo [ERROR] Docker Compose failed to start Master stack.
    echo Make sure Docker Desktop is running.
    pause
    exit /b 1
)

echo.
echo [Step 4/5] Checking container status...
docker compose -p threatnexus-master -f docker-compose.master.yml ps

echo.
echo [Step 5/5] Waiting for Master services initialization...
for /L %%I in (1,1,15) do (
    curl.exe --fail --silent http://localhost:8000/health >nul 2>&1
    if not errorlevel 1 goto :master_ready
    echo Waiting for API, Kafka, and HBase... attempt %%I/15
    timeout /t 5 /nobreak >nul
)

:master_ready
echo.
echo ===================================================================
echo  [SUCCESS] ThreatNexus MASTER NODE is RUNNING!
echo ===================================================================
echo.
echo  Share this Master IP with Worker 1 and Worker 2 machines:
echo  >>> MASTER IP: %MASTER_IP% <<<
echo.
echo  ACCESS DASHBOARDS ON MASTER OR ANY LAN BROWSER:
echo  - React SOC Dashboard:   http://%MASTER_IP%:3000
echo  - Spark Master Web UI:   http://%MASTER_IP%:8080   (Inspect registered workers)
echo  - HDFS NameNode Web UI:  http://%MASTER_IP%:9870   (Inspect live DataNodes)
echo  - HBase Master Web UI:   http://%MASTER_IP%:16010  (Inspect live RegionServers)
echo  - FastAPI Swagger Docs:  http://%MASTER_IP%:8000/docs
echo ===================================================================
echo.
echo Open SOC Dashboard in browser now?
choice /C YN /M "Open browser"
if errorlevel 1 if not errorlevel 2 start http://%MASTER_IP%:3000

echo.
echo Keep this terminal open to maintain Master Node logs.
pause
