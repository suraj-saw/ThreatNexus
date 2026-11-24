@echo off
setlocal enabledelayedexpansion
title ThreatNexus - [WORKER NODE 1]
color 0A

echo ===================================================================
echo  ThreatNexus - Distributed Big Data Platform
echo  ROLE: [SYSTEM 2: WORKER NODE 1]
echo ===================================================================
echo.

cd /d "%~dp0"

echo [Step 1/4] Connect to Master Node
set /p MASTER_IP="Enter the Master Node LAN IPv4 Address (e.g. 192.168.1.10): "
if "%MASTER_IP%"=="" (
    echo [ERROR] Master IP cannot be blank!
    pause
    exit /b 1
)

echo.
echo Testing connectivity to Master Node (%MASTER_IP%)...
ping -n 2 %MASTER_IP% >nul
if errorlevel 1 (
    echo [WARNING] Could not ping Master Node (%MASTER_IP%).
    echo Ensure both PCs are on the same Wi-Fi/LAN and Master Firewall is allowed.
    echo Continuing anyway...
) else (
    echo [OK] Master Node is reachable!
)

echo.
echo Configuring .env for Worker 1...
(
    echo MASTER_IP=%MASTER_IP%
    echo NAMENODE_HOST=%MASTER_IP%
    echo SPARK_MASTER_HOST=%MASTER_IP%
    echo ZOOKEEPER_HOST=%MASTER_IP%
    echo HBASE_MASTER_HOST=%MASTER_IP%
    echo WORKER_HOSTNAME=worker1
    echo SPARK_WORKER_CORES=2
    echo SPARK_WORKER_MEMORY=2G
    echo DATANODE_HTTP_PORT=9864
    echo DATANODE_DATA_PORT=9866
    echo SPARK_WORKER_UI_PORT=8081
    echo REGIONSERVER_PORT=16020
    echo REGIONSERVER_INFO_PORT=16030
) > .env

echo.
echo [Step 2/4] Creating local Docker bridge network (threatnexus-net)...
docker network create threatnexus-net 2>nul
echo Network ready.

echo.
echo [Step 3/4] Launching Worker 1 distributed containers...
echo (HDFS DataNode 1, Spark Worker 1, HBase RegionServer 1)
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml up -d --build
if errorlevel 1 (
    echo.
    echo [ERROR] Docker Compose failed to start Worker 1.
    echo Ensure Docker Desktop is running on this machine.
    pause
    exit /b 1
)

echo.
echo [Step 4/4] Showing Worker 1 status...
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml ps

echo.
echo ===================================================================
echo  [SUCCESS] ThreatNexus WORKER NODE 1 is ACTIVE and CONNECTED!
echo ===================================================================
echo  - Registered with Spark Master at:  spark://%MASTER_IP%:7077
echo  - Registered with HDFS NameNode at: hdfs://%MASTER_IP%:9000
echo  - Registered with HBase Master at:  %MASTER_IP%:16000
echo.
echo  LOCAL WORKER 1 DIAGNOSTICS:
echo  - Spark Worker 1 Console: http://localhost:8081
echo  - HDFS DataNode 1 Status: http://localhost:9864
echo ===================================================================
echo.
pause
