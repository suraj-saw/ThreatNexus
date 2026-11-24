@echo off
setlocal enabledelayedexpansion
title ThreatNexus - [3-NODE CLUSTER VERIFICATION]
color 0F

echo ===================================================================
echo  ThreatNexus - 3-Node Distributed Cluster Verification Suite
echo ===================================================================
echo.

cd /d "%~dp0"

set /p MASTER_IP="Enter Master Node IP (press Enter for localhost) [localhost]: "
if "%MASTER_IP%"=="" set MASTER_IP=localhost

echo.
echo ===================================================================
echo [1/4] Checking HDFS NameNode Distributed Storage (Target: 2 Live DataNodes)
echo ===================================================================
docker compose -p threatnexus-master -f docker-compose.master.yml exec namenode hdfs dfsadmin -report 2>nul
if errorlevel 1 (
    echo Note: HDFS CLI query via container skipped or running remotely.
    echo Check HDFS Web UI in browser: http://%MASTER_IP%:9870/dfshealth.html#tab-datanode
)

echo.
echo ===================================================================
echo [2/4] Checking Spark Master Cluster (Target: 2 Registered Workers)
echo ===================================================================
curl.exe -s http://%MASTER_IP%:8080/json/ | python -m json.tool | findstr /i "workers aliveworkers cores memory"
echo View full Spark cluster topology: http://%MASTER_IP%:8080

echo.
echo ===================================================================
echo [3/4] Checking HBase Master & RegionServers (Target: 2 RegionServers)
echo ===================================================================
docker compose -p threatnexus-master -f docker-compose.master.yml exec hbase-master bash -c "printf 'status\nlist\n' | /usr/local/hbase/bin/hbase shell -n" 2>nul
if errorlevel 1 (
    echo Check HBase Web UI in browser: http://%MASTER_IP%:16010/master-status#regionservers
)

echo.
echo ===================================================================
echo [4/4] Running Automated End-to-End Pipeline Verification Suite
echo ===================================================================
python scripts\test_pipeline.py --api http://%MASTER_IP%:8000

echo.
echo ===================================================================
echo  VERIFICATION COMPLETE!
echo ===================================================================
pause
