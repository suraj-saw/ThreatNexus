@echo off
setlocal
title ThreatNexus - Real-Time Cybersecurity Platform

echo =======================================================
echo ThreatNexus - Distributed Threat Detection Platform
echo =======================================================
echo.

cd /d "%~dp0"
if errorlevel 1 (
    echo ERROR: Could not access the project directory.
    pause
    exit /b 1
)

echo [1/8] Checking Docker...
docker version
if errorlevel 1 (
    echo ERROR: Docker Desktop is not running or Docker is not installed.
    pause
    exit /b 1
)

echo.
echo [2/8] Creating .env from .env.example if needed...
if not exist ".env" (
    copy /Y ".env.example" ".env"
    if errorlevel 1 (
        echo ERROR: Could not create .env.
        pause
        exit /b 1
    )
) else (
    echo .env already exists. Keeping existing configuration.
)

echo.
echo [3/8] Building and starting ThreatNexus cluster containers...
docker compose up -d --build
if errorlevel 1 (
    echo ERROR: Docker Compose failed to start.
    echo Run: docker compose logs
    pause
    exit /b 1
)

echo.
echo [4/8] Showing container status...
docker compose ps

echo.
echo [5/8] Probing API and HBase table readiness...
set "API_READY="
for /L %%I in (1,1,20) do (
    curl.exe --fail --silent http://localhost:8000/health >nul 2>&1
    if not errorlevel 1 (
        curl.exe --fail --silent http://localhost:8000/api/alerts >nul 2>&1
        if not errorlevel 1 (
            set "API_READY=1"
            goto :api_ready
        )
    )
    echo Waiting for API, Kafka, and HBase initialization... attempt %%I/20
    timeout /t 10 /nobreak >nul
)

:api_ready
if not defined API_READY (
    echo ERROR: API did not become ready after 200 seconds.
    echo Run: docker compose logs --tail=100 api hbase hbase-init
    pause
    exit /b 1
)
curl.exe --fail http://localhost:8000/health
echo.
echo [API HEALTH CHECK PASSED]

echo.
echo [6/8] Opening ThreatNexus React SOC Dashboard...
start "" "http://localhost:3000"

echo.
echo [7/8] Triggering SSH Brute Force attack scenario (7 failed logins)...
python scripts\replay_dataset.py --scenario brute_force --target http://localhost:8000/api/events
if errorlevel 1 (
    echo Notice: Python replay tool finished or Python not in PATH.
)

echo.
echo [8/8] Waiting 25 seconds for Spark streaming window, then fetching alerts...
timeout /t 25 /nobreak
curl.exe http://localhost:8000/api/alerts

echo.
echo =======================================================
echo ThreatNexus is running successfully.
echo - React SOC Dashboard: http://localhost:3000
echo - FastAPI Docs:        http://localhost:8000/docs
echo - HDFS NameNode UI:    http://localhost:9870
echo - Spark Master UI:     http://localhost:8080
echo - HBase Master UI:     http://localhost:16010
echo =======================================================
echo.
echo To run the automated end-to-end verification suite:
echo   python scripts\test_pipeline.py
echo.
echo To view live logs:
echo   docker compose logs -f api spark-streaming
echo.
echo To stop the cluster:
echo   docker compose down
echo.
pause
endlocal
