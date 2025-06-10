@echo off
setlocal enabledelayedexpansion

REM Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Python is not installed or not in PATH
    exit /b 1
)

REM Install required packages
echo Installing required packages...
pip install -r requirements.txt

if %ERRORLEVEL% neq 0 (
    echo Failed to install required packages
    exit /b 1
)

REM Run the Kafka topics setup
echo Setting up Kafka topics...
python setup_kafka_topics.py %*

if %ERRORLEVEL% neq 0 (
    echo Failed to set up Kafka topics
    exit /b 1
)

echo Kafka topics setup completed successfully
exit /b 0
