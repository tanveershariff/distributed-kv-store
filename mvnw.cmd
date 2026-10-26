@REM ---------------------------------------------------------------------------
@REM Apache Maven Wrapper startup script for Windows
@REM This script downloads Maven on first run into .mvn\dist\ and then
@REM delegates all arguments to it — no system-wide Maven install needed.
@REM ---------------------------------------------------------------------------
@echo off
setlocal EnableExtensions EnableDelayedExpansion

set MAVEN_VERSION=3.9.16
set MAVEN_DIR=%~dp0.mvn\dist\apache-maven-%MAVEN_VERSION%
set MVN_CMD=%MAVEN_DIR%\bin\mvn.cmd

if exist "%MVN_CMD%" goto :run

echo [mvnw] Maven %MAVEN_VERSION% not found locally. Downloading...
set DOWNLOAD_URL=https://downloads.apache.org/maven/maven-3/%MAVEN_VERSION%/binaries/apache-maven-%MAVEN_VERSION%-bin.zip
set ZIP_FILE=%TEMP%\apache-maven-%MAVEN_VERSION%-bin.zip
set DIST_DIR=%~dp0.mvn\dist

if not exist "%DIST_DIR%" mkdir "%DIST_DIR%"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;" ^
  "Write-Host '[mvnw] Downloading from %DOWNLOAD_URL%';" ^
  "Invoke-WebRequest -Uri '%DOWNLOAD_URL%' -OutFile '%ZIP_FILE%' -UseBasicParsing;" ^
  "Write-Host '[mvnw] Extracting...';" ^
  "Expand-Archive -Path '%ZIP_FILE%' -DestinationPath '%DIST_DIR%' -Force;" ^
  "Remove-Item '%ZIP_FILE%' -Force;" ^
  "Write-Host '[mvnw] Maven %MAVEN_VERSION% ready.'"

if not exist "%MVN_CMD%" (
  echo [mvnw] ERROR: Maven installation failed. Exiting.
  exit /b 1
)

:run
"%MVN_CMD%" %*
endlocal
