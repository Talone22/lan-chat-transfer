@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8081"

rem 按顺序找 Node：随包自带的便携版 -> 本机安装版 -> PATH 里的 node
set "NODE_EXE=%~dp0runtime\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=C:\Users\DMU\.workbuddy\binaries\node\versions\22.22.2-6\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=node"

title Stop P2P LAN Transfer

"%NODE_EXE%" stop.js %PORT%

if errorlevel 1 (
  echo.
  echo   Node 调用失败，改用 PID 文件直接结束进程...
  if exist ".server.pid" (
    for /f "usebackq delims=" %%p in (".server.pid") do (
      taskkill /PID %%p /F >nul 2>&1 && echo   已结束 PID %%p
      del /q ".server.pid" >nul 2>&1
    )
  ) else (
    echo   没有找到 .server.pid，服务可能本来就没在跑。
  )
)

echo.
pause
