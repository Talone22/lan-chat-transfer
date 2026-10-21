@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8081"
set "STUN=%~2"
if "%STUN%"=="" set "STUN=3478"

rem 按顺序找 Node：随包自带的便携版 -> 本机安装版 -> PATH 里的 node
set "NODE_EXE=%~dp0runtime\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=C:\Users\DMU\.workbuddy\binaries\node\versions\22.22.2-6\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=node"

rem 服务就绪后自动用默认浏览器打开本机页面（由 server.js 触发）
set "OPEN_BROWSER=1"

title P2P LAN Transfer - port %PORT%

echo.
echo   P2P 局域网传输 正在启动，端口 %PORT%（手机 https 端口 %PORT%+100）
echo   ------------------------------------------------
echo.

"%NODE_EXE%" server.js %PORT% %STUN%

echo.
echo   服务已退出。
if errorlevel 1 (
  echo   如果上面报错，检查端口是否被占用:  netstat -ano ^| findstr :%PORT%
)

pause
