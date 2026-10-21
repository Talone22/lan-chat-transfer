@echo off
chcp 936 >nul
setlocal
cd /d "%~dp0"

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8081"
set "STUN=%~2"
if "%STUN%"=="" set "STUN=3478"
set /a "HTTPS=PORT+100"

rem 一键部署：放行防火墙 -> 启动服务 -> 自动开浏览器
rem 需要管理员权限放行防火墙，脚本会自动触发 UAC 提权

net session >nul 2>&1
if not "%errorlevel%"=="0" (
  echo.
  echo   需要管理员权限放行防火墙，正在请求提权...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -ArgumentList '%PORT%' -Verb RunAs -WorkingDirectory '%~dp0'"
  exit /b
)

echo.
echo   ============ P2P 局域网传输 · 一键部署 ============
echo   端口：TCP %PORT% (http)  TCP %HTTPS% (https)  UDP %STUN% (STUN)
echo   --------------------------------------------------
echo.
echo   [1/2] 放行防火墙（覆盖专用/公用/域全部网络类型）...

netsh advfirewall firewall delete rule name="P2P LAN Transfer %PORT%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN Transfer %PORT%" dir=in action=allow protocol=TCP localport=%PORT% profile=any
set "RC1=%errorlevel%"

netsh advfirewall firewall delete rule name="P2P LAN Transfer HTTPS %HTTPS%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN Transfer HTTPS %HTTPS%" dir=in action=allow protocol=TCP localport=%HTTPS% profile=any
set "RC3=%errorlevel%"

netsh advfirewall firewall delete rule name="P2P LAN STUN %STUN%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN STUN %STUN%" dir=in action=allow protocol=UDP localport=%STUN% profile=any
set "RC2=%errorlevel%"

if "%RC1%"=="0" (echo   [OK] TCP %PORT% 已放行) else (echo   [警告] TCP %PORT% 放行失败)
if "%RC3%"=="0" (echo   [OK] TCP %HTTPS% 已放行) else (echo   [警告] TCP %HTTPS% 放行失败)
if "%RC2%"=="0" (echo   [OK] UDP %STUN% 已放行) else (echo   [警告] UDP %STUN% 放行失败)

echo.
echo   [2/2] 启动服务并自动打开浏览器...
echo.
call start.bat %PORT% %STUN%
