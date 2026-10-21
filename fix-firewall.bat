@echo off
chcp 936 >nul
setlocal

rem 放行端口 [需要 专用/公用/域 全部网络类型]
rem   TCP 8081   网页(http)
rem   TCP 8181   手机网页(https，大文件落盘必须)
rem   UDP 3478   内置 STUN（缺了手机和电脑会一直"连接中"）
rem 换网络、换手机热点、校园 WiFi 后跑一次即可
rem 需要管理员权限，脚本会自动触发 UAC 提权

net session >nul 2>&1
if not "%errorlevel%"=="0" (
  echo.
  echo   需要管理员权限，正在请求提权...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)

set "PORT=8081"
if not "%~1"=="" set "PORT=%~1"
set "STUN=3478"
if not "%~2"=="" set "STUN=%~2"
set /a "HTTPS=PORT+100"

echo.
echo   正在添加防火墙入站规则，覆盖全部网络类型...
echo     TCP %PORT%   网页(http)
echo     TCP %HTTPS%  手机网页(https)
echo     UDP %STUN%   STUN 打洞
echo.

netsh advfirewall firewall delete rule name="P2P LAN Transfer %PORT%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN Transfer %PORT%" dir=in action=allow protocol=TCP localport=%PORT% profile=any
set "RC1=%errorlevel%"

netsh advfirewall firewall delete rule name="P2P LAN Transfer HTTPS %HTTPS%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN Transfer HTTPS %HTTPS%" dir=in action=allow protocol=TCP localport=%HTTPS% profile=any
set "RC3=%errorlevel%"

netsh advfirewall firewall delete rule name="P2P LAN STUN %STUN%" >nul 2>&1
netsh advfirewall firewall add rule name="P2P LAN STUN %STUN%" dir=in action=allow protocol=UDP localport=%STUN% profile=any
set "RC2=%errorlevel%"

echo.
if "%RC1%"=="0" (echo   [OK] 网页 TCP %PORT% 已放行) else (echo   [警告] TCP %PORT% 放行失败)
if "%RC3%"=="0" (echo   [OK] 手机 https TCP %HTTPS% 已放行) else (echo   [警告] TCP %HTTPS% 放行失败)
if "%RC2%"=="0" (echo   [OK] STUN UDP %STUN% 已放行) else (echo   [警告] UDP %STUN% 放行失败)

echo.
echo   当前网络类型：
powershell -NoProfile -Command "Get-NetConnectionProfile | Select-Object InterfaceAlias,NetworkCategory | Format-Table -AutoSize"

echo.
pause
