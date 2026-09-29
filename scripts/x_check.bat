@echo off
REM X 插件一键检查：契约守卫（秒级）+ 黄金路径冒烟（分钟级）
REM 详见 docs\VERIFICATION_RULES.md
REM 用法：scripts\x_check.bat            两个都跑
REM       scripts\x_check.bat guard      只跑契约守卫（可在无服务时用 --offline 变体）
setlocal
cd /d "%~dp0.."

set ONLY=%1
if /i "%ONLY%"=="guard" goto guard

echo ===== 契约守卫（x_guard）=====
node scripts\x_guard.js
if errorlevel 1 (
  echo.
  echo [x_check] 契约守卫未通过，停止后续检查。
  exit /b 1
)

echo.
echo ===== 黄金路径冒烟（x_smoke）=====
node scripts\x_smoke.js
set RC=%errorlevel%
if not "%RC%"=="0" (
  echo.
  echo [x_check] 冒烟未通过（退出码 %RC%）。
  exit /b %RC%
)
echo.
echo [x_check] 全部通过。
exit /b 0

:guard
node scripts\x_guard.js
exit /b %errorlevel%
