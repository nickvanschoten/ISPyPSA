@echo off
setlocal EnableDelayedExpansion
REM =====================================================================
REM  ISPyPSA Pass-1 dashboard launcher (Windows).
REM
REM  Designed for zero-setup: a fresh machine with no Python and no `uv`
REM  can double-click this file and reach the dashboard. The script:
REM    1. Locates `uv` on PATH or at the standalone install location, OR
REM       installs it via the official astral.sh standalone installer.
REM    2. Runs `uv sync --extra dashboard`, which creates .venv, installs
REM       project deps (including streamlit), and provisions a Python
REM       interpreter if none is present.
REM    3. Launches Streamlit. Browser opens at http://localhost:8501.
REM
REM  Subsequent runs skip the install steps (they no-op when up to date).
REM =====================================================================

cd /d "%~dp0"

REM ---------------------------------------------------------------------
REM  Step 1: locate or install `uv`.
REM ---------------------------------------------------------------------
set "UV="
where uv >nul 2>&1
if %errorlevel%==0 (
    set "UV=uv"
) else if exist "%USERPROFILE%\.local\bin\uv.exe" (
    set "UV=%USERPROFILE%\.local\bin\uv.exe"
)

if "%UV%"=="" (
    echo.
    echo  =====================================================================
    echo   First-run setup: installing the `uv` Python package manager.
    echo   One-off step. Downloads from astral.sh, ~30 MB. No admin needed.
    echo  =====================================================================
    echo.
    powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://astral.sh/uv/install.ps1 | iex"
    if errorlevel 1 (
        echo.
        echo  ERROR: failed to install `uv` automatically.
        echo  Please install it manually from:
        echo    https://docs.astral.sh/uv/getting-started/installation/
        echo  then re-run this script.
        echo.
        pause
        exit /b 1
    )
    if exist "%USERPROFILE%\.local\bin\uv.exe" (
        set "UV=%USERPROFILE%\.local\bin\uv.exe"
    ) else (
        echo.
        echo  ERROR: `uv` was installed but is not at the expected location
        echo    %USERPROFILE%\.local\bin\uv.exe
        echo  Please open a new terminal, confirm `uv --version` works,
        echo  then re-run this script.
        echo.
        pause
        exit /b 1
    )
)

echo Using uv: %UV%

REM ---------------------------------------------------------------------
REM  Step 2: sync project dependencies.
REM  `uv sync --extra dashboard` creates .venv and installs everything from
REM  pyproject.toml + uv.lock, INCLUDING the optional `dashboard` extra that
REM  provides streamlit. It also auto-provisions a Python interpreter if
REM  needed. Fast no-op on subsequent runs when nothing has changed.
REM ---------------------------------------------------------------------
echo.
echo Syncing dependencies. One-off on first run; fast after.
"%UV%" sync --extra dashboard
if errorlevel 1 (
    echo.
    echo  ERROR: `uv sync` failed. See messages above.
    echo  Most common cause: no internet connection on first run.
    echo.
    pause
    exit /b 1
)

REM ---------------------------------------------------------------------
REM  Self-heal: verify the dashboard's core dependency actually installed.
REM  A pre-existing corrupt .venv (e.g. missing RECORD files) can cause
REM  `uv sync` to uninstall packages without installing replacements. If
REM  streamlit isn't importable, force a clean reinstall.
REM ---------------------------------------------------------------------
"%UV%" run --no-sync python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo.
    echo  Streamlit not importable after sync — .venv looks stale.
    echo  Forcing a clean reinstall ...
    echo.
    "%UV%" sync --reinstall --extra dashboard
    if errorlevel 1 (
        echo.
        echo  ERROR: clean reinstall failed. Try deleting .venv and re-running.
        echo.
        pause
        exit /b 1
    )
    REM Final check after the forced reinstall.
    "%UV%" run --no-sync python -c "import streamlit" >nul 2>&1
    if errorlevel 1 (
        echo.
        echo  ERROR: Streamlit still not importable after reinstall.
        echo  Manual recovery: delete the `.venv` folder and re-run this script.
        echo.
        pause
        exit /b 1
    )
)

REM ---------------------------------------------------------------------
REM  Step 3: launch the dashboard.
REM ---------------------------------------------------------------------
echo.
echo Starting dashboard.
echo It will open in your default browser at http://localhost:8501
echo Press Ctrl+C in this window to stop the server.
echo.
"%UV%" run streamlit run analysis\dashboard\frontier_dashboard.py

REM Keep the window open after Streamlit exits so any final messages stay visible.
echo.
pause
endlocal